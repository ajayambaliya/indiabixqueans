import requests
import logging
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
import re
from pymongo import MongoClient, errors
import datetime
import time
import urllib3
from tenacity import retry, stop_after_attempt, wait_fixed
import os

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('current_affairs.log'), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration Constants
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MONGO_DB_URI = os.getenv("MONGO_DB_URI")
ENGLISH_CHANNEL = "@daily_current_all_source"
GUJARATI_CHANNEL = "@currentadda"
CHANNEL_JOIN_LINK = "https://t.me/+UkTcRyx3rhERLwQR"
QUESTIONS_PER_MESSAGE = 4  # Number of questions per message


def get_mongo_collection():
    try:
        logger.info("Connecting to MongoDB...")
        client = MongoClient(MONGO_DB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client['current_affairs']
        logger.info("MongoDB connection successful.")
        return db['processed_urls']
    except errors.PyMongoError as e:
        logger.error(f"MongoDB connection failed: {e}")
        return None

def clean_html_text(text):
    """
    Clean and escape HTML special characters to prevent parsing issues.
    """
    html_escape_table = {
        "&": "&amp;",
        '"': "&quot;",
        "'": "&apos;",
        ">": "&gt;",
        "<": "&lt;",
    }
    return ''.join(html_escape_table.get(c, c) for c in text)

def extract_question_data(soup, url):
    try:
        logger.info(f"Extracting questions from URL: {url}")
        date = re.search(r'(\d{4}-\d{2}-\d{2})', url).group(0)

        question_containers = soup.find_all('div', class_='bix-div-container')
        if not question_containers:
            logger.warning(f"No question containers found in URL: {url}")
            return None

        questions = []
        for index, container in enumerate(question_containers, 1):
            try:
                question_text_div = container.find('div', class_='bix-td-qtxt')
                if not question_text_div:
                    continue

                question_text = clean_html_text(question_text_div.text.strip())
                correct_answer_key = container.find('input', {'class': 'jq-hdnakq'}).get('value', '').strip()

                options = container.find_all('div', class_='bix-td-option-val')
                option_map = {chr(65 + idx): clean_html_text(option.text.strip()) for idx, option in enumerate(options)}

                correct_answer_text = option_map.get(correct_answer_key, "Unknown")

                explanation_div = container.find('div', class_='bix-ans-description')
                explanation_text = clean_html_text(explanation_div.text.strip()) if explanation_div else "No detailed explanation available"

                questions.append({
                    'index': index,
                    'question_text': question_text,
                    'correct_answer': correct_answer_text,
                    'explanation': explanation_text,
                })
            except Exception as e:
                logger.error(f"Error processing individual question: {e}")

        return questions
    except Exception as e:
        logger.error(f"Unexpected error in extract_question_data: {e}")
        return None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def send_telegram_message(message, channel):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': channel,
        'text': message,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True
    }
    try:
        logger.info(f"Sending message to {channel}. Length: {len(message)}")
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        result = response.json().get('result', {})
        message_id = result.get('message_id')
        logger.info(f"Message sent successfully to {channel}. Message ID: {message_id}")
        return message_id
    except requests.exceptions.RequestException as e:
        logger.error(f"Telegram send message failed: {e}, Response: {response.text if 'response' in locals() else 'No response'}")
        raise

def format_message(questions, date, language="en"):
    """
    Formats a group of questions into a single message.
    """
    message = (
        f"<b>🌟 Current Important Events 📅 {date}</b>\n\n"
        f"<b>📚 Today's Current Affairs Quiz 🤔</b>\n\n"
    )

    for question in questions:
        if language == "gu":
            question_text = GoogleTranslator(source='en', target='gu').translate(question['question_text'])
            correct_answer = GoogleTranslator(source='en', target='gu').translate(question['correct_answer'])
            explanation = GoogleTranslator(source='en', target='gu').translate(question['explanation'])
        else:
            question_text = question['question_text']
            correct_answer = question['correct_answer']
            explanation = question['explanation']

        message += (
            f"<b>❓ Question {question['index']}:</b>\n"
            f"<i>{question_text}</i>\n\n"
            f"<b>🏆 Correct Answer:</b> {correct_answer}\n\n"
            f"<b>💡 Explanation:</b> {explanation}\n\n"
            f"{'=' * 40}\n\n"
        )

    return message


def format_gujarati_message(html_message):
    """
    Translates an HTML message to Gujarati while preserving HTML formatting.
    """
    try:
        # Extract plain text for translation, excluding HTML tags
        soup = BeautifulSoup(html_message, 'html.parser')
        
        # Function to translate text while preserving HTML structure
        def translate_text_in_soup(tag):
            if tag.name:  # If it's an HTML tag
                for child in tag.children:
                    if child.name is None:  # If it's a text node
                        try:
                            translated_text = GoogleTranslator(source='en', target='gu').translate(child.strip())
                            child.replace_with(clean_html_text(translated_text))
                        except Exception as e:
                            logger.error(f"Translation error: {e}")
            return tag

        # Apply translation to the soup
        translated_soup = translate_text_in_soup(soup)
        
        return str(translated_soup)
    except Exception as e:
        logger.error(f"Error in translating message: {e}")
        return html_message

def process_current_affairs_url(url, collection):
    """
    Processes the current affairs data from a given URL.
    """
    try:
        logger.info(f"Processing URL: {url}")
        response = requests.get(url, verify=False, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        date = re.search(r'(\d{4}-\d{2}-\d{2})', url).group(0)
        questions = extract_question_data(soup, url)
        if not questions:
            logger.warning(f"No questions extracted from URL: {url}")
            return

        # Group questions into batches of QUESTIONS_PER_MESSAGE
        question_batches = [questions[i:i + QUESTIONS_PER_MESSAGE] for i in range(0, len(questions), QUESTIONS_PER_MESSAGE)]

        last_english_link = None  # Variable to store the last English message link

        # Prepare promotional message
        promotional_message = (
            f"<b>🔔 Stay Updated!</b>\n"
            f"Join our Telegram channels for daily current affairs:\n"
            f"ગુજરાત સરકારની કોઇ એવી ભરતી નહી હોઇ જેમા અમારુ કરંટ અફેરના પ્ર્શ્નો ના આવ્યા હોઇ:\n"
            f"🇬🇧 For Daily English Current Affairs: {ENGLISH_CHANNEL}\n"
            f"🇮🇳 For Daily Gujarati Current Affairs: {GUJARATI_CHANNEL}\n\n"
            f"<a href='{CHANNEL_JOIN_LINK}'>અહિયા કલિક કરી સિધા જોડાઇ જાવ અમારી સાથે!</a>"
        )

        # Process English messages
        for idx, batch in enumerate(question_batches):
            english_message = format_message(batch, date, language="en")

            # Append promotional message to the last English message
            if idx == len(question_batches) - 1:  # Last batch
                english_message += f"\n\n{promotional_message}"

            # Send English message
            message_id = send_telegram_message(english_message, ENGLISH_CHANNEL)
            if message_id:
                # Store the last English message link
                last_english_link = f"https://t.me/{ENGLISH_CHANNEL.strip('@')}/{message_id}"

            time.sleep(1.5)  # To avoid rate limits

        # Check if we have the last English link
        if not last_english_link:
            logger.error("No English messages were sent, cannot proceed with Gujarati messages.")
            return

        # Process Gujarati messages
        for idx, batch in enumerate(question_batches):
            gujarati_message = format_message(batch, date, language="gu")

            # For the last Gujarati message, add the English link and promotional message
            if idx == len(question_batches) - 1:
                gujarati_message += f"\n\n<b>📘 For reading this message in English:</b> <a href='{last_english_link}'>Click here</a>"
                gujarati_message += f"\n\n{promotional_message}"

            # Send Gujarati message
            send_telegram_message(gujarati_message, GUJARATI_CHANNEL)

            time.sleep(1.5)

        # Mark the URL as processed in MongoDB
        if collection is not None:
            collection.insert_one({"url": url, "processed_at": datetime.datetime.now()})
        logger.info(f"Finished processing URL: {url}")
    except Exception as e:
        logger.error(f"Unexpected error processing URL {url}: {e}")









def smart_split_message(message, max_length=4096, include_promo=False):
    """
    Intelligently splits HTML messages without breaking tags or questions.
    The promotional message is added only to the last part if include_promo is True.
    """
    promotional_message = (
        f"<b>🔔 Stay Updated!</b>\n"
        f"Join our Telegram channels for daily current affairs:\n"
        f"🇬🇧 For English Current Affairs: {ENGLISH_CHANNEL}\n"
        f"🇮🇳 For Gujarati Current Affairs: {GUJARATI_CHANNEL}\n\n"
        f"<a href='{CHANNEL_JOIN_LINK}'>Click here to join our CurrentAdda Channel!</a>"
    )

    messages = []
    current_message = ""
    lines = message.split('\n')

    for line in lines:
        # Prepare test message to check length
        test_message = (current_message + '\n' + line).strip()

        # If adding this line exceeds max length, finalize the current message
        if len(test_message.encode('utf-8')) > max_length:
            messages.append(current_message.strip())
            current_message = ""

        # Add line to the current message
        current_message += (line + '\n')

    # Add the last message
    if current_message.strip():
        messages.append(current_message.strip())

    # Append promotional message only to the last message if include_promo is True
    if include_promo and messages:
        messages[-1] += f"\n\n{promotional_message}"

    return messages


def fetch_and_process_current_affairs():
    """
    Fetches and processes the current affairs quiz from the website.
    """
    url = "https://www.indiabix.com/current-affairs/questions-and-answers/"
    try:
        logger.info(f"Fetching main page: {url}")
        response = requests.get(url, verify=False, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', class_='text-link me-3')
        collection = get_mongo_collection()
        current_month = datetime.datetime.now().strftime('%Y-%m')

        for link in links:
            href = link.get('href')
            if not href or current_month not in href:
                continue

            if collection is not None and collection.find_one({"url": href}):
                logger.info(f"URL already processed: {href}")
                continue

            process_current_affairs_url(href, collection)
            time.sleep(2)
    except Exception as e:
        logger.error(f"Unexpected error in fetch_and_process_current_affairs: {e}")

if __name__ == '__main__':
    logger.info("Starting Current Affairs Processing")
    fetch_and_process_current_affairs()
    logger.info("Current Affairs Processing Completed")
