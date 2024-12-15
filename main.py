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
    handlers=[
        logging.FileHandler('current_affairs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Environment Variables
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MONGO_DB_URI = os.getenv("MONGO_DB_URI")
ENGLISH_CHANNEL = "@gujtest2"
GUJARATI_CHANNEL = "@gujtest"

# Validate Environment Variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN is not set. Exiting...")
    exit(1)

if not MONGO_DB_URI:
    logger.error("MONGO_DB_URI is not set. Exiting...")
    exit(1)

# MongoDB setup with dynamic connection check
def get_mongo_collection():
    try:
        client = MongoClient(MONGO_DB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()  # Verify connection
        db = client['current_affairs']
        return db['processed_urls']
    except errors.ServerSelectionTimeoutError:
        logger.error("MongoDB server is unreachable. Skipping database operations.")
        return None

# Escape Markdown characters for Telegram messages
def escape_markdown(text):
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

# Extract date from URL
def extract_date_from_url(url):
    try:
        match = re.search(r'(\d{4}-\d{2}-\d{2})', url)
        return match.group(0) if match else datetime.datetime.now().strftime('%Y-%m-%d')
    except Exception as e:
        logger.warning(f"Date extraction failed: {e}")
        return datetime.datetime.now().strftime('%Y-%m-%d')

# Telegram message sender with retry logic
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
# Telegram message sender with retry logic
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def send_telegram_message(message, channel):
    """
    Sends a Telegram message to the specified channel. Logs the message length and
    tracks the retry mechanism for potential failures.
    """
    message_length = len(message)
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': channel,
        'text': message,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': True
    }
    logger.info(f"Attempting to send message to {channel} with length: {message_length} characters.")
    
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        result = response.json().get('result', {})
        message_id = result.get('message_id')
        logger.info(f"Message sent successfully to {channel} with length: {message_length} characters.")
        return message_id
    except requests.exceptions.RequestException as e:
        logger.error(f"Telegram send message failed with length: {message_length} characters. Error: {e}")
        raise


# Intelligently split messages
def smart_split_message(message, max_length=4096, footer=""):
    if len(message) + len(footer) <= max_length:
        return [message + footer]

    blocks = message.split("--------------------------------------\n\n")
    split_messages = []
    current_message = ""

    for block in blocks:
        if len(current_message) + len(block) + len(footer) <= max_length:
            current_message += block + "--------------------------------------\n\n"
        else:
            split_messages.append(current_message + footer)
            current_message = block + "--------------------------------------\n\n"

    if current_message.strip():
        split_messages.append(current_message + footer)

    return split_messages

# Extract questions from page
def extract_question_data(soup, url):
    try:
        date = extract_date_from_url(url)
        message = f"✨✨ *Current Important Events - {date}* ✨✨\n\n"
        message += "🌟 *Today's Current Affairs Quiz* 🌟\n\n"

        question_containers = soup.find_all('div', class_='bix-div-container')
        if not question_containers:
            logger.warning(f"No question containers found in URL: {url}")
            return None

        for container in question_containers:
            try:
                question_text = container.find('div', class_='bix-td-qtxt')
                if not question_text:
                    continue

                question_text = escape_markdown(question_text.text.strip())
                correct_answer = container.find('input', {'class': 'jq-hdnakq'})
                correct_answer = correct_answer.get('value', '').strip() if correct_answer else ''

                option_divs = container.find_all('div', {'class': 'bix-td-option-val'})
                option_letters = ['A', 'B', 'C', 'D']

                correct_answer_text = ""
                if correct_answer and option_divs:
                    for opt_idx, div in enumerate(option_divs):
                        if div and div.text.strip() and option_letters[opt_idx] == correct_answer:
                            correct_answer_text = escape_markdown(div.text.strip())
                            break

                explanation_div = container.find('div', class_='bix-ans-description')
                explanation_text = escape_markdown(
                    explanation_div.text.strip() if explanation_div else "No detailed explanation available"
                )

                question_message = f"❓ *Question:* {question_text}\n\n"
                question_message += f"🎯 *Correct Answer:* {correct_answer_text}\n\n"
                question_message += f"💡 *Explanation:* {explanation_text}\n\n"
                question_message += "--------------------------------------\n\n"

                message += question_message

            except Exception as e:
                logger.error(f"Error processing individual question: {e}")

        return message

    except Exception as e:
        logger.error(f"Unexpected error in extract_question_data: {e}")
        return None

# Translate message to Gujarati
def translate_message(message):
    try:
        sections = message.split("\n\n")
        translated_sections = []
        for section in sections:
            if "Question:" in section or "Correct Answer:" in section or "Explanation:" in section:
                translated_sections.append(GoogleTranslator(source='en', target='gu').translate(section))
            else:
                translated_sections.append(section)
        return "\n\n".join(translated_sections)
    except Exception as e:
        logger.error(f"Translation error: {e}")
        return message

# Process single URL
def process_current_affairs_url(url, collection):
    try:
        response = requests.get(url, verify=False, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        message_english = extract_question_data(soup, url)
        if not message_english:
            logger.warning(f"No questions extracted from URL: {url}")
            return

        promotional_message = "\n\n🚀 Join [Daily Current Affairs](https://t.me/daily_current_all_source) 🌟"
        english_messages = smart_split_message(message_english, footer=promotional_message)

        english_links = []
        for msg in english_messages:
            message_id = send_telegram_message(msg, ENGLISH_CHANNEL)
            if message_id:
                english_links.append(f"https://t.me/{ENGLISH_CHANNEL.strip('@')}/{message_id}")

        for msg, link in zip(english_messages, english_links):
            translated_msg = translate_message(msg)
            translated_msg += f"\n\n🔗 Read in English: [Click here]({link})"
            send_telegram_message(translated_msg, GUJARATI_CHANNEL)

        if collection is not None:
            collection.insert_one({"url": url, "processed_at": datetime.datetime.utcnow()})
            logger.info(f"Logged URL to MongoDB: {url}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error processing URL {url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing URL {url}: {e}")

# Main fetching function
def fetch_and_process_current_affairs():
    url = "https://www.indiabix.com/current-affairs/questions-and-answers/"
    try:
        response = requests.get(url, verify=False, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        current_date = datetime.datetime.now().strftime('%Y-%m')
        links = soup.find_all('a', class_='text-link me-3')
        collection = get_mongo_collection()

        for link in links:
            href = link.get('href')
            if not href or current_date not in href:
                continue

            if collection is not None and collection.find_one({"url": href}):
                logger.info(f"URL already processed: {href}")
                continue

            process_current_affairs_url(href, collection)
            time.sleep(2)  # Rate limit to avoid server overload

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in fetch_and_process_current_affairs: {e}")

if __name__ == '__main__':
    logger.info("Starting Current Affairs Processing")
    fetch_and_process_current_affairs()
    logger.info("Current Affairs Processing Completed")
