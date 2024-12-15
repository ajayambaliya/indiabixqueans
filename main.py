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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MONGO_DB_URI = os.getenv("MONGO_DB_URI")
ENGLISH_CHANNEL = "@gujtest2"
GUJARATI_CHANNEL = "@gujtest"

# MongoDB setup with dynamic connection check
def get_mongo_collection():
    """
    Get MongoDB collection with error handling.
    """
    try:
        client = MongoClient(MONGO_DB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()  # Verify connection
        db = client['current_affairs']
        return db['processed_urls']
    except errors.PyMongoError as e:
        logger.error(f"MongoDB connection failed: {e}")
        return None




def extract_date_from_url(url):
    """
    Extract date from URL or return current date.
    """
    try:
        match = re.search(r'(\d{4}-\d{2}-\d{2})', url)
        return match.group(0) if match else datetime.datetime.now().strftime('%Y-%m-%d')
    except Exception as e:
        logger.warning(f"Date extraction failed: {e}")
        return datetime.datetime.now().strftime('%Y-%m-%d')

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def send_telegram_message(message, channel):
    """
    Send message to Telegram with retry logic.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': channel,
        'text': message,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': True  # Disable web page previews
    }
    
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        result = response.json().get('result', {})
        message_id = result.get('message_id')
        logger.info(f"Message sent successfully to {channel}")
        return message_id
    except requests.exceptions.RequestException as e:
        logger.error(f"Telegram send message failed: {e}")
        raise  # Retry using tenacity


def smart_split_message(message, max_length=4096, footer=""):
    """
    Intelligently split long messages into smaller chunks, ensuring no question is cut mid-block.
    """
    if len(message) + len(footer) <= max_length:
        return [message + footer]
    
    blocks = message.split("--------------------------------------\n\n")
    split_messages = []
    current_message = ""
    
    for block in blocks:
        # Add block to current message if it fits within the limit
        if len(current_message) + len(block) + len(footer) <= max_length:
            current_message += block + "--------------------------------------\n\n"
        else:
            # Finalize the current message and start a new one
            split_messages.append(current_message + footer)
            current_message = block + "--------------------------------------\n\n"
    
    # Add the last remaining message
    if current_message.strip():
        split_messages.append(current_message + footer)
    
    return split_messages





def extract_question_data(soup, url):
    """
    Extract current affairs questions from the webpage.
    """
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
                
                question_text = question_text.text.strip()
                correct_answer = container.find('input', {'class': 'jq-hdnakq'})
                correct_answer = correct_answer.get('value', '').strip() if correct_answer else ''
                
                option_divs = container.find_all('div', {'class': 'bix-td-option-val'})
                option_letters = ['A', 'B', 'C', 'D']
                
                correct_answer_text = ""
                if correct_answer and option_divs:
                    for opt_idx, div in enumerate(option_divs):
                        if div and div.text.strip() and option_letters[opt_idx] == correct_answer:
                            correct_answer_text = div.text.strip()
                            break
                
                explanation_div = container.find('div', class_='bix-ans-description')
                explanation_text = explanation_div.text.strip() if explanation_div else "No detailed explanation available"
                
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

def process_current_affairs_url(url, collection):
    """
    Process a single current affairs URL.
    """
    try:
        response = requests.get(url, verify=False, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        message_english = extract_question_data(soup, url)
        if not message_english:
            logger.warning(f"No questions extracted from URL: {url}")
            return
        
        # Add a promotional footer (only for English messages during splitting)
        promotional_message_english = (
            "\n\n🚀 Never miss an update on the latest current affairs and quizzes! 🌟\n"
            "👉 Join [Daily Current Affairs in English](https://t.me/daily_current_all_source) @Daily_Current_All_Source.\n"
            "👉 Follow [Gujarati Current Affairs](https://t.me/gujtest) @CurrentAdda. 🇮🇳✨\n\n"
            "Stay ahead of the competition. Join us now! 💪📚"
        )
        
        # Split English messages into chunks with the promotional footer
        english_messages = smart_split_message(message_english, footer=promotional_message_english)
        english_message_links = []  # To store links of English posts
        
        # Send English messages
        for msg in english_messages:
            message_id = send_telegram_message(msg, ENGLISH_CHANNEL)
            if message_id:
                # Construct the URL for the Telegram post
                post_link = f"https://t.me/{ENGLISH_CHANNEL.strip('@')}/{message_id}"
                english_message_links.append(post_link)
        
        # Send Gujarati messages with translated content
        for msg_english, english_link in zip(english_messages, english_message_links):
            # Translate the English message
            msg_gujarati = translate_message(msg_english)
            
            # Append the English post link to the Gujarati message without duplicating the promotional footer
            gujarati_message = (
                f"{msg_gujarati}\n\n"
                f"Read this post in English: [Click here]({english_link})\n\n"
                "👉 Join [Daily Current Affairs in English](https://t.me/daily_current_all_source)\n"
                "👉 Follow [Gujarati Current Affairs](https://t.me/gujtest)\n\n"
            )
            
            send_telegram_message(gujarati_message, GUJARATI_CHANNEL)
        
        # Mark the URL as processed in the database
        if collection is not None:
            collection.insert_one({"url": url, "processed_at": datetime.datetime.now()})
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error processing URL {url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing URL {url}: {e}")







def translate_message(message):
    """
    Translate the provided message to Gujarati.
    """
    try:
        # Split the message into sections
        sections = message.split("\n\n")
        translated_sections = []
        
        for section in sections:
            if 'Question:' in section or 'Correct Answer:' in section or 'Explanation:' in section:
                try:
                    # Translate only relevant sections
                    translated_section = GoogleTranslator(source='en', target='gu').translate(section)
                    translated_sections.append(translated_section)
                except Exception as translation_err:
                    logger.warning(f"Translation error for section: {translation_err}")
                    translated_sections.append(section)  # Fallback to original section
            else:
                # Append other sections as is
                translated_sections.append(section)
        
        return "\n\n".join(translated_sections)
    except Exception as e:
        logger.error(f"Error in translation: {e}")
        return message



def fetch_and_process_current_affairs():
    """
    Fetch and process current affairs from the main page.
    """
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
            
            # Use 'collection is not None' for truth-value testing
            if collection is not None and collection.find_one({"url": href}):
                logger.info(f"URL already processed: {href}")
                continue
            
            process_current_affairs_url(href, collection)
            time.sleep(2)  # Delay to avoid server overload
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in fetch_and_process_current_affairs: {e}")


if __name__ == '__main__':
    logger.info("Starting Current Affairs Processing")
    fetch_and_process_current_affairs()
    logger.info("Current Affairs Processing Completed")
