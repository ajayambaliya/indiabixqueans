import os
import sys
import re
import time
import logging
import datetime
import urllib3

import requests
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
from pymongo import MongoClient, errors
from tenacity import retry, stop_after_attempt, wait_fixed

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('current_affairs.log', mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Environment Variables
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MONGO_DB_URI = os.getenv("MONGO_DB_URI")
ENGLISH_CHANNEL = "@gujtest2"
GUJARATI_CHANNEL = "@gujtest"

def validate_environment():
    """Validate critical environment variables."""
    required_vars = ['TELEGRAM_BOT_TOKEN', 'MONGO_DB_URI']
    for var in required_vars:
        if not os.getenv(var):
            logger.critical(f"{var} is not set. Exiting...")
            sys.exit(1)

def get_mongo_collection():
    """
    Establish MongoDB connection with enhanced error handling.
    
    Returns:
        pymongo.collection.Collection or None: MongoDB collection or None if connection fails
    """
    try:
        client = MongoClient(MONGO_DB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()  # Verify connection
        db = client['current_affairs']
        return db['processed_urls']
    except errors.ServerSelectionTimeoutError:
        logger.error("MongoDB server is unreachable. Skipping database operations.")
        return None
    except Exception as e:
        logger.error(f"Unexpected MongoDB connection error: {e}")
        return None

def escape_markdown_v2(text):
    """
    Advanced Markdown V2 escaping for Telegram messages.
    
    Args:
        text (str): Input text to escape
    
    Returns:
        str: Markdown V2 escaped text
    """
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in text)

@retry(stop=stop_after_attempt(5), wait=wait_fixed(5))
def fetch_url_with_retry(url, timeout=15):
    """
    Enhanced URL fetching with robust error handling and user agent.
    
    Args:
        url (str): URL to fetch
        timeout (int): Request timeout in seconds
    
    Returns:
        requests.Response: HTTP response
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(
            url, 
            headers=headers,
            verify=True,  # Enable SSL verification
            timeout=timeout
        )
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"URL fetch error for {url}: {e}")
        raise

def send_telegram_message(message, channel):
    """
    Send message to Telegram with enhanced error handling and Markdown V2 support.
    
    Args:
        message (str): Message to send
        channel (str): Telegram channel
    
    Returns:
        int or None: Message ID if successful, None otherwise
    """
    try:
        # Additional escaping for Markdown V2
        message = escape_markdown_v2(message)
        
        payload = {
            'chat_id': channel,
            'text': message,
            'parse_mode': 'MarkdownV2',
            'disable_web_page_preview': True
        }
        
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
            data=payload, 
            timeout=15,
            verify=True
        )
        
        if response.status_code != 200:
            logger.error(f"Telegram API Error: {response.status_code}")
            logger.error(f"Response Content: {response.text}")
            return None
        
        return response.json().get('result', {}).get('message_id')
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error sending Telegram message: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error sending Telegram message: {e}")
        return None

def translate_message(message):
    """
    Robust translation with section-wise translation and error handling.
    
    Args:
        message (str): Message to translate
    
    Returns:
        str: Translated message
    """
    try:
        sections = message.split("\n\n")
        translated_sections = []
        
        for section in sections:
            try:
                if any(key in section for key in ["Question:", "Correct Answer:", "Explanation:"]):
                    translated = GoogleTranslator(source='en', target='gu').translate(section)
                    translated_sections.append(translated)
                else:
                    translated_sections.append(section)
            except Exception as inner_e:
                logger.warning(f"Translation section error: {inner_e}")
                translated_sections.append(section)
        
        return "\n\n".join(translated_sections)
    except Exception as e:
        logger.error(f"Overall translation error: {e}")
        return message

def extract_current_affairs_questions(soup, url):
    """
    Extract current affairs questions with robust error handling.
    
    Args:
        soup (BeautifulSoup): Parsed HTML
        url (str): Source URL
    
    Returns:
        str or None: Extracted questions message
    """
    try:
        date = time.strftime('%Y-%m-%d')
        message = f"📅 *Current Affairs Quiz - {date}* 📚\n\n"

        question_containers = soup.find_all('div', class_='bix-div-container')
        if not question_containers:
            logger.warning(f"No question containers in URL: {url}")
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
                explanation_text = explanation_div.text.strip() if explanation_div else "No explanation available"

                message += f"❓ *Question:* {question_text}\n"
                message += f"🎯 *Correct Answer:* {correct_answer_text}\n"
                message += f"💡 *Explanation:* {explanation_text}\n\n"
                message += "--------------------------------------\n\n"

            except Exception as inner_e:
                logger.error(f"Error processing individual question: {inner_e}")

        return message
    except Exception as e:
        logger.error(f"Unexpected error extracting questions: {e}")
        return None

def process_current_affairs_url(url, collection):
    """
    Process current affairs URL with comprehensive error handling.
    
    Args:
        url (str): URL to process
        collection (pymongo.collection.Collection): MongoDB collection
    
    Returns:
        bool: Processing success status
    """
    try:
        response = fetch_url_with_retry(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        message_english = extract_current_affairs_questions(soup, url)
        if not message_english:
            logger.warning(f"No questions extracted from URL: {url}")
            return False

        promotional_message = "\n\n🚀 Join [Daily Current Affairs](https://t.me/daily_current_all_source) 🌟"
        
        # Send English message
        message_id = send_telegram_message(message_english + promotional_message, ENGLISH_CHANNEL)
        if not message_id:
            logger.error(f"Failed to send English message for {url}")
            return False

        # Create English link
        english_link = f"https://t.me/{ENGLISH_CHANNEL.strip('@')}/{message_id}"

        # Translate and send Gujarati message
        translated_message = translate_message(message_english)
        gujarati_message = translated_message + f"\n\n🔗 Read in English: [Link]({english_link})"
        
        send_telegram_message(gujarati_message + promotional_message, GUJARATI_CHANNEL)

        # Log processed URL
        if collection is not None:
            collection.insert_one({"url": url, "processed_at": datetime.datetime.utcnow()})

        return True
    
    except Exception as e:
        logger.error(f"Comprehensive error processing URL {url}: {e}")
        return False

def fetch_current_affairs_links():
    """
    Fetch current affairs links with robust error handling.
    
    Returns:
        list: URLs of current affairs
    """
    url = "https://www.indiabix.com/current-affairs/questions-and-answers/"
    try:
        response = fetch_url_with_retry(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        current_date = datetime.datetime.now().strftime('%Y-%m')
        
        links = [
            link.get('href') 
            for link in soup.find_all('a', class_='text-link me-3') 
            if link.get('href') and current_date in link.get('href')
        ]
        
        return links
    except Exception as e:
        logger.error(f"Error fetching current affairs links: {e}")
        return []

def main():
    """
    Main execution function with comprehensive error handling.
    """
    try:
        validate_environment()
        logger.info("Starting Current Affairs Processing")
        
        collection = get_mongo_collection()
        links = fetch_current_affairs_links()
        
        processed_count = 0
        for url in links:
            if collection is not None and collection.find_one({"url": url}):
                logger.info(f"URL already processed: {url}")
                continue
            
            if process_current_affairs_url(url, collection):
                processed_count += 1
                time.sleep(2)  # Rate limiting
        
        logger.info(f"Processed {processed_count} URLs successfully")
    
    except Exception as e:
        logger.critical(f"Critical failure in main process: {e}")
    finally:
        logger.info("Current Affairs Processing Completed")

if __name__ == '__main__':
    main()
