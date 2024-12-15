import os
import sys
import time
import logging
import datetime
import urllib3
import certifi  # For SSL verification
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
    Enhanced URL fetching with comprehensive error handling.
    
    Args:
        url (str): URL to fetch
        timeout (int): Request timeout in seconds
    
    Returns:
        requests.Response: HTTP response
    """
    try:
        # Validate URL before request
        if not url or not url.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid URL: {url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.google.com/'
        }
        
        logger.info(f"Attempting to fetch URL: {url}")
        
        response = requests.get(
            url, 
            headers=headers,
            verify=certifi.where(),  # Use certifi for SSL verification
            timeout=timeout
        )
        response.raise_for_status()
        
        logger.info(f"Successfully fetched URL: {url}")
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Detailed request error for {url}: {e}")
        logger.error(f"Error type: {type(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching {url}: {e}")
        logger.error(f"Error type: {type(e)}")
        raise

def fetch_current_affairs_links():
    """
    Fetch current affairs links with comprehensive error handling and debugging.
    
    Returns:
        list: URLs of current affairs
    """
    url = "https://www.indiabix.com/current-affairs/questions-and-answers/"
    try:
        logger.info(f"Attempting to fetch links from: {url}")
        response = fetch_url_with_retry(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        current_date = datetime.datetime.now().strftime('%Y-%m')
        logger.info(f"Searching for links matching current date pattern: {current_date}")
        
        all_links = soup.find_all('a', class_='text-link me-3')
        links = [link.get('href') for link in all_links if link.get('href') and current_date in link.get('href')]
        
        logger.info(f"Filtered links count: {len(links)}")
        return links
    except Exception as e:
        logger.error(f"Error fetching current affairs links: {e}")
        return []

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
                question_text = container.find('div', class_='bix-td-qtxt').text.strip()
                correct_answer = container.find('input', {'class': 'jq-hdnakq'}).get('value', '').strip()
                explanation_div = container.find('div', class_='bix-ans-description')
                explanation_text = explanation_div.text.strip() if explanation_div else "No explanation available"

                message += f"❓ *Question:* {question_text}\n"
                message += f"🎯 *Correct Answer:* {correct_answer}\n"
                message += f"💡 *Explanation:* {explanation_text}\n\n"
                message += "--------------------------------------\n\n"
            except Exception as inner_e:
                logger.error(f"Error processing individual question: {inner_e}")

        return message
    except Exception as e:
        logger.error(f"Unexpected error extracting questions: {e}")
        return None

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
        payload = {
            'chat_id': channel,
            'text': escape_markdown_v2(message),
            'parse_mode': 'MarkdownV2',
            'disable_web_page_preview': True
        }
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
            data=payload, 
            timeout=15,
            verify=certifi.where()  # Use certifi for secure API calls
        )
        response.raise_for_status()
        return response.json().get('result', {}).get('message_id')
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")
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
        message = extract_current_affairs_questions(soup, url)
        if not message:
            logger.warning(f"No questions extracted from URL: {url}")
            return False

        promotional_message = "\n\n🚀 Join [Daily Current Affairs](https://t.me/daily_current_all_source) 🌟"
        send_telegram_message(message + promotional_message, ENGLISH_CHANNEL)

        if collection:
            collection.insert_one({"url": url, "processed_at": datetime.datetime.utcnow()})

        return True
    except Exception as e:
        logger.error(f"Error processing URL {url}: {e}")
        return False

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
            if collection and collection.find_one({"url": url}):
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
