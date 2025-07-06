"""
Telegram Bot Configuration
Handles Telegram bot setup and message sending

Requirements:
pip install python-telegram-bot requests python-dotenv

To get your Telegram bot token and chat ID:
1. Create a bot using @BotFather on Telegram
2. Get your chat ID using @userinfobot or by sending a message to your bot
   and checking: https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates

For better security, use environment variables:
1. Copy .env.template to .env
2. Fill in your actual values in .env
3. The .env file will be automatically loaded
"""

import os
import requests
import logging
from typing import Optional

# Try to load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not installed, will use direct values
    pass

# Configure your Telegram bot here
# Get configuration from environment variables or use defaults
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'YOUR_BOT_TOKEN_HERE')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', 'YOUR_CHAT_ID_HERE')

# Optional: Multiple chat IDs for group notifications
# Can be set via TELEGRAM_BROADCAST_IDS env var (comma-separated)
broadcast_ids = os.getenv('TELEGRAM_BROADCAST_IDS', '')
if broadcast_ids:
    TELEGRAM_CHAT_IDS = [TELEGRAM_CHAT_ID] + [id.strip() for id in broadcast_ids.split(',') if id.strip()]
else:
    TELEGRAM_CHAT_IDS = [
        TELEGRAM_CHAT_ID,  # Primary chat ID
        # "ANOTHER_CHAT_ID",  # Add more chat IDs if needed
    ]

# Telegram API URL
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Set up logging
logger = logging.getLogger(__name__)

def send_telegram_message(
    message: str, 
    chat_id: Optional[str] = None, 
    parse_mode: Optional[str] = 'Markdown',
    disable_notification: bool = False
) -> bool:
    """
    Send a message via Telegram bot
    
    Args:
        message (str): Message to send
        chat_id (str, optional): Chat ID to send to. Uses default if not provided
        parse_mode (str, optional): Parse mode ('Markdown' or 'HTML')
        disable_notification (bool): Whether to send silently
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Use provided chat_id or default
        target_chat_id = chat_id or TELEGRAM_CHAT_ID
        
        # Prepare the request
        url = f"{TELEGRAM_API_URL}/sendMessage"
        payload = {
            'chat_id': target_chat_id,
            'text': message,
            'parse_mode': parse_mode,
            'disable_notification': disable_notification
        }
        
        # Send the request
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code == 200:
            logger.info(f"✅ Telegram message sent successfully to {target_chat_id}")
            return True
        else:
            logger.error(f"❌ Failed to send Telegram message: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error sending Telegram message: {e}")
        return False

def send_telegram_photo(
    photo_path: str,
    caption: Optional[str] = None,
    chat_id: Optional[str] = None,
    parse_mode: Optional[str] = 'Markdown'
) -> bool:
    """
    Send a photo via Telegram bot
    
    Args:
        photo_path (str): Path to the photo file
        caption (str, optional): Photo caption
        chat_id (str, optional): Chat ID to send to
        parse_mode (str, optional): Parse mode for caption
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        target_chat_id = chat_id or TELEGRAM_CHAT_ID
        url = f"{TELEGRAM_API_URL}/sendPhoto"
        
        with open(photo_path, 'rb') as photo:
            files = {'photo': photo}
            data = {
                'chat_id': target_chat_id,
                'caption': caption,
                'parse_mode': parse_mode
            }
            
            response = requests.post(url, files=files, data=data, timeout=30)
            
        if response.status_code == 200:
            logger.info(f"✅ Telegram photo sent successfully to {target_chat_id}")
            return True
        else:
            logger.error(f"❌ Failed to send Telegram photo: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error sending Telegram photo: {e}")
        return False

def send_telegram_document(
    document_path: str,
    caption: Optional[str] = None,
    chat_id: Optional[str] = None
) -> bool:
    """
    Send a document via Telegram bot
    
    Args:
        document_path (str): Path to the document file
        caption (str, optional): Document caption
        chat_id (str, optional): Chat ID to send to
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        target_chat_id = chat_id or TELEGRAM_CHAT_ID
        url = f"{TELEGRAM_API_URL}/sendDocument"
        
        with open(document_path, 'rb') as doc:
            files = {'document': doc}
            data = {
                'chat_id': target_chat_id,
                'caption': caption
            }
            
            response = requests.post(url, files=files, data=data, timeout=30)
            
        if response.status_code == 200:
            logger.info(f"✅ Telegram document sent successfully to {target_chat_id}")
            return True
        else:
            logger.error(f"❌ Failed to send Telegram document: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error sending Telegram document: {e}")
        return False

def broadcast_telegram_message(
    message: str,
    parse_mode: Optional[str] = 'Markdown',
    disable_notification: bool = False
) -> dict:
    """
    Broadcast a message to all configured chat IDs
    
    Args:
        message (str): Message to broadcast
        parse_mode (str, optional): Parse mode
        disable_notification (bool): Whether to send silently
        
    Returns:
        dict: Results for each chat ID
    """
    results = {}
    
    for chat_id in TELEGRAM_CHAT_IDS:
        success = send_telegram_message(
            message=message,
            chat_id=chat_id,
            parse_mode=parse_mode,
            disable_notification=disable_notification
        )
        results[chat_id] = success
    
    return results

def test_telegram_connection():
    """Test Telegram bot connection"""
    test_message = """
🧪 *Telegram Bot Test*

✅ Connection successful!
🤖 Bot is ready to send notifications.

_This is a test message._
"""
    
    success = send_telegram_message(test_message)
    
    if success:
        print("✅ Telegram test successful! Check your Telegram for the message.")
    else:
        print("❌ Telegram test failed! Check your bot token and chat ID.")
        print(f"   Bot Token: {TELEGRAM_BOT_TOKEN[:10]}...")
        print(f"   Chat ID: {TELEGRAM_CHAT_ID}")

# Run test if executed directly
if __name__ == "__main__":
    import sys
    
    if TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ Error: Please update TELEGRAM_BOT_TOKEN in telegram_config.py or set in .env file")
        sys.exit(1)
    
    if TELEGRAM_CHAT_ID == "YOUR_CHAT_ID_HERE":
        print("❌ Error: Please update TELEGRAM_CHAT_ID in telegram_config.py or set in .env file")
        sys.exit(1)
    
    print("🧪 Testing Telegram configuration...")
    test_telegram_connection()
