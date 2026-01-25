from typing import Optional
import logging
from telegram import Bot
from telegram.ext import Application, CommandHandler

from config import settings
from bot.handlers import (
    start_command, brands_command, addbrand_command, removebrand_command,
    listbrands_command, setmaxprice_command, status_command,
    reset_command, scrape_command, help_command
)

logger = logging.getLogger(__name__)

def create_bot_application() -> Optional[Application]:
    """Create the Telegram bot application."""
    if not settings.telegram_bot_token:
        logger.warning("No Telegram bot token configured")
        return None

    app = Application.builder().token(settings.telegram_bot_token).build()

    # Add command handlers
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("brands", brands_command))
    app.add_handler(CommandHandler("addbrand", addbrand_command))
    app.add_handler(CommandHandler("removebrand", removebrand_command))
    app.add_handler(CommandHandler("listbrands", listbrands_command))
    app.add_handler(CommandHandler("setmaxprice", setmaxprice_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("reset", reset_command))
    app.add_handler(CommandHandler("scrape", scrape_command))
    app.add_handler(CommandHandler("help", help_command))
    return app

async def send_test_message():
    """Send a test message to verify configuration."""
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        print("Error: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set in .env")
        return False

    try:
        bot = Bot(token=settings.telegram_bot_token)
        await bot.send_message(
            chat_id=settings.telegram_chat_id,
            text="✅ Cat Food Alert Bot is connected!\n\nUse /help to see available commands."
        )
        print("Test message sent successfully!")
        return True
    except Exception as e:
        print(f"Failed to send test message: {e}")
        return False
