#!/usr/bin/env python3
"""
Cat Food Sale Alert - Main Entry Point

Monitors Zooplus.de for wet cat food sales and sends Telegram notifications.
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import settings
from database import init_db
from database import init_db
from tracker import run_check, cleanup_old_offers
from bot.application import create_bot_application, send_test_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("catfood.log")
    ]
)
logger = logging.getLogger(__name__)


async def scheduled_check():
    """Run the scheduled price check."""
    logger.info("Running scheduled price check...")
    try:
        stats = await run_check()
        if stats:
            logger.info(f"Check completed: {stats}")
    except Exception as e:
        logger.error(f"Error during scheduled check: {e}")



async def scheduled_cleanup():
    """Run the daily cleanup of old offers."""
    logger.info("Running scheduled database cleanup...")
    try:
        # Run synchronous cleanup in thread
        count = await asyncio.to_thread(cleanup_old_offers, days_retention=7)
        if count > 0:
            logger.info(f"Cleanup finished: Removed {count} old records")
    except Exception as e:
        logger.error(f"Error during scheduled cleanup: {e}")


async def run_bot_and_scheduler():
    """Run both the Telegram bot and the scheduler."""
    # Initialize database
    init_db()
    logger.info("Database initialized")

    # Validate configuration
    if not settings.telegram_bot_token:
        logger.error("TELEGRAM_BOT_TOKEN not set! Please configure .env file.")
        sys.exit(1)

    if not settings.telegram_chat_id:
        logger.warning("TELEGRAM_CHAT_ID not set - alerts will not be sent until configured")

    # Create scheduler
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        scheduled_check,
        trigger=IntervalTrigger(hours=settings.check_interval_hours),
        id="price_check",
        name="Hourly price check",
        next_run_time=datetime.now()  # Run immediately on start
    )
    
    # Schedule cleanup daily
    scheduler.add_job(
        scheduled_cleanup,
        trigger=IntervalTrigger(days=1),
        id="db_cleanup",
        name="Daily database cleanup",
        # Start cleanup 10 mins after startup to not interfere with initial check
        next_run_time=datetime.now().replace(microsecond=0) + timedelta(minutes=10) 
    )

    scheduler.start()
    logger.info(f"Scheduler started - checking every {settings.check_interval_hours} hour(s)")

    # Create and run bot
    app = create_bot_application()
    if app:
        logger.info("Starting Telegram bot...")
        await app.initialize()
        await app.start()
        await app.updater.start_polling()

        # Keep running until interrupted
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
            scheduler.shutdown()
    else:
        # Run without bot, just scheduler
        logger.info("Running in scheduler-only mode (no bot token)")
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            scheduler.shutdown()


def run_once():
    """Run a single price check (useful for testing)."""
    init_db()
    asyncio.run(run_check())


def test_telegram():
    """Test Telegram connection."""
    asyncio.run(send_test_message())


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "once":
            print("Running single check...")
            run_once()
        elif command == "test":
            print("Testing Telegram connection...")
            test_telegram()
        elif command == "init":
            print("Initializing database...")
            init_db()
            print("Done!")
        else:
            print(f"Unknown command: {command}")
            print("Usage: python main.py [once|test|init]")
            sys.exit(1)
    else:
        print("Starting Cat Food Sale Alert...")
        print(f"Check interval: {settings.check_interval_hours} hour(s)")
        print("-" * 40)
        asyncio.run(run_bot_and_scheduler())


if __name__ == "__main__":
    main()
