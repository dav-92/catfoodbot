# Claude Code Memory for catfood project

## Important Reminders

### Always restart the bot after code changes!
After modifying any Python files (notifier.py, scraper.py, main.py, database.py, etc.), always restart the bot:
```bash
pkill -f "python3.*main.py" 2>/dev/null; sleep 1; nohup python3 /root/catfood/main.py > /root/catfood/bot.log 2>&1 &
```

## Project Structure
- `main.py` - Entry point, runs scheduler and bot
- `notifier.py` - Telegram bot commands and alert formatting
- `scraper.py` - Zooplus/Bitiba/Zooroyal price scrapers (3 sites, parallel scraping)
- `database.py` - SQLAlchemy models (Product, PriceHistory, AlertSent, UserPreferences)
- `config.py` - Settings from environment variables
- `tracker.py` - Price tracking logic, run_check() for scheduled scrapes

## Commands
- `/scrape` - Manually trigger full scrape of all 3 sites
- `/reset` - Clear alert history and re-send existing deals
- `/setmaxprice` - Set max price per kg filter
- `/addbrand` / `/removebrand` - Manage watched brands
