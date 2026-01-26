# Claude Code Memory for CatFoodBot

## ⚠️ CRITICAL DEPLOYMENT INSTRUCTION
**You MUST rebuild the container to apply ANY code changes.**
Can NOT just restart. The code is copied into the image, not mounted.
```bash
docker-compose up --build -d
```

## Project Structure
- `main.py` - Entry point, starts Scheduler and Telegram Bot
- `bot/` - Telegram bot implementation
    - `application.py` - Bot setup and handlers registration
    - `handlers.py` - Command handlers (`/status`, `/scrape`, etc.)
- `services/` - Business logic
    - `alert_service.py` - Logic for sending alerts
    - `deal_service.py` - Logic for finding deals
- `scraper.py` - Async scrapers for Zooplus, Bitiba, Zooroyal, etc.
- `tracker.py` - Orchestrates scraping and DB updates (runs in background)
- `database.py` - SQLAlchemy models (`WatchedBrand`, `UserPreferences` with eager loading)
- `migrate_brands.py` - Script to migrate legacy comma-separated brands to `WatchedBrand` table

## Common Tasks
- **Manual Scrape**: Send `/scrape` to the bot.
- **Check Status**: Send `/status` to see last check time and next scheduled run.
- **Add Brand**: `/addbrands macs wild freedom` (automatically normalizes matches, comma or space separated)
- **Remove Brand**: `/removebrands macs`

## Maintenance
- **Database Cleanup**: Runs automatically every 24 hours (first run 10 minutes after startup) to delete `PriceHistory` records older than 7 days.
- **Migration**: Run `python migrate_brands.py` inside the container if upgrading from v1.

## Architecture Notes
- **Async Only**: HTML parsing and DB commits are offloaded to threads (`asyncio.to_thread`) to prevent blocking the event loop.
- **Database**: Uses `joinedload` for `UserPreferences.brands` to prevent `DetachedInstanceError`.

## Agent Tools
- **Get Statistics**: To get product counts grouped by site and brand, import and use the function:
  ```python
  from database import get_product_statistics
  stats = get_product_statistics()
  # Returns a list of dictionaries like:
  # [
  #   {'site': 'zooplus', 'brand': 'MACs', 'count': 42},
  #   {'site': 'zooplus', 'brand': 'Feringa', 'count': 15},
  # ]
  ```
