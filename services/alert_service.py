import logging
import asyncio
from telegram import Bot

from config import settings
from database import get_session, Product, PriceHistory, AlertSent, UserPreferences
from scraper import ZooplusScraper
from bot.formatter import format_alert_message, format_cheapest_variant_alert
from services.deal_service import find_cheapest_variants

logger = logging.getLogger(__name__)

async def send_message_to_user(chat_id: str, message: str) -> bool:
    """Send a formatted message to a user via Telegram."""
    if not settings.telegram_bot_token:
        return False

    try:
        bot = Bot(token=settings.telegram_bot_token)
        await bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode="Markdown",
            disable_web_page_preview=False
        )
        return True
    except Exception as e:
        logger.error(f"Failed to send message to {chat_id}: {e}")
        return False

async def send_alert_to_user(product: Product, price: PriceHistory, chat_id: str, max_price_per_kg: float = None) -> bool:
    """Send a Telegram alert for a product to a specific user."""
    if not settings.telegram_bot_token:
        logger.warning("Telegram bot token not configured")
        return False

    try:
        bot = Bot(token=settings.telegram_bot_token)
        message = format_alert_message(product, price, max_price_per_kg)

        await bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode="Markdown",
            disable_web_page_preview=False
        )

        logger.info(f"Alert sent to {chat_id} for {product.name}")
        return True

    except Exception as e:
        logger.error(f"Failed to send alert to {chat_id}: {e}")
        return False

async def send_alerts_grouped(product_price_ids: list[tuple[int, int]]) -> int:
    """
    Send alerts for products, only alerting for the cheapest variant per product.
    Args:
        product_price_ids: List of (product_id, price_id) tuples to alert
    Returns:
        Total number of alerts sent
    """
    if not settings.telegram_bot_token or not product_price_ids:
        return 0

    session = get_session()
    alerts_sent = 0

    try:
        # Load all products and prices
        products_data = []
        for product_id, price_id in product_price_ids:
            product = session.query(Product).get(product_id)
            price = session.query(PriceHistory).get(price_id)
            if product and price:
                ppkg = price.reduced_price_per_kg or price.original_price_per_kg
                products_data.append((product, price, ppkg))

        # Get all configured users
        all_users = session.query(UserPreferences).filter(
            UserPreferences.max_price_per_kg != None
        ).all()

        for prefs in all_users:
            # Filter products for this user (brand + price threshold)
            user_products = []
            for product, price, ppkg in products_data:
                if not prefs.should_notify_for_brand(product.brand):
                    continue
                if ppkg is None or ppkg > prefs.max_price_per_kg:
                    continue
                user_products.append((product, price, ppkg))

            if not user_products:
                continue

            # Find cheapest variants
            cheapest_deals = find_cheapest_variants(user_products)

            for cheapest_product, cheapest_price, _, other_sites in cheapest_deals:
                # Check if already alerted for this cheapest variant
                existing = session.query(AlertSent).filter(
                    AlertSent.product_id == cheapest_product.id,
                    AlertSent.price_at_alert == cheapest_price.current_price,
                    AlertSent.chat_id == prefs.chat_id
                ).first()
                if existing:
                    continue

                # Send alert for cheapest variant only
                message = format_cheapest_variant_alert(
                    cheapest_product, cheapest_price, prefs.max_price_per_kg, other_sites
                )
                success = await send_message_to_user(prefs.chat_id, message)

                if success:
                    alert = AlertSent(
                        product_id=cheapest_product.id,
                        price_at_alert=cheapest_price.current_price,
                        chat_id=prefs.chat_id
                    )
                    session.add(alert)
                    session.commit()
                    alerts_sent += 1
                    logger.info(f"Alert sent to {prefs.chat_id} for {cheapest_product.base_product_id}")

    finally:
        session.close()

    return alerts_sent

async def background_scrape_and_notify(chat_id: str, prefs: UserPreferences, new_ceiling: float, specific_brands: list[str] = None) -> None:
    """
    Run a background scrape for a higher price range and notify user of new deals.
    """
    try:
        logger.info(f"Starting background scrape for chat_id={chat_id}, new_ceiling={new_ceiling}, specific_brands={specific_brands}")
        
        # Note: Ideally this should use the shared browser instance from main, 
        # but for on-demand scraped triggered by user commands, a fresh instance is acceptable 
        # (or pass one if we restructure dependency injection). 
        # For now, we instantiate scraper which will launch its own browser (legacy mode warning but functional).
        scraper = ZooplusScraper()
        
        if specific_brands:
            brands_to_scrape = specific_brands
            include_defaults = False
        else:
            brands_to_scrape = prefs.get_brands_list()
            include_defaults = True

        # Scrape with new price ceiling
        products = await scraper.scrape_brand_products(
            brands_to_scrape if brands_to_scrape else None,
            max_price_per_kg=new_ceiling,
            include_default_brands=include_defaults
        )

        if not products:
            logger.info(f"Background scrape found no new products")
            return

        # Filter to only products within the new range
        session = get_session()
        try:
            new_deals_count = 0
            
            for scraped in products:
                price_per_kg = scraped.reduced_price_per_kg or scraped.original_price_per_kg

                if price_per_kg is None or price_per_kg > prefs.max_price_per_kg:
                    continue

                if not prefs.should_notify_for_brand(scraped.brand):
                    continue

                # Check if we already have this product
                existing_product = session.query(Product).filter(
                    Product.external_id == scraped.external_id
                ).first()

                if existing_product:
                    # Check if already alerted
                    existing_alert = session.query(AlertSent).filter(
                        AlertSent.product_id == existing_product.id,
                        AlertSent.price_at_alert == scraped.current_price,
                        AlertSent.chat_id == chat_id
                    ).first()
                    if existing_alert:
                        continue
                
                new_deals_count += 1

                if new_deals_count <= 5:  # Limit background notifications
                    msg = f"🔔 *New deal found!*\n\n"
                    msg += f"*{scraped.brand or 'Unknown'}* - {scraped.name}\n"
                    if scraped.size:
                        msg += f"📦 {scraped.size}\n"
                    msg += f"\n💰 *{scraped.current_price:.2f}€*"
                    if price_per_kg:
                        msg += f"\n📊 *{price_per_kg:.2f}€/kg*"
                    site_name = scraped.site.capitalize() if scraped.site else "Store"
                    msg += f"\n\n🔗 [View on {site_name}]({scraped.url})"

                    success = await send_message_to_user(chat_id, msg)

                    if success and existing_product:
                        alert = AlertSent(
                            product_id=existing_product.id,
                            price_at_alert=scraped.current_price,
                            chat_id=chat_id
                        )
                        session.add(alert)
                        session.commit()

            if new_deals_count > 5:
                await send_message_to_user(chat_id, f"🔔 Found {new_deals_count - 5} more deals!")
            elif new_deals_count > 0:
                logger.info(f"Background scrape sent {new_deals_count} new deals to {chat_id}")
            else:
                logger.info(f"Background scrape found no new deals for {chat_id}")

        finally:
            session.close()

    except Exception as e:
        logger.error(f"Background scrape failed: {e}")
