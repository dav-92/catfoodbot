import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import func

from config import settings
from database import get_session, Product, PriceHistory, init_db
from scraper import scrape_all_async, ScrapedProduct, ZooplusScraper
from database import get_or_create_preferences

logger = logging.getLogger(__name__)


def save_product(scraped: ScrapedProduct) -> tuple[int, bool]:
    """Save or update a product in the database. Returns (product_id, is_new)."""
    session = get_session()
    try:
        # Check if product exists
        product = session.query(Product).filter(
            Product.external_id == scraped.external_id,
            Product.site == scraped.site
        ).first()

        is_new = False
        if not product:
            # Create new product
            product = Product(
                external_id=scraped.external_id,
                base_product_id=scraped.base_product_id,
                variant_name=scraped.variant_name,
                name=scraped.name,
                brand=scraped.brand,
                size=scraped.size,
                url=scraped.url,
                site=scraped.site,
                is_wet_food=True
            )
            session.add(product)
            session.commit()
            is_new = True
            logger.info(f"New product: {product.name} (variant: {scraped.variant_name})")
        else:
            # Update existing product
            product.name = scraped.name
            product.brand = scraped.brand or product.brand
            product.size = scraped.size or product.size
            product.url = scraped.url
            product.base_product_id = scraped.base_product_id or product.base_product_id
            product.variant_name = scraped.variant_name or product.variant_name
            product.updated_at = datetime.utcnow()
            session.commit()

        return product.id, is_new

    finally:
        session.close()


def save_price(product_id: int, scraped: ScrapedProduct) -> int:
    """Record a price point for a product. Returns price_id."""
    session = get_session()
    try:
        price = PriceHistory(
            product_id=product_id,
            current_price=scraped.current_price,
            original_price=scraped.original_price,
            is_on_sale=scraped.is_on_sale,
            sale_tag=scraped.sale_tag,
            original_price_per_kg=scraped.original_price_per_kg,
            reduced_price_per_kg=scraped.reduced_price_per_kg
        )
        session.add(price)
        session.commit()
        session.refresh(price)
        return price.id

    finally:
        session.close()


def get_historical_average(product_id: int, days: int = 30) -> Optional[float]:
    """Get the average price for a product over the last N days."""
    session = get_session()
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        result = session.query(func.avg(PriceHistory.current_price)).filter(
            PriceHistory.product_id == product_id,
            PriceHistory.recorded_at >= cutoff
        ).scalar()

        return result

    finally:
        session.close()


def check_for_price_drop(product: Product, price: PriceHistory) -> bool:
    """Check if the current price is lower than historical average."""
    avg_price = get_historical_average(product.id)

    if not avg_price:
        # No historical data yet
        return False

    drop_percent = ((avg_price - price.current_price) / avg_price) * 100

    if drop_percent > 0:
        logger.info(
            f"Price drop detected for {product.name}: "
            f"{price.current_price}€ vs avg {avg_price:.2f}€ ({drop_percent:.1f}% off)"
        )
        return True

    return False


def check_under_max_price(product: Product, price: PriceHistory, chat_id: str) -> bool:
    """Check if product is under user's max price threshold and from a watched brand."""
    prefs = get_or_create_preferences(chat_id)

    # Must have max price set
    if prefs.max_price_per_kg is None:
        return False

    # Must be from a watched brand
    if not prefs.should_notify_for_brand(product.brand):
        return False

    # Calculate price per kg - use reduced if available, otherwise calculate from current price
    price_per_kg = price.reduced_price_per_kg
    if price_per_kg is None and price.original_price_per_kg is not None:
        # If there's a discount, calculate the reduced price per kg
        if price.discount_percent > 0:
            price_per_kg = round(price.original_price_per_kg * (1 - price.discount_percent / 100), 2)
        else:
            price_per_kg = price.original_price_per_kg

    if price_per_kg is None:
        return False

    # Check if under threshold
    if price_per_kg <= prefs.max_price_per_kg:
        logger.info(
            f"Good price found for {product.name}: "
            f"{price_per_kg:.2f}€/kg <= {prefs.max_price_per_kg:.2f}€/kg threshold"
        )
        return True

    return False


async def process_products(products: list[ScrapedProduct]) -> dict:
    """Process scraped products, save to DB, and send alerts."""
    from notifier import send_alerts_grouped

    stats = {
        "total": len(products),
        "new_products": 0,
        "price_updates": 0,
        "alerts_sent": 0,
        "on_sale": 0
    }

    # Collect products that should be alerted (for grouped sending)
    products_to_alert = []

    for scraped in products:
        try:
            # Save product and get its ID
            product_id, is_new = save_product(scraped)
            if is_new:
                stats["new_products"] += 1

            # Save price and get its ID
            price_id = save_price(product_id, scraped)
            stats["price_updates"] += 1

            if scraped.is_on_sale:
                stats["on_sale"] += 1

            # Check if we should send an alert using fresh session
            session = get_session()
            try:
                fresh_product = session.query(Product).get(product_id)
                fresh_price = session.query(PriceHistory).get(price_id)

                if not fresh_product or not fresh_price:
                    continue

                # Determine if this product might be worth alerting about
                should_alert = False

                # Alert if explicitly on sale with any discount
                if fresh_price.is_on_sale and fresh_price.discount_percent > 0:
                    should_alert = True

                # Also alert if price dropped compared to historical average
                if not should_alert and check_for_price_drop(fresh_product, fresh_price):
                    should_alert = True

                # Also alert if product has a price per kg (could be under someone's threshold)
                price_per_kg = fresh_price.reduced_price_per_kg or fresh_price.original_price_per_kg
                if not should_alert and price_per_kg is not None:
                    should_alert = True

                if should_alert:
                    # Collect for grouped sending instead of sending immediately
                    products_to_alert.append((fresh_product.id, fresh_price.id))
            finally:
                session.close()

        except Exception as e:
            logger.error(f"Error processing product {scraped.name}: {e}")

    # Send alerts grouped by base_product_id + price
    if products_to_alert:
        alerts_count = await send_alerts_grouped(products_to_alert)
        stats["alerts_sent"] = alerts_count

    return stats


async def run_check():
    """Run a full price check cycle."""
    logger.info("Starting price check...")

    # Initialize database
    init_db()

    # Get all watched brands and lowest max price from all users
    session = get_session()
    try:
        from database import UserPreferences
        all_users = session.query(UserPreferences).filter(
            UserPreferences.alerts_enabled == True
        ).all()

        # Collect all unique watched brands from all users
        watched_brands = set()
        price_ceiling = None  # Track highest max_price (to include products for all users)
        for user in all_users:
            watched_brands.update(user.get_brands_list())
            if user.max_price_per_kg is not None:
                if price_ceiling is None or user.max_price_per_kg > price_ceiling:
                    price_ceiling = user.max_price_per_kg

        logger.info(f"Scraping for {len(all_users)} users, {len(watched_brands)} brands")
    finally:
        session.close()

    # Scrape products (including watched brands specifically, filtered by max price)
    products = await scrape_all_async(watched_brands=list(watched_brands), max_price_per_kg=price_ceiling)

    if not products:
        logger.warning("No products scraped!")
        return

    # Process and send alerts
    stats = await process_products(products)

    logger.info(
        f"Check complete: {stats['total']} products, "
        f"{stats['new_products']} new, {stats['on_sale']} on sale, "
        f"{stats['alerts_sent']} alerts sent"
    )

    return stats


def run_check_sync():
    """Synchronous wrapper for run_check."""
    return asyncio.run(run_check())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    run_check_sync()
