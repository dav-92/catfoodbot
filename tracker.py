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

# Global state to track if a check is currently running
_is_running = False
_last_run_start = None

def is_check_running() -> bool:
    """Check if a price check is currently in progress."""
    return _is_running

def get_last_run_start() -> Optional[datetime]:
    """Get the start time of the last run."""
    return _last_run_start



def save_product(scraped: ScrapedProduct, session=None) -> tuple[int, bool]:
    """Save or update a product in the database. Returns (product_id, is_new)."""
    close_session = False
    if not session:
        session = get_session()
        close_session = True
        
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
            # Flush to get ID if we are in a transaction
            session.flush()
            if close_session:
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
            if close_session:
                session.commit()

        return product.id, is_new

    finally:
        if close_session:
            session.close()


def save_price(product_id: int, scraped: ScrapedProduct, session=None) -> int:
    """Record a price point for a product. Returns price_id."""
    close_session = False
    if not session:
        session = get_session()
        close_session = True
        
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
        session.flush() # Ensure ID is generated
        if close_session:
            session.commit()
            session.refresh(price)
        return price.id

    finally:
        if close_session:
            session.close()


def get_historical_average(product_id: int, days: int = 30, session=None) -> Optional[float]:
    """Get the average price for a product over the last N days."""
    close_session = False
    if not session:
        session = get_session()
        close_session = True
        
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        result = session.query(func.avg(PriceHistory.current_price)).filter(
            PriceHistory.product_id == product_id,
            PriceHistory.recorded_at >= cutoff
        ).scalar()

        return result

    finally:
        if close_session:
            session.close()


def check_for_price_drop(product: Product, price: PriceHistory, session=None) -> bool:
    """Check if the current price is lower than historical average."""
    avg_price = get_historical_average(product.id, session=session)

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
    from services.alert_service import send_alerts_grouped

    stats = {
        "total": len(products),
        "new_products": 0,
        "price_updates": 0,
        "alerts_sent": 0,
        "on_sale": 0
    }

    # Run the heavy synchronous DB operations in a separate thread
    batch_stats, products_to_alert = await asyncio.to_thread(_save_products_batch_sync, products)
    
    # Merge stats
    stats["new_products"] = batch_stats["new_products"]
    stats["price_updates"] = batch_stats["price_updates"]
    stats["on_sale"] = batch_stats["on_sale"]

    # Send alerts grouped by base_product_id + price
    if products_to_alert:
        alerts_count = await send_alerts_grouped(products_to_alert)
        stats["alerts_sent"] = alerts_count

    return stats


def _save_products_batch_sync(products: list[ScrapedProduct]) -> tuple[dict, list]:
    """
    Synchronous function to save a batch of products to DB.
    Should be run in a separate thread to avoid blocking the event loop.
    Returns (stats_dict, list_of_alert_tuples).
    """
    stats = {
        "new_products": 0,
        "price_updates": 0,
        "on_sale": 0
    }
    products_to_alert = []

    # Use a single session for all processing
    session = get_session()
    try:
        for scraped in products:
            try:
                # Save product and get its ID (using shared session)
                product_id, is_new = save_product(scraped, session=session)
                if is_new:
                    stats["new_products"] += 1

                # Save price and get its ID (using shared session)
                price_id = save_price(product_id, scraped, session=session)
                stats["price_updates"] += 1

                if scraped.is_on_sale:
                    stats["on_sale"] += 1

                # Determine if we should alert
                # We need to query specific objects to ensure they are attached to session
                fresh_price = session.query(PriceHistory).get(price_id)
                fresh_product = session.query(Product).get(product_id)

                if not fresh_product or not fresh_price:
                    continue

                should_alert = False

                # Alert if explicitly on sale with any discount
                if fresh_price.is_on_sale and fresh_price.discount_percent > 0:
                    should_alert = True

                # Also alert if price dropped compared to historical average
                if not should_alert and check_for_price_drop(fresh_product, fresh_price, session=session):
                    should_alert = True

                # Also alert if product has a price per kg (could be under someone's threshold)
                price_per_kg = fresh_price.reduced_price_per_kg or fresh_price.original_price_per_kg
                if not should_alert and price_per_kg is not None:
                    should_alert = True

                if should_alert:
                    # Collect for grouped sending instead of sending immediately
                    products_to_alert.append((fresh_product.id, fresh_price.id))

            except Exception as e:
                logger.error(f"Error processing product {scraped.name}: {e}")
                session.rollback()
        
        # Commit all changes at the end
        session.commit()
        
    finally:
        session.close()

    return stats, products_to_alert


async def run_check():
    """Run a full price check cycle."""
    global _is_running, _last_run_start
    _is_running = True
    _last_run_start = datetime.utcnow()
    
    try:
        logger.info("Starting price check...")

        # Initialize database
        init_db()

        # Fixed scraping parameters: All brands, up to 10€/kg
        # This decouples scraping from user settings
        logger.info("Scraping ALL brands up to 10.00€/kg")

        # Accumulate stats across chunks
        total_stats = {
            "total": 0,
            "new_products": 0,
            "on_sale": 0,
            "alerts_sent": 0
        }

        async def handle_chunk(products: list[ScrapedProduct]):
            """Process a chunk of products immediately."""
            if not products:
                return
            
            chunk_stats = await process_products(products)
            
            # Update total stats
            total_stats["total"] += chunk_stats["total"]
            total_stats["new_products"] += chunk_stats["new_products"]
            total_stats["on_sale"] += chunk_stats["on_sale"]
            total_stats["alerts_sent"] += chunk_stats["alerts_sent"]
            
            logger.info(f"Processed chunk: {chunk_stats['total']} products, {chunk_stats['alerts_sent']} new alerts")

        # Scrape products with streaming callback
        await scrape_all_async(
            watched_brands=None,  # None = scrape all quality brands
            max_price_per_kg=10.0,
            on_chunk_callback=handle_chunk
        )

        logger.info(
            f"Check complete: {total_stats['total']} products, "
            f"{total_stats['new_products']} new, {total_stats['on_sale']} on sale, "
            f"{total_stats['alerts_sent']} alerts sent"
        )

        return total_stats
    finally:
        _is_running = False


def cleanup_old_offers(days_retention: int = 7) -> int:
    """Delete PriceHistory records older than N days."""
    session = get_session()
    try:
        cutoff = datetime.utcnow() - timedelta(days=days_retention)
        deleted_count = session.query(PriceHistory).filter(
            PriceHistory.recorded_at < cutoff
        ).delete()
        session.commit()
        if deleted_count > 0:
            logger.info(f"Cleanup: Deleted {deleted_count} old price records (< {cutoff.date()})")
        return deleted_count
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        session.rollback()
        return 0
    finally:
        session.close()



def run_check_sync():
    """Synchronous wrapper for run_check."""
    return asyncio.run(run_check())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    run_check_sync()
