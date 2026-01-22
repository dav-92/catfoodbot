import logging
import asyncio
from typing import Optional
from datetime import datetime, timedelta

from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes

from config import settings
from database import (
    get_session, Product, PriceHistory, AlertSent, UserPreferences,
    get_or_create_preferences
)
from scraper import ZooplusScraper

logger = logging.getLogger(__name__)

# Available brands for reference
AVAILABLE_BRANDS = ZooplusScraper.BRANDS


# =============================================================================
# Helper functions for instant DB-based responses
# =============================================================================

def _get_deals_from_db(prefs: UserPreferences, session, brands_filter: list[str] = None, limit: int = 100) -> list[tuple]:
    """
    Query existing deals from database matching user preferences.
    Returns list of (product, price, price_per_kg) tuples sorted by price_per_kg.

    Args:
        prefs: User preferences with max_price_per_kg and brand settings
        session: Database session
        brands_filter: Optional list of specific brands to filter (overrides prefs)
        limit: Maximum number of deals to return
    """
    if prefs.max_price_per_kg is None:
        return []

    from sqlalchemy import func

    # Subquery to get the latest PriceHistory record for each product
    latest_price_subq = session.query(
        PriceHistory.product_id,
        func.max(PriceHistory.recorded_at).label('max_recorded')
    ).group_by(PriceHistory.product_id).subquery()

    # Query products with their latest price, filtered by price threshold
    deals = session.query(Product, PriceHistory).join(
        PriceHistory, Product.id == PriceHistory.product_id
    ).join(
        latest_price_subq,
        (PriceHistory.product_id == latest_price_subq.c.product_id) &
        (PriceHistory.recorded_at == latest_price_subq.c.max_recorded)
    ).filter(
        (PriceHistory.reduced_price_per_kg <= prefs.max_price_per_kg) |
        ((PriceHistory.reduced_price_per_kg == None) &
         (PriceHistory.original_price_per_kg <= prefs.max_price_per_kg))
    ).all()

    # Filter by brand and build result list
    filtered_deals = []

    for product, price in deals:
        # Apply brand filter
        if brands_filter:
            if product.brand and product.brand not in brands_filter:
                continue
        else:
            if not prefs.should_notify_for_brand(product.brand):
                continue

        ppkg = price.reduced_price_per_kg or price.original_price_per_kg
        filtered_deals.append((product, price, ppkg))

    # Sort by price per kg
    filtered_deals.sort(key=lambda x: x[2] if x[2] else 999)

    return filtered_deals[:limit]


def _has_data_for_price_range(max_price: float) -> bool:
    """
    Check if we have any product data in the DB for the given price range.
    This determines if we need to scrape - if data exists, no scrape needed.
    """
    session = get_session()
    try:
        from sqlalchemy import or_, and_
        # Check if any products exist at or below this price
        count = session.query(PriceHistory).filter(
            or_(
                PriceHistory.reduced_price_per_kg <= max_price,
                and_(
                    PriceHistory.reduced_price_per_kg == None,
                    PriceHistory.original_price_per_kg <= max_price
                )
            )
        ).limit(1).count()
        return count > 0
    finally:
        session.close()


def _get_data_freshness() -> Optional[datetime]:
    """Get the timestamp of the most recent price record."""
    session = get_session()
    try:
        from sqlalchemy import func
        result = session.query(func.max(PriceHistory.recorded_at)).scalar()
        return result
    finally:
        session.close()


def _format_freshness(last_updated: datetime) -> str:
    """Format a freshness indicator string."""
    if not last_updated:
        return "no data yet"

    delta = datetime.utcnow() - last_updated
    if delta < timedelta(minutes=1):
        return "just now"
    elif delta < timedelta(hours=1):
        minutes = int(delta.total_seconds() / 60)
        return f"{minutes} min ago"
    elif delta < timedelta(hours=24):
        hours = int(delta.total_seconds() / 3600)
        return f"{hours}h ago"
    else:
        days = delta.days
        return f"{days}d ago"


async def _send_deals_message(update: Update, deals: list[tuple], max_price: float, header: str = None) -> None:
    """
    Format and send deals to the user, only showing the cheapest variant per product.

    Args:
        update: Telegram update object
        deals: List of (product, price, price_per_kg) tuples
        max_price: User's max price threshold for display
        header: Optional custom header text
    """
    if not deals:
        await update.message.reply_text("No deals found matching your criteria.")
        return

    # Group all variants by match_key (for cross-site matching)
    from collections import defaultdict
    product_groups = defaultdict(list)
    for product, price, ppkg in deals:
        key = product.match_key
        product_groups[key].append((product, price, ppkg))

    # For each product, find the cheapest variant(s)
    cheapest_deals = []
    for key, variants in product_groups.items():
        # Find minimum price
        min_ppkg = min(ppkg for _, _, ppkg in variants)
        # Get cheapest variant (first one at min price)
        cheapest = next((p, pr, ppkg) for p, pr, ppkg in variants if ppkg == min_ppkg)

        # Get other sites (different site, best price per site)
        other_sites = []
        sites_seen = {cheapest[0].site}
        for p, pr, ppkg in sorted(variants, key=lambda x: x[2]):
            if p.site not in sites_seen:
                sites_seen.add(p.site)
                other_sites.append((p.site.capitalize(), ppkg, p.url))

        cheapest_deals.append((cheapest[0], cheapest[1], cheapest[2], other_sites))

    # Sort by price per kg
    cheapest_deals.sort(key=lambda x: x[2])

    for product, price, ppkg, other_sites in cheapest_deals:
        message = format_cheapest_variant_alert(product, price, max_price, other_sites)
        await update.message.reply_text(
            message,
            parse_mode="Markdown",
            disable_web_page_preview=False
        )


async def _background_scrape_and_notify(chat_id: str, prefs: UserPreferences, new_ceiling: float) -> None:
    """
    Run a background scrape for a higher price range and notify user of new deals.

    Args:
        chat_id: Telegram chat ID to notify
        prefs: User preferences
        new_ceiling: The new price ceiling to scrape up to
    """
    try:
        logger.info(f"Starting background scrape for chat_id={chat_id}, new_ceiling={new_ceiling}")

        scraper = ZooplusScraper()
        brands = prefs.get_brands_list()

        # Scrape with new price ceiling
        products = await scraper.scrape_brand_products(
            brands if brands else None,
            max_price_per_kg=new_ceiling
        )

        if not products:
            logger.info(f"Background scrape found no new products")
            return

        # Filter to only products within the new range
        session = get_session()
        try:
            new_deals_count = 0
            bot = Bot(token=settings.telegram_bot_token)

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

                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=msg,
                            parse_mode="Markdown",
                            disable_web_page_preview=False
                        )

                        # Record alert if product exists
                        if existing_product:
                            alert = AlertSent(
                                product_id=existing_product.id,
                                price_at_alert=scraped.current_price,
                                chat_id=chat_id
                            )
                            session.add(alert)
                            session.commit()
                    except Exception as e:
                        logger.error(f"Failed to send background alert: {e}")

            if new_deals_count > 5:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"🔔 Found {new_deals_count - 5} more deals!",
                    parse_mode="Markdown"
                )
            elif new_deals_count > 0:
                logger.info(f"Background scrape sent {new_deals_count} new deals to {chat_id}")
            else:
                logger.info(f"Background scrape found no new deals for {chat_id}")

        finally:
            session.close()

    except Exception as e:
        logger.error(f"Background scrape failed: {e}")


def format_alert_message(product: Product, price: PriceHistory, max_price_per_kg: float = None) -> str:
    """Format a sale alert message."""
    discount = price.discount_percent
    price_per_kg = price.reduced_price_per_kg or price.original_price_per_kg

    # Determine message type based on discount
    if discount > 0:
        msg = f"🔥 *{discount:.0f}% Sale!*\n\n"
    else:
        msg = f"💰 *Good Price!*\n\n"

    msg += f"*{product.brand or 'Unknown'}* - {product.name}\n"

    if product.size:
        msg += f"📦 {product.size}\n"

    # Price line
    msg += f"\n💵 *{price.current_price:.2f}€*"
    if price.original_price and discount > 0:
        msg += f" ~~{price.original_price:.2f}€~~"

    # Price per kg (highlighted if under max)
    if price_per_kg:
        if max_price_per_kg and price_per_kg <= max_price_per_kg:
            msg += f"\n📊 *{price_per_kg:.2f}€/kg* ✓"
        else:
            msg += f"\n📊 {price_per_kg:.2f}€/kg"

    # Site name for link (capitalize first letter)
    site_name = product.site.capitalize() if product.site else "Store"
    msg += f"\n\n🔗 [View on {site_name}]({product.url})"

    return msg


def format_cheapest_variant_alert(
    product: Product,
    price: PriceHistory,
    max_price_per_kg: float = None,
    other_sites: list[tuple] = None
) -> str:
    """
    Format an alert for the cheapest variant.

    Args:
        product: The cheapest variant product
        price: The price history for this variant
        max_price_per_kg: User's max price threshold for highlighting
        other_sites: List of (site_name, price_per_kg, url) for other sites with this product
    """
    discount = price.discount_percent
    price_per_kg = price.reduced_price_per_kg or price.original_price_per_kg

    # Header based on discount
    if discount > 0:
        msg = f"🔥 *{discount:.0f}% Sale!*\n\n"
    else:
        msg = f"💰 *Good Price!*\n\n"

    msg += f"*{product.brand or 'Unknown'}* - {product.name}\n"

    if product.size:
        msg += f"📦 {product.size}\n"

    # Price line
    msg += f"\n💵 *{price.current_price:.2f}€*"
    if price.original_price and discount > 0:
        msg += f" ~~{price.original_price:.2f}€~~"

    # Price per kg (highlighted if under max)
    if price_per_kg:
        if max_price_per_kg and price_per_kg <= max_price_per_kg:
            msg += f"\n📊 *{price_per_kg:.2f}€/kg* ✓"
        else:
            msg += f"\n📊 {price_per_kg:.2f}€/kg"

    # Show all sites where product is available (including main product)
    all_sites = [(product.site.capitalize(), price_per_kg, product.url)]
    if other_sites:
        all_sites.extend(other_sites)

    msg += "\n\nAvailable on:"
    # Sort: Zooplus first, then Bitiba
    site_order = {"Zooplus": 0, "Bitiba": 1, "Zoo24": 4}
    for site_name, ppkg, url in sorted(all_sites, key=lambda x: site_order.get(x[0], 99)):
        msg += f"\n  • [{site_name}]({url}) ({ppkg:.2f}€/kg)"

    return msg


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


async def _send_message_to_user(chat_id: str, message: str) -> bool:
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

            # Group all variants by match_key (for cross-site matching)
            from collections import defaultdict
            product_groups = defaultdict(list)
            for product, price, ppkg in user_products:
                # Use match_key for cross-site product matching
                key = product.match_key
                product_groups[key].append((product, price, ppkg))

            # For each product, find the cheapest variant and alert only for that
            for base_id, variants in product_groups.items():
                # Find minimum price
                min_ppkg = min(ppkg for _, _, ppkg in variants)
                # Get cheapest variant
                cheapest_product, cheapest_price, _ = next(
                    (p, pr, ppkg) for p, pr, ppkg in variants if ppkg == min_ppkg
                )

                # Get other sites (different site, best price per site)
                other_sites = []
                sites_seen = {cheapest_product.site}
                for p, pr, ppkg in sorted(variants, key=lambda x: x[2]):
                    if p.site not in sites_seen:
                        sites_seen.add(p.site)
                        other_sites.append((p.site.capitalize(), ppkg, p.url))

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
                success = await _send_message_to_user(prefs.chat_id, message)

                if success:
                    alert = AlertSent(
                        product_id=cheapest_product.id,
                        price_at_alert=cheapest_price.current_price,
                        chat_id=prefs.chat_id
                    )
                    session.add(alert)
                    session.commit()
                    alerts_sent += 1
                    logger.info(f"Alert sent to {prefs.chat_id} for {base_id}")

    finally:
        session.close()

    return alerts_sent


# Bot command handlers
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    chat_id = str(update.effective_chat.id)
    prefs = get_or_create_preferences(chat_id)

    # Check if already configured
    is_configured = prefs.max_price_per_kg is not None

    if is_configured:
        brands = prefs.get_brands_list()
        brands_text = ', '.join(brands[:3]) + ('...' if len(brands) > 3 else '') if brands else "None (add with /addbrand)"
        await update.message.reply_text(
            "🐱 *Cat Food Price Alert Bot*\n\n"
            f"✅ You're set up!\n"
            f"Max price: *{prefs.max_price_per_kg:.2f}€/kg*\n"
            f"Brands: {brands_text}\n\n"
            "*Commands:*\n"
            "/setmaxprice - Change max price\n"
            "/addbrand - Add brands to watch\n"
            "/removebrand - Remove brands\n"
            "/brands - Show watched brands\n"
            "/status - Check settings",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            "🐱 *Cat Food Price Alert Bot*\n\n"
            "I'll notify you when wet cat food is under your price!\n\n"
            "*Get started:*\n"
            "1️⃣ Set your max price: /setmaxprice 7\n"
            "2️⃣ Choose brands: /addbrand MAC's, Wild Freedom\n\n"
            "You'll automatically receive alerts once configured!\n\n"
            "*All commands:*\n"
            "/setmaxprice <price> - Set max €/kg\n"
            "/addbrand <name> - Add brand(s)\n"
            "/removebrand <name> - Remove brand\n"
            "/brands - Show watched brands\n"
            "/listbrands - All available brands\n"
            "/status - Check settings",
            parse_mode="Markdown"
        )


async def brands_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /brands command - show watched brands."""
    chat_id = str(update.effective_chat.id)
    prefs = get_or_create_preferences(chat_id)

    brands = prefs.get_brands_list()
    if brands:
        brand_list = "\n".join(f"  • {b}" for b in brands)
        await update.message.reply_text(
            f"📋 *Your Watched Brands*\n\n{brand_list}\n\n"
            f"Use /addbrand or /removebrand to modify.",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            "📋 No brands configured yet.\n\n"
            "Use /addbrand <name> to add brands to watch.\n"
            "Example: /addbrand MAC's, Wild Freedom",
            parse_mode="Markdown"
        )


async def addbrand_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /addbrand command - supports multiple brands separated by commas."""
    chat_id = str(update.effective_chat.id)

    if not context.args:
        await update.message.reply_text(
            "Usage: /addbrand <brand name(s)>\n\n"
            "Examples:\n"
            "  /addbrand Animonda\n"
            "  /addbrand MAC's, Wild Freedom, Leonardo\n\n"
            "Use /listbrands to see available brands."
        )
        return

    # Join all args and split by comma for multiple brands
    input_text = " ".join(context.args)
    brand_inputs = [b.strip() for b in input_text.split(",") if b.strip()]

    added_brands = []
    already_added = []
    unknown_brands = []
    ambiguous = []

    session = get_session()
    try:
        prefs = session.query(UserPreferences).filter(
            UserPreferences.chat_id == chat_id
        ).first()

        if not prefs:
            prefs = UserPreferences(chat_id=chat_id)
            session.add(prefs)

        for brand in brand_inputs:
            # Check if it's a known brand (exact match, case-insensitive)
            matched_brand = None
            for known in AVAILABLE_BRANDS:
                if known.lower() == brand.lower():
                    matched_brand = known
                    break

            # Try partial match if no exact match
            if not matched_brand:
                partial_matches = []
                for known in AVAILABLE_BRANDS:
                    if brand.lower() in known.lower() or known.lower() in brand.lower():
                        partial_matches.append(known)

                if len(partial_matches) == 1:
                    matched_brand = partial_matches[0]
                elif len(partial_matches) > 1:
                    ambiguous.append(f"{brand} ({', '.join(partial_matches)})")
                    continue

            if not matched_brand:
                unknown_brands.append(brand)
                continue

            if prefs.add_brand(matched_brand):
                added_brands.append(matched_brand)
            else:
                already_added.append(matched_brand)

        session.commit()

        # Build response message
        msg_parts = []
        if added_brands:
            msg_parts.append(f"✅ Added: {', '.join(added_brands)}")
        if already_added:
            msg_parts.append(f"ℹ️ Already watching: {', '.join(already_added)}")
        if unknown_brands:
            msg_parts.append(f"❌ Unknown: {', '.join(unknown_brands)}")
        if ambiguous:
            msg_parts.append(f"❓ Ambiguous: {'; '.join(ambiguous)}")

        if added_brands:
            msg_parts.append(f"\nCurrent brands: {', '.join(prefs.get_brands_list())}")

        await update.message.reply_text("\n".join(msg_parts), parse_mode="Markdown")

        # Immediately show deals from DB for newly added brands (instant response!)
        if added_brands and prefs.max_price_per_kg is not None:
            deals = _get_deals_from_db(prefs, session, brands_filter=added_brands)

            if deals:
                freshness = _format_freshness(_get_data_freshness())
                await update.message.reply_text(
                    f"📦 Found {len(deals)} deal(s) from recent data (updated {freshness}):",
                    parse_mode="Markdown"
                )
                await _send_deals_message(update, deals, prefs.max_price_per_kg)
            else:
                # No deals in DB for these brands - check if brand has any products at all
                brand_products = session.query(Product).filter(
                    Product.brand.in_(added_brands)
                ).first()

                if not brand_products:
                    # Brand has no data in DB - trigger background scrape
                    await update.message.reply_text(
                        "🔄 No data for this brand yet. Checking for deals...",
                        parse_mode="Markdown"
                    )
                    asyncio.create_task(_background_scrape_and_notify(
                        chat_id, prefs, prefs.max_price_per_kg
                    ))
                else:
                    await update.message.reply_text(
                        "No deals found under your max price for these brand(s).",
                        parse_mode="Markdown"
                    )
        elif added_brands:
            await update.message.reply_text(
                "Set /setmaxprice first to see deals!",
                parse_mode="Markdown"
            )

    finally:
        session.close()


async def removebrand_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /removebrand command."""
    chat_id = str(update.effective_chat.id)

    if not context.args:
        await update.message.reply_text("Usage: /removebrand <brand name>")
        return

    brand = " ".join(context.args)

    session = get_session()
    try:
        prefs = session.query(UserPreferences).filter(
            UserPreferences.chat_id == chat_id
        ).first()

        if not prefs:
            await update.message.reply_text("No brands configured yet.")
            return

        if prefs.remove_brand(brand):
            session.commit()
            remaining = prefs.get_brands_list()
            if remaining:
                await update.message.reply_text(
                    f"✅ Removed *{brand}*\n\nRemaining: {', '.join(remaining)}",
                    parse_mode="Markdown"
                )
            else:
                await update.message.reply_text(
                    f"✅ Removed *{brand}*\n\nNo brands left. Use /addbrand to add more.",
                    parse_mode="Markdown"
                )
        else:
            await update.message.reply_text(f"❌ *{brand}* not found in your watch list.", parse_mode="Markdown")
    finally:
        session.close()


async def listbrands_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /listbrands command - show available brands."""
    brands_list = "\n".join(f"  • {b}" for b in sorted(AVAILABLE_BRANDS))
    await update.message.reply_text(
        f"🏷️ *Available Brands*\n\n{brands_list}\n\n"
        f"Use /addbrand <name> to watch a brand.",
        parse_mode="Markdown"
    )


async def setmaxprice_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setmaxprice command - set max price per kg threshold."""
    chat_id = str(update.effective_chat.id)

    if not context.args:
        prefs = get_or_create_preferences(chat_id)
        current = prefs.max_price_per_kg
        if current is not None:
            await update.message.reply_text(
                f"💰 *Max Price per KG*\n\n"
                f"Current threshold: *{current:.2f}€/kg*\n\n"
                f"Usage: /setmaxprice <price>\n"
                f"Example: /setmaxprice 15.00\n\n"
                f"Use /setmaxprice off to disable.",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                f"💰 *Max Price per KG*\n\n"
                f"No threshold set (all prices allowed)\n\n"
                f"Usage: /setmaxprice <price>\n"
                f"Example: /setmaxprice 15.00\n\n"
                f"Only get alerts for deals below this price per kg.",
                parse_mode="Markdown"
            )
        return

    value = context.args[0].lower()

    # Handle "off" to disable
    if value in ("off", "none", "disable", "clear"):
        session = get_session()
        try:
            prefs = session.query(UserPreferences).filter(
                UserPreferences.chat_id == chat_id
            ).first()
            if not prefs:
                prefs = UserPreferences(chat_id=chat_id)
                session.add(prefs)
            prefs.max_price_per_kg = None
            # Reset alerts for this user
            session.query(AlertSent).filter(AlertSent.chat_id == chat_id).delete()
            session.commit()
            await update.message.reply_text(
                "✅ Max price per kg threshold *disabled*.\n"
                "You'll receive alerts regardless of price per kg.",
                parse_mode="Markdown"
            )
        finally:
            session.close()
        return

    # Parse numeric value
    try:
        max_price = float(value.replace(",", "."))
        if max_price <= 0:
            raise ValueError("Price must be positive")
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid price. Please enter a number.\n"
            "Example: /setmaxprice 15.00"
        )
        return

    session = get_session()
    try:
        prefs = session.query(UserPreferences).filter(
            UserPreferences.chat_id == chat_id
        ).first()
        if not prefs:
            prefs = UserPreferences(chat_id=chat_id)
            session.add(prefs)

        old_max = prefs.max_price_per_kg
        prefs.max_price_per_kg = max_price

        # Reset alerts for this user if max price changed
        if old_max != max_price:
            session.query(AlertSent).filter(AlertSent.chat_id == chat_id).delete()

        session.commit()

        # Immediately show deals from database (instant response!)
        deals = _get_deals_from_db(prefs, session)

        if deals:
            freshness = _format_freshness(_get_data_freshness())
            await update.message.reply_text(
                f"✅ Max price set to *{max_price:.2f}€/kg*\n\n"
                f"📦 Found {len(deals)} deal(s) from recent data (updated {freshness}):",
                parse_mode="Markdown"
            )
            await _send_deals_message(update, deals, max_price)
        else:
            await update.message.reply_text(
                f"✅ Max price set to *{max_price:.2f}€/kg*\n\n"
                f"No deals found in database yet.",
                parse_mode="Markdown"
            )

        # Only scrape if we have NO data at all for this price range
        # (data persists in DB regardless of user preference changes)
        needs_background_scrape = not _has_data_for_price_range(max_price)

        if needs_background_scrape:
            await update.message.reply_text(
                "🔄 No cached data yet. Checking for deals...",
                parse_mode="Markdown"
            )
            # Run scrape in background so user isn't blocked
            asyncio.create_task(_background_scrape_and_notify(chat_id, prefs, max_price))

    finally:
        session.close()


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command."""
    chat_id = str(update.effective_chat.id)
    prefs = get_or_create_preferences(chat_id)

    session = get_session()
    try:
        user_alert_count = session.query(AlertSent).filter(AlertSent.chat_id == chat_id).count()

        brands = prefs.get_brands_list()
        brands_info = ", ".join(brands) if brands else "None"
        max_price_info = f"{prefs.max_price_per_kg:.2f}€/kg" if prefs.max_price_per_kg else "⚠️ Not set"

        is_active = prefs.max_price_per_kg is not None
        freshness = _format_freshness(_get_data_freshness())

        msg = f"📊 *Your Settings*\n\n"
        msg += f"Status: {'✅ Active' if is_active else '⚠️ Set maxprice to activate'}\n"
        msg += f"Max price: {max_price_info}\n"
        msg += f"Brands: {brands_info}\n"
        msg += f"Alerts received: {user_alert_count}\n"
        msg += f"Data updated: {freshness}\n\n"
        msg += "Use /reset to get all current deals again."

        await update.message.reply_text(msg, parse_mode="Markdown")

    finally:
        session.close()


async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reset command - clear alert history and resend all matching deals."""
    chat_id = str(update.effective_chat.id)
    prefs = get_or_create_preferences(chat_id)

    if prefs.max_price_per_kg is None:
        await update.message.reply_text(
            "⚠️ Set your max price first!\n"
            "Use /setmaxprice <price>",
            parse_mode="Markdown"
        )
        return

    session = get_session()
    try:
        # Clear this user's alert history
        deleted = session.query(AlertSent).filter(AlertSent.chat_id == chat_id).delete()
        session.commit()

        await update.message.reply_text(
            f"🔄 Reset complete! Cleared {deleted} previous alerts.\n"
            "Checking for current deals...",
            parse_mode="Markdown"
        )

        # Fetch and send all current matching deals
        # Improved logic: Get the latest price record for each product that is under threshold
        from sqlalchemy import func
        
        # Subquery to get the latest recorded_at for each product
        latest_price_sub = session.query(
            PriceHistory.product_id,
            func.max(PriceHistory.recorded_at).label('max_recorded')
        ).group_by(PriceHistory.product_id).subquery()

        # Join with subquery to get full PriceHistory records
        deals = session.query(Product, PriceHistory).join(
            PriceHistory, Product.id == PriceHistory.product_id
        ).join(
            latest_price_sub,
            (PriceHistory.product_id == latest_price_sub.c.product_id) & 
            (PriceHistory.recorded_at == latest_price_sub.c.max_recorded)
        ).all()

        # Deduplicate and filter
        unique_deals = []
        for product, price in deals:
            # Calculate price per kg
            ppkg = price.reduced_price_per_kg or price.original_price_per_kg
            
            # Use fallback calculation if ppkg is missing but we have current price
            if ppkg is None:
                from scraper import ZooplusScraper
                scraper = ZooplusScraper()
                grams = scraper._parse_weight_grams(product.name)
                ppkg = scraper._calculate_price_per_kg(price.current_price, grams)

            if ppkg and ppkg <= prefs.max_price_per_kg and prefs.should_notify_for_brand(product.brand):
                unique_deals.append((product, price, ppkg))

        # Group all variants by match_key (for cross-site matching)
        from collections import defaultdict
        product_groups = defaultdict(list)
        for product, price, ppkg in unique_deals:
            key = product.match_key
            product_groups[key].append((product, price, ppkg))

        # For each product, find the cheapest variant
        cheapest_deals = []
        for key, variants in product_groups.items():
            min_ppkg = min(ppkg for _, _, ppkg in variants)
            cheapest = next((p, pr, ppkg) for p, pr, ppkg in variants if ppkg == min_ppkg)

            # Get other sites (different site, best price per site)
            other_sites = []
            sites_seen = {cheapest[0].site}
            for p, pr, ppkg in sorted(variants, key=lambda x: x[2]):
                if p.site not in sites_seen:
                    sites_seen.add(p.site)
                    other_sites.append((p.site.capitalize(), ppkg, p.url))

            cheapest_deals.append((cheapest[0], cheapest[1], cheapest[2], other_sites))

        # Sort by price per kg
        cheapest_deals.sort(key=lambda x: x[2])

        sent_count = 0
        for product, price, ppkg, other_sites in cheapest_deals:
            try:
                message = format_cheapest_variant_alert(product, price, prefs.max_price_per_kg, other_sites)

                await update.message.reply_text(
                    message,
                    parse_mode="Markdown",
                    disable_web_page_preview=False
                )

                # Record alert for the cheapest variant only
                alert = AlertSent(
                    product_id=product.id,
                    price_at_alert=price.current_price,
                    chat_id=chat_id
                )
                session.add(alert)
                session.commit()
                sent_count += 1

                if sent_count >= 30:  # Higher limit for reset
                    break
            except Exception as e:
                logger.error(f"Failed to send reset alert: {e}")

        if sent_count == 0:
            await update.message.reply_text("No deals found matching your settings.")
        else:
            remaining = len(cheapest_deals) - sent_count
            if remaining > 0:
                await update.message.reply_text(f"Sent {sent_count} alerts. {remaining} more available.")
            else:
                await update.message.reply_text(f"Sent {sent_count} alert(s)!")

    finally:
        session.close()


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    await start_command(update, context)


async def scrape_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /scrape command - manually trigger a full scrape of all sites."""
    from tracker import run_check

    await update.message.reply_text(
        "🔄 Starting full scrape of all sites (Zooplus, Bitiba, Zooroyal, Fressnapf, Zoo24)...\n"
        "This may take a few minutes."
    )

    try:
        stats = await run_check()
        if stats:
            await update.message.reply_text(
                f"✅ Scrape complete!\n\n"
                f"📊 **Results:**\n"
                f"• Total products: {stats['total']}\n"
                f"• New products: {stats['new_products']}\n"
                f"• On sale: {stats['on_sale']}\n"
                f"• Alerts sent: {stats['alerts_sent']}",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text("⚠️ No products found during scrape.")
    except Exception as e:
        logger.error(f"Scrape command failed: {e}")
        await update.message.reply_text(f"❌ Scrape failed: {e}")


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


if __name__ == "__main__":
    import asyncio
    asyncio.run(send_test_message())
