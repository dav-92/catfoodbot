import asyncio
import logging
from telegram import Update
from telegram.ext import ContextTypes

from database import get_session, get_or_create_preferences, UserPreferences, Product, AlertSent
from services.deal_service import (
    get_deals_from_db, 
    get_data_freshness, 
    has_data_for_price_range, 
    format_freshness_string,
    find_cheapest_variants
)
from services.alert_service import background_scrape_and_notify
from bot.formatter import format_cheapest_variant_alert
from scraper import ZooplusScraper
from tracker import run_check

logger = logging.getLogger(__name__)
AVAILABLE_BRANDS = ZooplusScraper.BRANDS

async def send_deals_response(update: Update, deals: list, max_price: float):
    """Helper to format and send deals to values."""
    if not deals:
        await update.message.reply_text("No deals found matching your criteria.")
        return

    cheapest_deals = find_cheapest_variants(deals)

    for product, price, ppkg, other_sites in cheapest_deals:
        message = format_cheapest_variant_alert(product, price, max_price, other_sites)
        await update.message.reply_text(
            message,
            parse_mode="Markdown",
            disable_web_page_preview=False
        )

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    chat_id = str(update.effective_chat.id)
    prefs = get_or_create_preferences(chat_id)
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
    """Handle /brands command."""
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
    """Handle /addbrand command."""
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
            matched_brand = None
            for known in AVAILABLE_BRANDS:
                if known.lower() == brand.lower():
                    matched_brand = known
                    break

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

        if added_brands and prefs.max_price_per_kg is not None:
            deals = get_deals_from_db(prefs, session, brands_filter=added_brands)
            if deals:
                freshness = format_freshness_string(get_data_freshness())
                await update.message.reply_text(
                    f"📦 Found {len(deals)} deal(s) from recent data (updated {freshness}):",
                    parse_mode="Markdown"
                )
                await send_deals_response(update, deals, prefs.max_price_per_kg)
            else:
                brand_products = session.query(Product).filter(
                    Product.brand.in_(added_brands)
                ).first()

                if not brand_products:
                    await update.message.reply_text(
                        "🔄 No data for this brand yet. Checking for deals...",
                        parse_mode="Markdown"
                    )
                    asyncio.create_task(background_scrape_and_notify(
                        chat_id, prefs, prefs.max_price_per_kg, specific_brands=added_brands
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
    """Handle /listbrands command."""
    brands_list = "\n".join(f"  • {b}" for b in sorted(AVAILABLE_BRANDS))
    await update.message.reply_text(
        f"🏷️ *Available Brands*\n\n{brands_list}\n\n"
        f"Use /addbrand <name> to watch a brand.",
        parse_mode="Markdown"
    )

async def setmaxprice_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /setmaxprice command."""
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

        if old_max != max_price:
            session.query(AlertSent).filter(AlertSent.chat_id == chat_id).delete()

        session.commit()

        deals = get_deals_from_db(prefs, session)

        if deals:
            freshness = format_freshness_string(get_data_freshness())
            await update.message.reply_text(
                f"✅ Max price set to *{max_price:.2f}€/kg*\n\n"
                f"📦 Found {len(deals)} deal(s) from recent data (updated {freshness}):",
                parse_mode="Markdown"
            )
            await send_deals_response(update, deals, max_price)
        else:
            await update.message.reply_text(
                f"✅ Max price set to *{max_price:.2f}€/kg*\n\n"
                f"No deals found in database yet.",
                parse_mode="Markdown"
            )

        if not has_data_for_price_range(max_price):
            await update.message.reply_text(
                "🔄 No cached data yet. Checking for deals...",
                parse_mode="Markdown"
            )
            asyncio.create_task(background_scrape_and_notify(chat_id, prefs, max_price))

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
        from tracker import is_check_running
        
        is_active = prefs.max_price_per_kg is not None
        status_text = "✅ Active" if is_active else "⚠️ Set maxprice to activate"
        
        # Override status if currently scraping
        if is_check_running():
            status_text = "🔄 *Checking for deals now...*"

        freshness = format_freshness_string(get_data_freshness())
 
        msg = f"📊 *Your Settings*\n\n"
        msg += f"Status: {status_text}\n"
        msg += f"Max price: {max_price_info}\n"
        msg += f"Brands: {brands_info}\n"
        msg += f"Alerts received: {user_alert_count}\n"
        msg += f"Data updated: {freshness}\n\n"
        msg += "Use /reset to get all current deals again."
        await update.message.reply_text(msg, parse_mode="Markdown")
    finally:
        session.close()

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /reset command."""
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
        deleted = session.query(AlertSent).filter(AlertSent.chat_id == chat_id).delete()
        session.commit()

        await update.message.reply_text(
            f"🔄 Reset complete! Cleared {deleted} previous alerts.\n"
            "Checking for current deals...",
            parse_mode="Markdown"
        )

        deals = get_deals_from_db(prefs, session)
        if not deals:
            await update.message.reply_text("No deals found matching your settings.")
            return

        cheapest_deals = find_cheapest_variants(deals)
        sent_count = 0
        
        for product, price, ppkg, other_sites in cheapest_deals:
            try:
                message = format_cheapest_variant_alert(product, price, prefs.max_price_per_kg, other_sites)
                await update.message.reply_text(
                    message,
                    parse_mode="Markdown",
                    disable_web_page_preview=False
                )

                alert = AlertSent(
                    product_id=product.id,
                    price_at_alert=price.current_price,
                    chat_id=chat_id
                )
                session.add(alert)
                session.commit()
                sent_count += 1

                if sent_count >= 30:
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

async def scrape_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /scrape command."""
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

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    await start_command(update, context)
