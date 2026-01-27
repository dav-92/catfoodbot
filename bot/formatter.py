from database import Product, PriceHistory

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
    site_name = product.site.capitalize() if product.site else "Store"
    all_sites = [(site_name, price_per_kg, product.url)]
    if other_sites:
        all_sites.extend(other_sites)

    msg += "\n\nAvailable on:"
    # Sort: Zooplus first, then Bitiba
    site_order = {"Zooplus": 0, "Bitiba": 1, "Zoo24": 4}
    for site_name, ppkg, url in sorted(all_sites, key=lambda x: site_order.get(x[0], 99)):
        if ppkg:
            msg += f"\n  • [{site_name}]({url}) ({ppkg:.2f}€/kg)"
        else:
            msg += f"\n  • [{site_name}]({url})"

    return msg
