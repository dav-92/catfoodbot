import logging
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

from sqlalchemy import func, or_, and_

from database import get_session, Product, PriceHistory, UserPreferences
from scraper import ZooplusScraper

logger = logging.getLogger(__name__)

def get_deals_from_db(prefs: UserPreferences, session, brands_filter: list[str] = None, limit: int = 1000) -> list[tuple]:
    """
    Query existing deals from database matching user preferences.
    Returns list of (product, price, price_per_kg) tuples sorted by price_per_kg.
    """
    if prefs.max_price_per_kg is None:
        return []

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
        (PriceHistory.recorded_at >= datetime.utcnow() - timedelta(hours=48)) &
        ((PriceHistory.reduced_price_per_kg <= prefs.max_price_per_kg) |
         ((PriceHistory.reduced_price_per_kg == None) &
          (PriceHistory.original_price_per_kg <= prefs.max_price_per_kg)))
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
    
    logger.info(f"get_deals_from_db: Found {len(filtered_deals)} matching deals (limit: {limit})")
    
    return filtered_deals[:limit]

def has_data_for_price_range(max_price: float) -> bool:
    """
    Check if we have any product data in the DB for the given price range.
    """
    session = get_session()
    try:
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

def get_data_freshness() -> Optional[datetime]:
    """Get the timestamp of the most recent price record."""
    session = get_session()
    try:
        result = session.query(func.max(PriceHistory.recorded_at)).scalar()
        return result
    finally:
        session.close()

def find_cheapest_variants(deals: List[Tuple[Product, PriceHistory, float]]) -> List[Tuple[Product, PriceHistory, float, List[Tuple[str, float, str]]]]:
    """
    Group deals by match_key and return the cheapest variant per group.
    Returns: List of (product, price, price_per_kg, other_sites_info)
    """
    from collections import defaultdict
    product_groups = defaultdict(list)
    
    for product, price, ppkg in deals:
        key = product.match_key
        product_groups[key].append((product, price, ppkg))

    cheapest_deals = []
    
    # Simple deduplication: prefer cheapest per (brand, size, site) tuple
    # Then group by (brand, size) to show "other sites"
    
    # 1. Deduplicate by (brand, size, site) -> keep cheapest
    unique_deals = {}
    for product, price, ppkg in deals:
        # Key: brand | size | site
        # Use match_key which is brand|size, then add site
        unique_key = f"{product.match_key}|{product.site}"
        
        if unique_key not in unique_deals:
            unique_deals[unique_key] = (product, price, ppkg)
        else:
            # Keep cheaper one
            if ppkg < unique_deals[unique_key][2]:
                unique_deals[unique_key] = (product, price, ppkg)
    
    logger.info(f"find_cheapest_variants: Reduced {len(deals)} deals to {len(unique_deals)} unique offerings")
    
    # 2. Group by (brand, size) to link same products from other sites
    grouped_variants = defaultdict(list)
    for product, price, ppkg in unique_deals.values():
        grouped_variants[product.match_key].append((product, price, ppkg))
        
    # 3. For each group, pick the absolute cheapest as main, others as "other sites"
    for key, variants in grouped_variants.items():
        # Sort variants by price ascending
        variants.sort(key=lambda x: x[2])
        
        cheapest = variants[0]
        other_sites = []
        
        # Add other sites info
        for p, pr, ppkg in variants[1:]:
            other_sites.append((p.site.capitalize(), ppkg, p.url))
            
        cheapest_deals.append((cheapest[0], cheapest[1], cheapest[2], other_sites))

    # Sort final list by price per kg
    cheapest_deals.sort(key=lambda x: x[2])
    return cheapest_deals

def format_freshness_string(last_updated: datetime) -> str:
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
