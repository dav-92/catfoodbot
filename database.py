import re
from datetime import datetime
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, joinedload
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, ForeignKey, func

from config import settings


def generate_match_key(brand: str, size: str, name: str = None) -> str:
    """
    Generate a match key for cross-site product matching.

    The key is based on brand + normalized size, which should be consistent
    across different sites for the same product.

    Examples:
        MAC's + "24 x 800 g" -> "macs|24x800g"
        Leonardo + "6 x 400 g" -> "leonardo|6x400g"
    """
    if not brand:
        brand = "unknown"

    # Normalize brand: lowercase, remove apostrophes and special chars
    brand_norm = brand.lower()
    brand_norm = re.sub(r"[''`´]", "", brand_norm)  # Remove apostrophes
    brand_norm = re.sub(r"[^a-z0-9]", "", brand_norm)  # Keep only alphanumeric

    # Normalize size: remove spaces, lowercase
    size_norm = ""
    if size:
        size_norm = size.lower()
        size_norm = re.sub(r"\s+", "", size_norm)  # Remove spaces
        size_norm = re.sub(r"[^a-z0-9x]", "", size_norm)  # Keep alphanumeric and 'x'

    # If no size, try to extract from name
    if not size_norm and name:
        # Try to extract size pattern from name (e.g., "24 x 800 g", "6x400g")
        size_match = re.search(r'(\d+)\s*x\s*(\d+)\s*g', name, re.IGNORECASE)
        if size_match:
            size_norm = f"{size_match.group(1)}x{size_match.group(2)}g"
        else:
            # Try single size pattern (e.g., "800g")
            single_match = re.search(r'(\d+)\s*g\b', name, re.IGNORECASE)
            if single_match:
                size_norm = f"{single_match.group(1)}g"

    if not size_norm:
        size_norm = "nosize"

    return f"{brand_norm}|{size_norm}"

engine = create_engine(settings.database_url, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    external_id = Column(String, unique=True, index=True)  # ID from the website (e.g., "564091.13")
    base_product_id = Column(String, index=True)  # Base product ID for variant grouping (e.g., "564091")
    variant_name = Column(String)  # Variant descriptor (e.g., "Chicken", "Beef")
    name = Column(String, nullable=False)
    brand = Column(String, index=True)
    size = Column(String)  # e.g., "85g", "400g"
    url = Column(String, nullable=False)
    site = Column(String, default="zooplus")  # For future multi-site support
    is_wet_food = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    prices = relationship("PriceHistory", back_populates="product")

    @property
    def match_key(self) -> str:
        """Get the cross-site match key for this product."""
        return generate_match_key(self.brand, self.size, self.name)

    def __repr__(self):
        return f"<Product {self.brand} {self.name} ({self.size})>"


class PriceHistory(Base):
    __tablename__ = "price_history"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    current_price = Column(Float, nullable=False)
    original_price = Column(Float)  # If on sale, this is the crossed-out price
    is_on_sale = Column(Boolean, default=False)
    sale_tag = Column(String)  # e.g., "Angebot", "-20%"
    original_price_per_kg = Column(Float)  # Original price per kg (before discount)
    reduced_price_per_kg = Column(Float)  # Reduced price per kg (after discount)
    recorded_at = Column(DateTime, default=datetime.utcnow)

    product = relationship("Product", back_populates="prices")

    @property
    def discount_percent(self) -> float:
        if self.original_price and self.original_price > self.current_price:
            return ((self.original_price - self.current_price) / self.original_price) * 100
        return 0.0

    def __repr__(self):
        return f"<PriceHistory {self.current_price}€ (was {self.original_price}€)>"


class AlertSent(Base):
    """Track which alerts we've already sent to avoid duplicates."""
    __tablename__ = "alerts_sent"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    price_at_alert = Column(Float, nullable=False)
    chat_id = Column(String, default="")  # Which user received this alert
    sent_at = Column(DateTime, default=datetime.utcnow)


class WatchedBrand(Base):
    """Normalize watched brands into a separate table."""
    __tablename__ = "watched_brands"

    id = Column(Integer, primary_key=True)
    user_pref_id = Column(Integer, ForeignKey("user_preferences.id"), nullable=False)
    brand_name = Column(String, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    user_prefs = relationship("UserPreferences", back_populates="brands")



class UserPreferences(Base):
    """Store user preferences for brand filtering."""
    __tablename__ = "user_preferences"

    id = Column(Integer, primary_key=True)
    chat_id = Column(String, unique=True, nullable=False, index=True)
    watched_brands = Column(String, default="")  # Comma-separated list of brands
    min_discount = Column(Integer, default=0)  # Minimum discount percentage to notify (0 = any discount)
    max_price_per_kg = Column(Float, default=None)  # Max price per kg threshold (e.g., 4.0 = 4€/kg)
    alerts_enabled = Column(Boolean, default=False)  # User must explicitly enable alerts
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # One-to-many relationship
    brands = relationship("WatchedBrand", back_populates="user_prefs", cascade="all, delete-orphan", lazy="joined")

    def get_brands_list(self) -> list[str]:
        """Get watched brands as a list."""
        # Prefer new relational data
        if self.brands:
            return [b.brand_name for b in self.brands]
            
        # Fallback to legacy field until migrated
        if not self.watched_brands:
            return []
        return [b.strip() for b in self.watched_brands.split(",") if b.strip()]

    def set_brands_list(self, brands: list[str]):
        """Legacy setter - do not use for new code. Use add_brand/remove_brand."""
        # This keeps the legacy field in sync for now, but really we should use the relationship
        self.watched_brands = ",".join(brands)

    def add_brand(self, brand: str) -> bool:
        """Add a brand to watch list. Returns True if added, False if already exists."""
        current_brands = self.get_brands_list()
        brand_lower = brand.lower()
        if any(b.lower() == brand_lower for b in current_brands):
            return False
            
        # Add to relationship
        new_brand = WatchedBrand(brand_name=brand)
        self.brands.append(new_brand)
        
        # Keep legacy field in sync for now
        current_brands.append(brand)
        self.set_brands_list(current_brands)
        return True

    def remove_brand(self, brand: str) -> bool:
        """Remove a brand from watch list. Returns True if removed."""
        # Remove from relationship
        found = False
        if self.brands:
            for b in list(self.brands):
                if b.brand_name.lower() == brand.lower():
                    self.brands.remove(b)
                    found = True
        
        # Update legacy field logic
        current_brands = self.get_brands_list() # This might use legacy or new depending on data
        brand_lower = brand.lower()
        new_brands = [b for b in current_brands if b.lower() != brand_lower]
        
        if len(new_brands) == len(current_brands) and not found:
            return False
            
        self.set_brands_list(new_brands)
        return True

    @staticmethod
    def normalize_brand(brand: str) -> str:
        """Normalize brand name for matching (handles apostrophe variants, case)."""
        if not brand:
            return ""
        normalized = brand.lower()
        # Replace various apostrophe characters with standard one
        normalized = normalized.replace("\xb4", "'")  # acute accent ´
        normalized = normalized.replace("\u2019", "'")  # right single quote '
        normalized = normalized.replace("`", "'")  # backtick
        return normalized

    def should_notify_for_brand(self, brand: str) -> bool:
        """Check if we should notify for this brand."""
        if not brand:
            return False
        brands = self.get_brands_list()
        brand_normalized = self.normalize_brand(brand)
        for b in brands:
            b_normalized = self.normalize_brand(b)
            # Exact match or partial match (e.g., "mac's" matches "MAC's Cat")
            if b_normalized == brand_normalized or b_normalized in brand_normalized or brand_normalized in b_normalized:
                return True
        return False


def init_db():
    """Create all tables."""
    Base.metadata.create_all(engine)


def get_session():
    """Get a database session."""
    return SessionLocal()




def get_or_create_preferences(chat_id: str) -> UserPreferences:
    """Get or create user preferences for a chat ID."""
    session = get_session()
    try:
        prefs = session.query(UserPreferences).options(joinedload(UserPreferences.brands)).filter(
            UserPreferences.chat_id == chat_id
        ).first()

        if not prefs:
            prefs = UserPreferences(chat_id=chat_id)
            session.add(prefs)
            session.commit()
            # Re-query with eager load to ensure brands are loaded
            prefs = session.query(UserPreferences).options(joinedload(UserPreferences.brands)).filter(
                UserPreferences.chat_id == chat_id
            ).first()

        return prefs
    finally:
        session.close()


def update_preferences(chat_id: str, **kwargs) -> UserPreferences:
    """Update user preferences."""
    session = get_session()
    try:
        prefs = session.query(UserPreferences).filter(
            UserPreferences.chat_id == chat_id
        ).first()

        if not prefs:
            prefs = UserPreferences(chat_id=chat_id, **kwargs)
            session.add(prefs)
        else:
            for key, value in kwargs.items():
                if hasattr(prefs, key):
                    setattr(prefs, key, value)

        session.commit()
        session.refresh(prefs)
        return prefs
    finally:
        session.close()


if __name__ == "__main__":
    init_db()
    print("Database initialized!")

def get_product_statistics() -> list[dict]:
    """Get product counts grouped by site and brand."""
    session = get_session()
    try:
        stats = session.query(
            Product.site, 
            Product.brand, 
            func.count(Product.id)
        ).group_by(Product.site, Product.brand).order_by(Product.site, Product.brand).all()
        
        return [
            {"site": site, "brand": brand, "count": count}
            for site, brand, count in stats
        ]
    finally:
        session.close()
