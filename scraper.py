import re
import time
import random
import logging
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin, urlparse, parse_qs, quote

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from typing import AsyncGenerator, Callable


from config import settings

logger = logging.getLogger(__name__)


@dataclass
class ScrapedProduct:
    """Represents a product scraped from a website."""
    external_id: str
    name: str
    brand: Optional[str]
    size: Optional[str]
    current_price: float
    original_price: Optional[float]
    is_on_sale: bool
    sale_tag: Optional[str]
    url: str
    site: str = "zooplus"
    weight_grams: Optional[int] = None  # Total weight in grams
    original_price_per_kg: Optional[float] = None  # Original price per kg (from website)
    reduced_price_per_kg: Optional[float] = None  # Reduced price per kg (after discount)
    base_product_id: Optional[str] = None  # Base product ID for variant grouping (e.g., "564091")
    variant_name: Optional[str] = None  # Variant descriptor (e.g., "Chicken", "Beef")


class BaseScraper(ABC):
    """Abstract base class for all scrapers with shared constants and utility methods."""

    # Default max price per kg for scraping (ensures we capture deals even if user threshold is lower)
    # User alerts still respect their own max_price setting, but scraping fetches broader range
    DEFAULT_MAX_PRICE_PER_KG = 10.0  # €10/kg covers most quality wet food multipacks

    # Quality wet cat food brands to always scrape (grain-free, high meat, no sugar)
    QUALITY_BRANDS = [
        # High Quality
        "Leonardo", "MAC's", "Catz Finefood", "MjAMjAM", "Animonda",
        "Granatapet", "Wildes Land", "Applaws", "Lily's Kitchen", "Bozita",
        "Terra Faelis", "Venandi Animal", "Carnilove", "Schesir", "Almo Nature",
        "Lucky Lou", "Tundra", "Edgard & Cooper", "Cat's Love", "Hardys",
        "Defu", "The Goodstuff", "Pure Nature", "STRAYZ",
        # Zooplus exclusives
        "Wild Freedom", "Purizon", "Feringa", "KITTY Cat",
        # Mid Quality
        "Miamor", "Sanabelle", "Happy Cat", "Royal Canin",
        "Kattovit", "Brit Care", "Josera",
    ]

    # Actual cat food brands (comprehensive list for extraction)
    BRANDS = [
        # Major premium brands
        "Almo Nature", "Animonda", "Animonda Carny", "Animonda Integra Protect",
        "Applaws", "Bozita", "Catz Finefood", "Concept for Life", "Cosma",
        "Crave", "Dreamies", "Feringa", "Felix", "Gourmet", "Gourmet Gold",
        "Granatapet", "GRAU", "Happy Cat", "Hill's", "Hill's Pet Nutrition",
        "Hill's Prescription Diet", "Hill's Science Plan",
        # K-M brands
        "Kitty Cat", "Kattovit", "Leonardo", "Lily's Kitchen", "Lucky Lou",
        "MAC's", "Mera", "Miamor", "MjAMjAM", "My Star",
        # N-R brands
        "N&D", "Nutrivet", "Perfect Fit", "Porta 21", "Pro Plan", "Purina", "Purina ONE", "Purizon",
        "Rocco", "Rosie's Farm", "Royal Canin", "Royal Canin Veterinary",
        # S-Z brands
        "Sanabelle", "Schesir", "Sheba", "Smilla",
        "Terra Faelis", "Thrive", "Tundra", "Venandi Animal", "Vitakraft", "Whiskas", "Wild Freedom", "Wildes Land",
        # Zooplus own/exclusive brands
        "zooplus Basics", "zooplus Bio",
        # Additional/specialty brands
        "Blink", "Canagan", "Cat's Love", "Cesar", "Encore", "GimCat", "Goood", "Greenies",
        "Josera", "Orijen", "Taste of the Wild", "Weruva", "Yarrah", "Ziwi Peak",
    ]

    # Keywords that indicate NOT wet food (to exclude)
    EXCLUDE_KEYWORDS = [
        "trockenfutter", "trocken", "dry", "kibble",
        "katzenstreu", "streu", "litter",
        "kratzbaum", "kratzmöbel", "spielzeug", "toy",
        "snacks", "leckerli", "treats", "sticks",
        "zubehör", "accessory", "napf", "bowl",
        "bürste", "kamm", "pflege", "shampoo",
        "halsband", "collar", "leine", "leash",
        "transport", "käfig", "korb",
    ]

    # Keywords that indicate wet food (to include)
    WET_FOOD_KEYWORDS = [
        "nassfutter", "dose", "beutel", "pouch", "paté", "pate",
        "mousse", "ragout", "sauce", "gelee", "jelly", "brühe",
        "filet", "schale", "frischebeutel", "multipack",
    ]

    @property
    @abstractmethod
    def SITE_NAME(self) -> str:
        """Site identifier (e.g., 'zooplus', 'bitiba', 'zooroyal')."""
        pass

    @abstractmethod
    async def scrape_brand_products(self, brands: list[str], max_price_per_kg: float = None, max_pages: int = 100, include_default_brands: bool = True) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape products for specific brands."""
        yield []

    @abstractmethod
    async def scrape_reduced_products(self, brands: list[str] = None) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape reduced/sale products."""
        yield []

    def _parse_price(self, price_str: str) -> Optional[float]:
        """Parse German price format (e.g., '12,99 €' -> 12.99)."""
        if not price_str:
            return None
        cleaned = re.sub(r"[€\s]", "", price_str).replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _extract_size(self, name: str) -> Optional[str]:
        """Extract size from product name (e.g., '6 x 400 g', '85 g')."""
        patterns = [
            r"(\d+\s*x\s*\d+\s*g)",
            r"(\d+\s*g)",
            r"(\d+\s*kg)",
            r"(\d+\s*ml)",
        ]
        for pattern in patterns:
            match = re.search(pattern, name, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None

    def _parse_weight_grams(self, name: str) -> Optional[int]:
        """Parse total weight in grams from product name."""
        # Pattern: "6 x 400 g" or "12 x 200 g" etc.
        multi_match = re.search(r'(\d+)\s*x\s*(\d+)\s*g', name, re.IGNORECASE)
        if multi_match:
            count = int(multi_match.group(1))
            weight = int(multi_match.group(2))
            return count * weight

        # Pattern: "400 g" or "800g"
        single_match = re.search(r'(\d+)\s*g(?:\b|$)', name, re.IGNORECASE)
        if single_match:
            return int(single_match.group(1))

        # Pattern: "1 kg" or "1,5 kg"
        kg_match = re.search(r'(\d+(?:[,\.]\d+)?)\s*kg', name, re.IGNORECASE)
        if kg_match:
            kg = float(kg_match.group(1).replace(',', '.'))
            return int(kg * 1000)

        return None

    def _calculate_price_per_kg(self, price: float, weight_grams: Optional[int]) -> Optional[float]:
        """Calculate price per kg."""
        if not weight_grams or weight_grams <= 0:
            return None
        return round(price / (weight_grams / 1000), 2)

    @staticmethod
    def normalize_brand(brand: str) -> str:
        """Normalize brand name for matching (handles apostrophe variants, case)."""
        if not brand:
            return ""
        # Lowercase and normalize apostrophe variants
        normalized = brand.lower()
        # Replace various apostrophe characters with standard one
        normalized = normalized.replace("\xb4", "'")  # acute accent ´
        normalized = normalized.replace("\u2019", "'")  # right single quote '
        normalized = normalized.replace("`", "'")  # backtick
        return normalized

    def _extract_brand(self, name: str) -> Optional[str]:
        """Extract brand from product name."""
        name_normalized = self.normalize_brand(name)
        
        # Sort brands by length (descending) to match "Venandi Animal" before "Grau"
        # This prevents "Grau" (grey) from matching in "Venandi Animal ... grau ..."
        sorted_brands = sorted(self.BRANDS, key=len, reverse=True)
        
        for brand in sorted_brands:
            brand_normalized = self.normalize_brand(brand)
            
            # Use word boundary check to avoid partial word matches
            # Escape regex special characters in brand name
            pattern = r'(?:^|\b|[^a-z0-9])' + re.escape(brand_normalized) + r'(?:\b|[^a-z0-9]|$)'
            
            if re.search(pattern, name_normalized):
                return brand
        return None

    def _extract_variant_name(self, full_name: str, brand: Optional[str], size: Optional[str]) -> Optional[str]:
        """
        Extract variant descriptor from product name.

        Examples:
        - "MAC's Cat 24x400g - Chicken" -> "Chicken"
        - "Leonardo All Meat 6x400g Reich an Huhn" -> "Reich an Huhn"
        - "Animonda Carny Adult 6x400g Rind + Herz" -> "Rind + Herz"
        """
        if not full_name:
            return None

        working_name = full_name

        # Remove brand from the name
        if brand:
            # Try various brand positions
            working_name = re.sub(re.escape(brand), '', working_name, flags=re.IGNORECASE).strip()

        # Remove size patterns (e.g., "24 x 400 g", "6x200g", "85g")
        working_name = re.sub(r'\d+\s*x\s*\d+\s*g\b', '', working_name, flags=re.IGNORECASE).strip()
        working_name = re.sub(r'\b\d+\s*g\b', '', working_name, flags=re.IGNORECASE).strip()
        working_name = re.sub(r'\b\d+\s*kg\b', '', working_name, flags=re.IGNORECASE).strip()

        # Remove common product type words
        common_words = [
            'sparpaket', 'mixpaket', 'probierpaket', 'multipack', 'spar-paket',
            'nassfutter', 'katzenfutter', 'cat', 'katze', 'kitten', 'adult', 'senior',
            'dose', 'dosen', 'schale', 'schalen', 'beutel', 'pouch',
            'all meat', 'classic', 'finest', 'premium', 'bio', 'organic',
            'vetcare', 'vet care', 'sensitive', 'sterilized', 'indoor',
        ]
        for word in common_words:
            working_name = re.sub(r'\b' + re.escape(word) + r'\b', '', working_name, flags=re.IGNORECASE)

        # Clean up extra whitespace and dashes
        working_name = re.sub(r'\s+', ' ', working_name).strip()
        working_name = re.sub(r'^[\s\-–]+|[\s\-–]+$', '', working_name).strip()

        # If there's a dash separator, take what's after it (often the variant)
        if ' - ' in working_name:
            parts = working_name.split(' - ')
            # Take the last non-empty part
            for part in reversed(parts):
                part = part.strip()
                if part and len(part) > 1:
                    return part

        # Return what remains if it looks like a variant name (not too long, not empty)
        if working_name and 2 < len(working_name) < 50:
            return working_name

        return None

    def _is_wet_food(self, name: str, url: str) -> bool:
        """Check if product is wet cat food (not dry food, litter, etc.)."""
        name_lower = name.lower()
        url_lower = url.lower()
        combined = name_lower + " " + url_lower

        # Exclude if contains any exclude keywords
        for keyword in self.EXCLUDE_KEYWORDS:
            if keyword in combined:
                return False

        # Include if URL is in nassfutter category
        if "/nassfutter" in url_lower:
            return True

        # Include if contains wet food keywords
        for keyword in self.WET_FOOD_KEYWORDS:
            if keyword in combined:
                return True

        # Check for common wet food size patterns (g not kg)
        if re.search(r'\d+\s*x\s*\d+\s*g\b', name_lower):
            return True
        if re.search(r'\b\d{2,3}\s*g\b', name_lower):  # 85g, 100g, 200g, 400g typical wet food sizes
            return True

        return False


class BeautifulSoupScraper(BaseScraper):
    """
    Intermediate class for scrapers using BeautifulSoup HTML parsing.

    Used by Zooplus and Bitiba which share the same HTML structure and parsing logic.
    Subclasses only need to provide URL configuration.
    """

    # URL configuration - must be overridden by subclasses
    BASE_URL: str = ""
    CATEGORY_URL: str = ""
    SEARCH_URL: str = ""
    SHOP_LINK_PATTERN: str = ""  # Pattern for product links
    CATEGORY_PATH: str = ""  # Category path for filters
    CATEGORY_PATH_CHECK: str = ""  # Skip category-level URLs

    def __init__(self, browser=None):
        self.browser = browser

    async def _fetch_page_with_js(self, url: str) -> Optional[str]:
        """Fetch page with JavaScript rendering using Playwright."""
        try:
            delay = random.uniform(settings.request_delay_min, settings.request_delay_max)
            await asyncio.sleep(delay)

            # Use existing browser if provided, otherwise context manager (legacy/fallback)
            if self.browser:
                context = await self.browser.new_context(
                    locale='de-DE',
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = await context.new_page()
                try:
                    await page.goto(url, wait_until='networkidle', timeout=60000)
                    
                    # Wait for product cards
                    try:
                        await page.wait_for_selector('[class*="ProductCard"]', timeout=15000)
                    except:
                        try:
                            await page.wait_for_selector('[class*="product"]', timeout=5000)
                        except:
                            pass

                    # Scroll
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                    await asyncio.sleep(1)

                    html = await page.content()
                    return html
                finally:
                    await page.close()
                    await context.close()
            else:
                # Legacy method: launch new browser (should be avoided)
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    # ... (rest of legacy logic, but we should just use the shared one)
                    # For brevity in this refactor, let's just error or assume browser is passed
                    logger.warning("No browser instance provided to Scraper - launching for single use (slow!)")
                    # ... [fallback logic if needed, but we aim to replace calls]
                    # Retaining fallback for safety for now:
                    context = await browser.new_context(
                        locale='de-DE',
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    )
                    page = await context.new_page()
                    await page.goto(url, wait_until='networkidle', timeout=60000)
                    # ... waiting logic ...
                    try:
                         await page.wait_for_selector('[class*="ProductCard"]', timeout=15000)
                    except: pass
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                    await asyncio.sleep(1)
                    html = await page.content()
                    await browser.close()
                    return html

        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def _parse_products_from_html(self, html: str) -> list[ScrapedProduct]:
        """Parse products from rendered HTML."""
        products = []
        soup = BeautifulSoup(html, 'lxml')

        # Find product cards - try multiple selectors
        product_cards = soup.select('[class*="ProductCard_productCard"]')
        if not product_cards:
            product_cards = soup.select('[class*="productCard"]')
        if not product_cards:
            product_cards = soup.select('[data-testid*="product"]')
        if not product_cards:
            product_cards = soup.select(f'a[href*="{self.SHOP_LINK_PATTERN}"]')
            product_cards = [p.parent for p in product_cards if p.parent]

        logger.info(f"Found {len(product_cards)} potential product cards")

        for card in product_cards:
            try:
                product = self._parse_single_product(card)
                if product:
                    products.append(product)
            except Exception as e:
                logger.debug(f"Failed to parse card: {e}")

        return products

    def _clean_product_name(self, raw_name: str) -> str:
        """Clean up product name by removing ratings, prices, and junk text."""
        name = raw_name

        # Remove star ratings text (various formats)
        name = re.sub(r'This is a stars rating area[^:]*:\s*', '', name, flags=re.IGNORECASE)
        name = re.sub(r'from zero to \d+:\s*', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\d+/5\s*\(\d+\)', '', name)  # Remove "5/5(123)"

        # Remove discount percentages at start
        name = re.sub(r'^\d+%\s*Rabatt\s*', '', name)

        # Remove prices at end
        name = re.sub(r'Einzeln\s*[\d,]+\s*€.*$', '', name)
        name = re.sub(r'[\d,]+\s*€\s*/\s*kg.*$', '', name)
        name = re.sub(r'[\d,]+\s*€.*$', '', name)

        # Clean up whitespace
        name = ' '.join(name.split())

        return name.strip()

    def _parse_single_product(self, card) -> Optional[ScrapedProduct]:
        """Parse a single product card."""
        link = card.select_one('a[href*="/shop/"]')
        if not link:
            link = card if card.name == 'a' and '/shop/' in card.get('href', '') else None
        if not link:
            return None

        url = link.get('href', '')
        if not url.startswith('http'):
            url = urljoin(self.BASE_URL, url)

        # Skip category-level URLs (not actual products)
        if self.CATEGORY_PATH_CHECK in url and url.count('/') < 6:
            return None

        # Extract external_id - prefer activeVariant param for full variant ID
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        if 'activeVariant' in query_params:
            raw_id = query_params['activeVariant'][0]  # e.g., "564091.13"
        else:
            match = re.search(r'/(\d+)(?:\?|$|#)', url)
            raw_id = match.group(1) if match else url

        # Extract base product ID for variant grouping (shared across sites for price comparison)
        base_product_id = raw_id.split('.')[0] if '.' in str(raw_id) else str(raw_id)

        # Prefix external_id with site for uniqueness in database
        external_id = f"{self.SITE_NAME}:{raw_id}"

        name = ""
        name_elem = card.select_one('[class*="productName"], [class*="ProductName"], [class*="title"], h2, h3, h4')
        if name_elem:
            name = name_elem.get_text(strip=True)
        if not name:
            name = link.get_text(strip=True)

        # Clean up the name
        name = self._clean_product_name(name)

        if not name or len(name) < 3:
            return None

        text = card.get_text()
        # Remove "activate" text to avoid matching discounts that are not active yet
        text = text.replace("aktivieren", "")

        # Skip unavailable products
        if 'nicht lieferbar' in text.lower() or 'not available' in text.lower():
            return None

        current_price = None
        original_price = None
        is_on_sale = False
        sale_tag = None
        discount_percent = None
        original_price_per_kg = None
        reduced_price_per_kg = None

        # 1. First identify abo prices (including per-kg abo prices) - MUST be done before per-kg extraction
        abo_pattern = r'(?:Abo|Abonnement)[^\d]*(\d+,\d{2})\s*€'
        abo_matches = re.findall(abo_pattern, text, re.IGNORECASE)
        abo_prices = {self._parse_price(p) for p in abo_matches}

        # Also catch "X,XX € Abo" and "X,XX € mit Abo" patterns
        abo_pattern2 = r'(\d+,\d{2})\s*€\s*(?:Abo|mit\s*Abo)'
        abo_matches2 = re.findall(abo_pattern2, text, re.IGNORECASE)
        abo_prices.update(self._parse_price(p) for p in abo_matches2)

        # Also catch abo price per kg patterns like "X,XX € / kg mit Abo"
        abo_per_kg_pattern = r'(\d+,\d{2})\s*€\s*/\s*kg\s*(?:mit\s*)?Abo'
        abo_per_kg_matches = re.findall(abo_per_kg_pattern, text, re.IGNORECASE)
        abo_prices.update(self._parse_price(p) for p in abo_per_kg_matches)

        # Bitiba: unlabeled abo price appears right after per-kg price (e.g., "4,06 € / kg 73,31 €")
        # Pattern: price immediately after "€ / kg " with no text in between
        unlabeled_abo_pattern = r'€\s*/\s*kg\s+(\d+,\d{2})\s*€'
        unlabeled_abo_matches = re.findall(unlabeled_abo_pattern, text, re.IGNORECASE)
        abo_prices.update(self._parse_price(p) for p in unlabeled_abo_matches)


        # 2. Extract price per kg, excluding abo prices
        original_price_per_kg = None
        all_per_kg = re.findall(r'(\d+,\d{2})\s*€\s*/\s*kg', text, re.IGNORECASE)
        for price_str in all_per_kg:
            price = self._parse_price(price_str)
            if price and price not in abo_prices:
                original_price_per_kg = price
                break

        # Find discount percentage from "Extra-Rabatt" badge (e.g., "-20% Extra-Rabatt")
        discount_match = re.search(r'(-?\s*\d+)\s*%\s*(?:Extra-?)?Rabatt', text, re.IGNORECASE)
        if discount_match:
            logger.info(f"Found discount match: {discount_match.group(0)} in card text: {text}")
            discount_percent = abs(int(re.sub(r'[^0-9]', '', discount_match.group(1))))
            sale_tag = f"-{discount_percent}% Rabatt"

            # Calculate reduced price per kg
            if original_price_per_kg:
                reduced_price_per_kg = round(original_price_per_kg * (1 - discount_percent / 100), 2)

        # Identify per-unit prices to exclude from product price
        per_unit_pattern = r'(\d+,\d{2})\s*€\s*/\s*(?:kg|g|ml|l|Stück)'
        per_unit_matches = re.findall(per_unit_pattern, text, re.IGNORECASE)
        per_unit_prices = {self._parse_price(p) for p in per_unit_matches}

        # Identify "Einzeln" (single item) prices to exclude - these are NOT the product price
        einzeln_pattern = r'Einzeln\s*(\d+,\d{2})\s*€'
        einzeln_matches = re.findall(einzeln_pattern, text, re.IGNORECASE)
        einzeln_prices = {self._parse_price(p) for p in einzeln_matches}

        # Identify UVP (recommended retail price) to exclude - handle variations like "UVP | 23,88 €"
        uvp_pattern = r'UVP[^\d]*(\d+,\d{2})\s*€'
        uvp_matches = re.findall(uvp_pattern, text, re.IGNORECASE)
        uvp_prices = {self._parse_price(p) for p in uvp_matches}

        # Look for explicit Mixpaket price first
        mixpaket_price = None
        mixpaket_match = re.search(r'(?:Mixpaket|Mix-?Paket|Sparpaket)[^\d]*(\d+,\d{2})\s*€', text, re.IGNORECASE)
        if mixpaket_match:
            mixpaket_price = self._parse_price(mixpaket_match.group(1))

        # Find all euro prices (excluding per-unit, einzeln, UVP, and Abo prices)
        all_price_matches = re.findall(r'(\d+,\d{2})\s*€', text)
        actual_prices = []
        for p in all_price_matches:
            parsed = self._parse_price(p)
            if parsed and parsed not in per_unit_prices and parsed not in einzeln_prices and parsed not in uvp_prices and parsed not in abo_prices:
                actual_prices.append(parsed)


        # Logic to determine original and current price:
        if mixpaket_price:
            # If we found a Sparpaket/Mixpaket price, it's often the main price
            original_price = mixpaket_price
        elif actual_prices:
            # Pick the most plausible price
            # If we have multiple, the highest is usually the "original" and lowest is "current"
            if len(actual_prices) > 1:
                original_price = max(actual_prices)
                current_price = min(actual_prices)
                # Don't mark as sale - only explicit discount tags should set is_on_sale
            else:
                original_price = actual_prices[0]
                current_price = actual_prices[0]

        # If we have a discount percentage, ensure it's applied
        if discount_percent and original_price:
            current_price = round(original_price * (1 - discount_percent / 100), 2)
            is_on_sale = True


        if not current_price:
            return None

        # Filter: only wet cat food
        if not self._is_wet_food(name, url):
            return None

        # Extract brand and size first (needed for variant name extraction)
        brand = self._extract_brand(name)
        size = self._extract_size(name)
        variant_name = self._extract_variant_name(name, brand, size)

        return ScrapedProduct(
            external_id=str(external_id),
            name=name,
            brand=brand,
            size=size,
            current_price=current_price,
            original_price=original_price,
            is_on_sale=is_on_sale,
            sale_tag=sale_tag,
            url=url,
            site=self.SITE_NAME,
            original_price_per_kg=original_price_per_kg,
            reduced_price_per_kg=reduced_price_per_kg,
            base_product_id=base_product_id,
            variant_name=variant_name
        )

    async def scrape_category(self, max_pages: int = 3) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape wet cat food category pages."""
        seen_ids = set()

        for page_num in range(1, max_pages + 1):
            url = f"{self.CATEGORY_URL}?p={page_num}"
            logger.info(f"Scraping page {page_num}: {url}")

            html = await self._fetch_page_with_js(url)
            if not html:
                break

            try:
                products = await asyncio.to_thread(self._parse_products_from_html, html)
            except Exception as e:
                logger.error(f"Error parsing HTML: {e}")
                products = []

            chunk = []
            new_products = 0
            for p in products:
                if p.external_id not in seen_ids:
                    seen_ids.add(p.external_id)
                    chunk.append(p)
                    new_products += 1

            if chunk:
                yield chunk

            logger.info(f"Page {page_num}: {new_products} new products")

            if new_products == 0:
                break

    async def scrape_reduced_products(self, brands: list[str] = None) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape actually reduced wet cat food using search with Reduziert filter."""
        seen_ids = set()

        # If no brands specified, scrape all reduced wet cat food
        if not brands:
            brands = [None]  # None means no brand filter

        for brand in brands:
            # Build search URL with filters
            # ct=katzen/katzenfutter_dose = wet cat food (Nassfutter)
            # action=Reduziert = actually reduced items
            filters = "action=Reduziert"
            if brand:
                filters += f"~brand={quote(brand)}"

            # Use category path for wet food only
            category_encoded = self.CATEGORY_PATH.replace("/", "%2F")
            url = f"{self.SEARCH_URL}?q=nassfutter&ct={category_encoded}&filters={quote(filters, safe='=~')}"
            logger.info(f"Scraping reduced items{f' for {brand}' if brand else ''}: {url}")

            html = await self._fetch_page_with_js(url)
            if not html:
                continue

            products = await asyncio.to_thread(self._parse_products_from_html, html)
            
            chunk = []
            # All products from this search are actually reduced
            for p in products:
                if p.external_id not in seen_ids:
                    p.is_on_sale = True  # These are confirmed reduced
                    seen_ids.add(p.external_id)
                    chunk.append(p)
            
            if chunk:
                yield chunk

            # Small delay between brand searches
            if brand:
                await asyncio.sleep(1)

    async def scrape_deals_page(self) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape the deals/sale page - now uses search with Reduziert filter."""
        # Use the new method that searches for actually reduced items
        async for chunk in self.scrape_reduced_products():
            yield chunk

    async def scrape_brand_products(self, brands: list[str], max_price_per_kg: float = None, max_pages: int = 100, include_default_brands: bool = True) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape wet cat food for specific brands using a bulk filter for efficiency."""
        # Merge quality brands with user's watched brands
        all_brands = list(set((self.QUALITY_BRANDS if include_default_brands else []) + (brands or [])))

        if not all_brands:
            return

        seen_ids = set()

        # Build bulk brand filter: brand=brand1;brand2;brand3
        # Use canonical names from BRANDS if possible to ensure correct casing
        canonical_brands = []
        for b in all_brands:
            matched = False
            for cb in self.BRANDS:
                if b.lower() == cb.lower():
                    canonical_brands.append(cb)
                    matched = True
                    break
            if not matched:
                canonical_brands.append(b)

        brand_filter = ";".join(canonical_brands)
        filters = f"brand={brand_filter}"

        # Use max of user's price and default to ensure we scrape common price ranges
        scrape_price = max(max_price_per_kg or 0, self.DEFAULT_MAX_PRICE_PER_KG)
        # 30% headroom to catch items that might be discounted into range
        fetch_max = int(scrape_price / 0.7) + 1
        filters += f"~price_per_kg=0;{fetch_max}"

        # URL with bulk filters and sorting
        base_url = f"{self.BASE_URL}/shop/{self.CATEGORY_PATH}?sorting=lowest-price-per-unit&filters={quote(filters, safe='=;~')}"
        logger.info(f"Bulk scraping brands: {', '.join(canonical_brands[:5])}{'...' if len(canonical_brands) > 5 else ''}")

        for page in range(1, max_pages + 1):
            url = f"{base_url}&p={page}" if page > 1 else base_url
            logger.info(f"Scraping bulk page {page}: {url}")

            html = await self._fetch_page_with_js(url)
            if not html:
                break

            products = await asyncio.to_thread(self._parse_products_from_html, html)
            if not products:
                break

            chunk = []
            new_count = 0
            for p in products:
                if p.external_id not in seen_ids:
                    seen_ids.add(p.external_id)
                    chunk.append(p)
                    new_count += 1
            
            if chunk:
                yield chunk

            if new_count == 0:
                break


class ZooplusScraper(BeautifulSoupScraper):
    """Scraper for zooplus.de wet cat food - URL configuration only."""

    # Site identification
    SITE_NAME = "zooplus"

    # URL configuration
    BASE_URL = "https://www.zooplus.de"
    CATEGORY_URL = "https://www.zooplus.de/shop/katzen/katzenfutter/nassfutter"
    SEARCH_URL = "https://www.zooplus.de/search/results"

    # Site-specific URL patterns
    SHOP_LINK_PATTERN = "/shop/katzen/"  # Pattern for product links
    CATEGORY_PATH = "katzen/katzenfutter_dose"  # Category path for filters
    CATEGORY_PATH_CHECK = "/shop/katzen/katzenfutter/nassfutter"  # Skip category-level URLs


class BitibaScraper(BeautifulSoupScraper):
    """Scraper for bitiba.de wet cat food - URL configuration only."""

    # Site identification
    SITE_NAME = "bitiba"

    # URL configuration
    BASE_URL = "https://www.bitiba.de"
    CATEGORY_URL = "https://www.bitiba.de/shop/katze/katzenfutter_nass"
    SEARCH_URL = "https://www.bitiba.de/search/results"

    # Site-specific URL patterns
    SHOP_LINK_PATTERN = "/shop/katze/"  # Pattern for product links
    CATEGORY_PATH = "katze/katzenfutter_nass"  # Category path for filters
    CATEGORY_PATH_CHECK = "/shop/katze/katzenfutter_nass"  # Skip category-level URLs


class FressnapfScraper(BaseScraper):
    """
    Scraper for fressnapf.de wet cat food.

    Fressnapf uses Vue 3/Nuxt 3 with client-side rendering, requiring
    Playwright for JavaScript execution. Uses BeautifulSoup for parsing
    the rendered HTML.

    Note: Fressnapf has limited online selection for quality brands compared
    to Zooplus/Bitiba. They're primarily a retail chain with different brand
    focus (more mainstream brands like Royal Canin, Animonda, fewer premium
    brands like Leonardo, Lily's Kitchen).
    """

    # Site identification
    SITE_NAME = "fressnapf"

    # Higher price threshold for Fressnapf since they're premium retailer
    DEFAULT_MAX_PRICE_PER_KG = 15.0

    # URL configuration
    BASE_URL = "https://www.fressnapf.de"
    CATEGORY_URL = "https://www.fressnapf.de/c/katze/katzenfutter/nassfutter/"

    # Fressnapf brand codes for URL filtering
    # Format: ?q=::brand:CODE1:brand:CODE2
    # Note: Many quality brands (Leonardo, Lily's Kitchen, Wildes Land, etc.)
    # are not available on Fressnapf - they focus on different brand portfolio
    BRAND_CODES = {
        # High Quality brands available on Fressnapf
        "mac's": "MACS",
        "catz finefood": "CATZ",
        "mjamjam": "MJAM",
        "animonda": "ANIM",
        "granatapet": "GRAN",
        "applaws": "APPL",
        "bozita": "BOZI",
        "schesir": "SCHE",
        "almo nature": "ALMO",
        "edgard & cooper": "EDGA",
        "hardys": "HARD",
        "strayz": "STRZ",
        # Mid Quality brands
        "miamor": "MIAM",
        "sanabelle": "SANA",
        "happy cat": "HAPC",
        "royal canin": "ROYA",
        "kattovit": "KATT",
        "josera": "JOSE",
        # Other common brands
        "gourmet": "GOUR",
        "felix": "FELI",
        "sheba": "SHEB",
        "whiskas": "WHIS",
        "yarrah": "YARR",
        "hill's": "HILL",
        "iams": "IAMS",
        "vitakraft": "VITA",
        "select gold": "SELE",
        "schmusy": "SCHM",
        "real nature": "RENA",
        "premiere": "PREM",
        "perfect fit": "PERF",
        "purina one": "ONE",
        "mera": "MERA_7",
        "kitekat": "KITE",
    }

    # Concurrency limit for parallel scraping
    MAX_CONCURRENT_BRANDS = 3

    def _is_wet_food(self, name: str, url: str) -> bool:
        """
        Check if product is wet cat food.

        For Fressnapf, we're scraping from the wet cat food category
        (/c/katze/katzenfutter/nassfutter/), so products are wet food by default.
        Only exclude obvious non-food items like snacks/treats.
        """
        name_lower = name.lower()

        # Exclude snacks/treats
        if any(kw in name_lower for kw in ['snack', 'leckerli', 'treat', 'sticks', 'dreamies', 'knuspies', 'soup', 'suppe']):
            return False

        # Exclude dry food keywords
        if any(kw in name_lower for kw in ['trockenfutter', 'trocken', 'kibble']):
            return False

        # All other products from wet food category are valid
        return True

    def __init__(self, browser=None):
        self.browser = browser

    async def _fetch_page_with_js(self, url: str) -> Optional[str]:
        """Fetch page with JavaScript rendering using Playwright."""
        try:
            delay = random.uniform(settings.request_delay_min, settings.request_delay_max)
            await asyncio.sleep(delay)

            if self.browser:
                context = await self.browser.new_context(
                    locale='de-DE',
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = await context.new_page()
                try:
                    await page.goto(url, wait_until='networkidle', timeout=60000)
                    
                    try:
                        await page.wait_for_selector('.product-teaser', timeout=15000)
                    except Exception:
                        logger.warning(f"No product teasers found on {url}")

                    for _ in range(2):
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(1)

                    html = await page.content()
                    return html
                finally:
                    await page.close()
                    await context.close()
            else:
                # Fallback
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    # ... [minimal fallback implementation]
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.goto(url, wait_until='networkidle')
                    html = await page.content()
                    await browser.close()
                    return html

        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def _parse_products_from_html(self, html: str) -> list[ScrapedProduct]:
        """Parse products from rendered Fressnapf HTML."""
        products = []
        soup = BeautifulSoup(html, 'lxml')

        teasers = soup.select('.product-teaser')
        logger.info(f"Found {len(teasers)} product teasers")

        for teaser in teasers:
            try:
                product = self._parse_single_product(teaser)
                if product:
                    products.append(product)
            except Exception as e:
                logger.debug(f"Failed to parse teaser: {e}")

        return products

    def _parse_single_product(self, teaser) -> Optional[ScrapedProduct]:
        """Parse a single Fressnapf product teaser."""
        # Get product link and URL
        link = teaser.select_one('a.pt-header')
        if not link:
            return None

        href = link.get('href', '')
        if not href or '/p/' not in href:
            return None

        url = urljoin(self.BASE_URL, href)

        # Extract product ID from URL (e.g., /p/product-name-123456/ -> 123456)
        id_match = re.search(r'-(\d+)/?$', href)
        external_id = f"{self.SITE_NAME}:{id_match.group(1)}" if id_match else f"{self.SITE_NAME}:{href}"

        # Get brand and name
        brand_elem = teaser.select_one('.pt-subhead')
        name_elem = teaser.select_one('.pt-head')

        brand = brand_elem.get_text(strip=True) if brand_elem else None
        name = name_elem.get_text(strip=True) if name_elem else None

        if not name:
            return None

        # Get prices
        price_elem = teaser.select_one('.p-regular-price.p-price:not(.p-per-unit):not(.p-friends-price)')
        per_kg_elem = teaser.select_one('.p-per-unit')

        current_price = None
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            current_price = self._parse_price(price_text)

        if not current_price:
            return None

        # Parse price per kg
        original_price_per_kg = None
        if per_kg_elem:
            per_kg_text = per_kg_elem.get_text(strip=True)
            # Extract number from "(X,XX €/kg)"
            match = re.search(r'([\d,]+)\s*€/kg', per_kg_text)
            if match:
                original_price_per_kg = self._parse_price(match.group(1))

        # Check for sale/strike prices
        strike_elem = teaser.select_one('.p-strike-price')
        original_price = current_price
        is_on_sale = False
        sale_tag = None

        if strike_elem:
            strike_text = strike_elem.get_text(strip=True)
            strike_price = self._parse_price(strike_text)
            if strike_price and strike_price > current_price:
                original_price = strike_price
                is_on_sale = True
                discount_pct = int((1 - current_price / original_price) * 100)
                sale_tag = f"-{discount_pct}%"

        # Check for discount badges
        badges = teaser.select('[class*="badge"]')
        for badge in badges:
            badge_text = badge.get_text(strip=True)
            if '%' in badge_text and '-' in badge_text:
                match = re.search(r'-?\s*(\d+)\s*%', badge_text)
                if match:
                    is_on_sale = True
                    sale_tag = f"-{match.group(1)}%"
                    break

        # Filter: only wet cat food
        if not self._is_wet_food(name, url):
            return None

        # Parse weight and calculate prices
        size = self._extract_size(name)
        weight_grams = self._parse_weight_grams(name)

        # Calculate reduced price per kg if on sale
        reduced_price_per_kg = None
        if is_on_sale and weight_grams:
            reduced_price_per_kg = self._calculate_price_per_kg(current_price, weight_grams)

        # Extract variant name
        variant_name = self._extract_variant_name(name, brand, size)

        # Base product ID (numeric part of URL)
        base_product_id = id_match.group(1) if id_match else None

        return ScrapedProduct(
            external_id=external_id,
            name=name,
            brand=brand or self._extract_brand(name),
            size=size,
            current_price=current_price,
            original_price=original_price,
            is_on_sale=is_on_sale,
            sale_tag=sale_tag,
            url=url,
            site=self.SITE_NAME,
            weight_grams=weight_grams,
            original_price_per_kg=original_price_per_kg,
            reduced_price_per_kg=reduced_price_per_kg,
            base_product_id=base_product_id,
            variant_name=variant_name
        )

    def _get_brand_filter_url(self, brands: list[str]) -> str:
        """Build URL with brand filter query string."""
        brand_codes = []
        for brand in brands:
            brand_lower = self.normalize_brand(brand)
            if brand_lower in self.BRAND_CODES:
                brand_codes.append(self.BRAND_CODES[brand_lower])
            else:
                # Try partial match
                for known_brand, code in self.BRAND_CODES.items():
                    if brand_lower in known_brand or known_brand in brand_lower:
                        brand_codes.append(code)
                        break

        if not brand_codes:
            return self.CATEGORY_URL

        # Build query string: ?q=::brand:CODE1:brand:CODE2
        brand_params = ':'.join(f'brand:{code}' for code in set(brand_codes))
        return f"{self.CATEGORY_URL}?q=::{brand_params}"

    async def scrape_brand_products(self, brands: list[str], max_price_per_kg: float = None, max_pages: int = 20, include_default_brands: bool = True) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape wet cat food for specific brands from Fressnapf."""
        # Merge quality brands with user's watched brands
        all_brands = list(set((self.QUALITY_BRANDS if include_default_brands else []) + (brands or [])))

        # Get brands that have Fressnapf codes (Fressnapf has limited brand selection)
        valid_brands = []
        for brand in all_brands:
            brand_lower = self.normalize_brand(brand)
            if brand_lower in self.BRAND_CODES:
                valid_brands.append(brand)
            else:
                # Try partial match
                for known_brand in self.BRAND_CODES.keys():
                    if brand_lower in known_brand or known_brand in brand_lower:
                        valid_brands.append(brand)
                        break

        if not valid_brands:
            logger.warning("No valid Fressnapf brand codes found")
            return

        logger.info(f"Scraping Fressnapf for {len(valid_brands)} brands (of {len(all_brands)} requested)")

        seen_ids = set()

        # Build URL with all brand filters
        url = self._get_brand_filter_url(valid_brands)
        logger.info(f"Fressnapf brand filter URL: {url}")

        for page_num in range(1, max_pages + 1):
            page_url = f"{url}&page={page_num}" if page_num > 1 else url
            logger.info(f"Scraping Fressnapf page {page_num}")

            html = await self._fetch_page_with_js(page_url)
            if not html:
                break

            products = await asyncio.to_thread(self._parse_products_from_html, html)
            if not products:
                break

            chunk = []
            new_count = 0
            for p in products:
                # Apply price filter
                scrape_price = max(max_price_per_kg or 0, self.DEFAULT_MAX_PRICE_PER_KG)
                price_per_kg = p.reduced_price_per_kg or p.original_price_per_kg
                if price_per_kg and price_per_kg > scrape_price * 1.3:  # 30% headroom
                    continue

                if p.external_id not in seen_ids:
                    seen_ids.add(p.external_id)
                    chunk.append(p)
                    new_count += 1
            
            if chunk:
                yield chunk

            if new_count == 0 and page_num > 1:
                break

    async def scrape_reduced_products(self, brands: list[str] = None) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape reduced/sale products from Fressnapf."""
        seen_ids = set()

        # Use sale filter: aktionen=angebote
        base_url = f"{self.CATEGORY_URL}?aktionen=angebote"

        # Add brand filter if specified
        if brands:
            brand_url = self._get_brand_filter_url(brands)
            if '?' in brand_url:
                base_url = f"{brand_url}&aktionen=angebote"

        logger.info(f"Scraping Fressnapf reduced items: {base_url}")

        html = await self._fetch_page_with_js(base_url)
        if not html:
            return

        products = await asyncio.to_thread(self._parse_products_from_html, html)
        
        chunk = []

        for p in products:
            if p.external_id not in seen_ids:
                # Mark as on sale if from deals page
                if not p.is_on_sale:
                    p.is_on_sale = True
                seen_ids.add(p.external_id)
                chunk.append(p)
        
        if chunk:
            yield chunk


class ZooroyalScraper(BaseScraper):
    """
    Scraper for zooroyal.de wet cat food.

    Zooroyal uses Stencil.js web components with shadow DOM, requiring
    JavaScript-based extraction via Playwright rather than BeautifulSoup parsing.
    Inherits directly from BaseScraper (not BeautifulSoupScraper).
    """

    # Site identification
    SITE_NAME = "zooroyal"

    # URL configuration
    BASE_URL = "https://www.zooroyal.de"
    CATEGORY_URL = "https://www.zooroyal.de/katze/katzenfutter/katzen-nassfutter/"
    SEARCH_URL = "https://www.zooroyal.de/search"

    # Site-specific URL patterns
    SHOP_LINK_PATTERN = "/katze/"
    CATEGORY_PATH = "katze/katzenfutter/katzen-nassfutter"
    CATEGORY_PATH_CHECK = "/katze/katzenfutter/katzen-nassfutter"

    # Zooroyal brand slugs for URL path filtering
    # URL format: https://www.zooroyal.de/katze/katzenfutter/katzen-nassfutter/{slug}
    BRAND_SLUGS = {
        # A
        "almo nature": "almo-nature",
        "animonda": "animonda-carny",
        "animonda carny": "animonda-carny",
        "animonda integra protect": "animonda-integra-protect",
        "animonda vom feinsten": "animonda-vom-feinsten",
        "applaws": "applaws",
        # B
        "bewi cat": "bewi-cat",
        "bozita": "bozita",
        "brit": "brit",
        "brit care": "brit-care",
        # C
        "carnilove": "carnilove",
        "cat's love": "cat-s-love",
        "catz finefood": "catz-finefood",
        # D
        "defu": "defu",
        "dogs'n tiger": "dogsn-tiger",
        "dreamies": "dreamies",
        # E
        "edgard & cooper": "edgard-cooper",
        # F
        "felix": "felix",
        # G
        "gimcat": "gimcat",
        "gourmet": "gourmet",
        "granatapet": "granatapet",
        "green petfood": "green-petfood",
        # H
        "happy cat": "happy-cat",
        "hardys": "hardys",
        # J
        "ja": "ja",
        "joe & pepper": "joe-pepper",
        "josera": "josera",
        "josera help": "josera-help",
        "josicat": "josicat",
        # K
        "kattovit": "kattovit",
        "kitekat": "kitekat",
        # L
        "leonardo": "leonardo",
        "lily's kitchen": "lily-s-kitchen",
        "lucky lou": "lucky-lou",
        # M
        "mac's": "macs",
        "mac's cat": "macs",
        "mac's vetcare": "mac-s-vetcare",
        "miamor": "miamor",
        "mjamjam": "mjamjam",
        "mjamjam vetcat": "mjamjam-vetcat",
        # P
        "pawsome!": "pawsome",
        "perfect fit": "perfect-fit",
        "pure nature": "pure-nature",
        "purina one": "purina-one",
        # R
        "royal canin": "royal-canin",
        # S
        "sanabelle": "sanabelle",
        "sanabelle heimat": "sanabelle-heimat",
        "schesir": "schesir",
        "schmusy": "schmusy",
        "sheba": "sheba",
        "strayz": "strayz",
        # T
        "terra felis": "terra-felis",
        "the goodstuff": "the-goodstuff",
        "tundra": "tundra",
        # V
        "venandi animal": "venandi-animal",
        "vet life": "vet-life",
        "vitakraft": "vitakraft",
        # W
        "whiskas": "whiskas",
        "wildes land": "wildes-land",
        "wow! cat": "wow-cat",
        # Z
        "zooroyal": "zooroyal",
        "zooroyal minkas naturkost": "zooroyal-minkas-natur",
    }

    # Concurrency limit for parallel brand scraping (to avoid rate limiting)
    MAX_CONCURRENT_BRANDS = 3

    # Quality brand slugs to always scrape (mapped from QUALITY_BRANDS)
    QUALITY_BRAND_SLUGS = [
        "leonardo",
        "macs",
        "catz-finefood",
        "mjamjam",
        "animonda-carny",
        "animonda-vom-feinsten",
        "granatapet",
        "wildes-land",
        "applaws",
        "lily-s-kitchen",
        "bozita",
        "terra-felis",
        "venandi-animal",
        "carnilove",
        "schesir",
        "almo-nature",
        "lucky-lou",
        "tundra",
        "edgard-cooper",
        "cat-s-love",
        "hardys",
        "defu",
        "the-goodstuff",
        "pure-nature",
        "strayz",
        # Mid Quality
        "miamor",
        "sanabelle",
        "happy-cat",
        "royal-canin",
        "kattovit",
        "brit-care",
        "josera",
    ]

    def _is_wet_food(self, name: str, url: str) -> bool:
        """
        Check if product is wet cat food.

        For Zooroyal, we're scraping from the wet cat food category
        (/katze/katzenfutter/katzen-nassfutter/), so all products are wet food
        by default. Only exclude obvious non-food items.
        """
        name_lower = name.lower()

        # Exclude snacks/treats
        if any(kw in name_lower for kw in ['snack', 'leckerli', 'treat', 'sticks', 'dreamies', 'knuspies']):
            return False

        # All other products from wet food category are valid
        return True

    # JavaScript to extract products from shadow DOM
    EXTRACT_PRODUCTS_JS = '''() => {
        const products = [];
        const tiles = document.querySelectorAll('zr-product-tile');

        tiles.forEach(tile => {
            const shadow = tile.shadowRoot;
            if (!shadow) return;

            const product = {};

            // Get main link
            const link = shadow.querySelector('a[href]');
            product.url = link ? link.href : '';

            // Get brand from supplier div
            const supplier = shadow.querySelector('.zr-product-tile__supplier');
            product.brand = supplier ? supplier.textContent.trim() : '';

            // Get product name and aria-label (contains name + size + price)
            const nameEl = shadow.querySelector('.zr-product-tile__name');
            product.name = nameEl ? nameEl.textContent.trim() : '';
            product.ariaLabel = nameEl ? nameEl.getAttribute('aria-label') : '';

            // Get size from variant div
            const variantEl = shadow.querySelector('.zr-product-tile__current-variant');
            if (variantEl) {
                // Extract just the size part (e.g., "12x400g")
                const variantText = variantEl.textContent.trim();
                const sizeMatch = variantText.match(/^([\\d]+x?[\\d]*(?:g|kg|ml))/i);
                product.size = sizeMatch ? sizeMatch[1] : variantText.split('\\u200B')[0].trim();
            }

            // Get badges (discount info)
            const badgesComp = shadow.querySelector('zr-badges');
            if (badgesComp && badgesComp.shadowRoot) {
                const badges = badgesComp.shadowRoot.querySelectorAll('zr-badge');
                product.badges = [];
                badges.forEach(badge => {
                    if (badge.shadowRoot) {
                        const text = badge.shadowRoot.textContent.trim();
                        if (text) product.badges.push(text);
                    }
                });
            }

            if (product.url) products.push(product);
        });

        return products;
    }'''

    def _parse_aria_label_price(self, aria_label: str) -> Optional[float]:
        """Extract price from aria-label (e.g., 'Product Name 12x400g 20.99 EUR')."""
        if not aria_label:
            return None
        # Match price pattern at end: "XX.XX EUR" or "XX,XX EUR"
        match = re.search(r'([\d,.]+)\s*EUR\s*$', aria_label, re.IGNORECASE)
        if match:
            return self._parse_price(match.group(1))
        return None

    def _parse_discount_from_badges(self, badges: list) -> Optional[int]:
        """Extract discount percentage from badges (e.g., '- 27 %')."""
        if not badges:
            return None
        for badge in badges:
            match = re.search(r'-\s*(\d+)\s*%', badge)
            if match:
                return int(match.group(1))
        return None

    def _extract_external_id(self, url: str) -> str:
        """Extract external ID from Zooroyal URL slug."""
        # URL format: https://www.zooroyal.de/animonda-carny-mix-2-adult-12x400g
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        # Remove query params for clean ID, but include sDetail if present
        query = parse_qs(parsed.query)
        base_id = path.split('/')[-1] if '/' in path else path
        if 'sDetail' in query:
            base_id = f"{base_id}:{query['sDetail'][0]}"
        return f"{self.SITE_NAME}:{base_id}"

    def __init__(self, browser=None):
        self.browser = browser

    async def _fetch_and_extract_products(self, url: str) -> list[dict]:
        """Fetch page and extract product data using JavaScript (for shadow DOM)."""
        try:
            delay = random.uniform(settings.request_delay_min, settings.request_delay_max)
            await asyncio.sleep(delay)

            if self.browser:
                context = await self.browser.new_context(
                    locale='de-DE',
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = await context.new_page()
                try:
                    await page.goto(url, wait_until='networkidle', timeout=60000)
                    
                    try:
                        await page.wait_for_selector('zr-product-tile', timeout=15000)
                    except Exception:
                        logger.warning(f"No product tiles found on {url}")
                        return []

                    for _ in range(3):
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(1.5)

                    products = await page.evaluate(self.EXTRACT_PRODUCTS_JS)
                    return products
                finally:
                    await page.close()
                    await context.close()
            else:
                 # Fallback
                async with async_playwright() as p:
                    # ... [same fallback logic]
                    browser = await p.chromium.launch(headless=True)
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.goto(url)
                    products = await page.evaluate(self.EXTRACT_PRODUCTS_JS)
                    await browser.close()
                    return products

        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return []

    def _convert_to_scraped_product(self, data: dict) -> Optional[ScrapedProduct]:
        """Convert raw extracted data to ScrapedProduct."""
        url = data.get('url', '')
        if not url:
            return None

        # Skip sponsored links (usually have tracking params)
        if 'sponsored=display' in url:
            # Clean URL by removing sponsored param
            url = re.sub(r'[?&]sponsored=display[^&]*', '', url)
            url = url.replace('?&', '?').rstrip('?')

        name = data.get('name', '')
        if not name:
            return None

        # Get brand
        brand = data.get('brand', '')

        # Get size from data or parse from name
        size = data.get('size', '') or self._extract_size(name)

        # Parse price from aria-label
        aria_label = data.get('ariaLabel', '')
        current_price = self._parse_aria_label_price(aria_label)

        if not current_price:
            return None

        # Parse discount from badges
        badges = data.get('badges', [])
        discount_percent = self._parse_discount_from_badges(badges)

        # Calculate original price if discounted
        original_price = current_price
        is_on_sale = False
        sale_tag = None

        if discount_percent:
            # Current price is already discounted, calculate original
            original_price = round(current_price / (1 - discount_percent / 100), 2)
            is_on_sale = True
            sale_tag = f"-{discount_percent}%"

        # Extract external ID
        external_id = self._extract_external_id(url)

        # Extract base product ID (URL slug without variant)
        base_product_id = external_id.split(':')[1].split(':')[0] if ':' in external_id else external_id

        # Filter: only wet cat food
        if not self._is_wet_food(name, url):
            return None

        # Parse weight and calculate price per kg
        weight_grams = self._parse_weight_grams(f"{name} {size}")
        original_price_per_kg = self._calculate_price_per_kg(original_price, weight_grams)
        reduced_price_per_kg = self._calculate_price_per_kg(current_price, weight_grams) if is_on_sale else None

        # Extract variant name
        variant_name = self._extract_variant_name(name, brand, size)

        return ScrapedProduct(
            external_id=external_id,
            name=f"{name} {size}".strip() if size and size not in name else name,
            brand=brand or self._extract_brand(name),
            size=size,
            current_price=current_price,
            original_price=original_price,
            is_on_sale=is_on_sale,
            sale_tag=sale_tag,
            url=url,
            site=self.SITE_NAME,
            weight_grams=weight_grams,
            original_price_per_kg=original_price_per_kg,
            reduced_price_per_kg=reduced_price_per_kg,
            base_product_id=base_product_id,
            variant_name=variant_name
        )

    async def scrape_category(self, max_pages: int = 3) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape wet cat food category pages from Zooroyal."""
        seen_ids = set()

        for page_num in range(1, max_pages + 1):
            url = f"{self.CATEGORY_URL}?p={page_num}"
            logger.info(f"Scraping Zooroyal page {page_num}: {url}")

            raw_products = await self._fetch_and_extract_products(url)

            chunk = []
            new_products = 0
            for data in raw_products:
                product = self._convert_to_scraped_product(data)
                if product and product.external_id not in seen_ids:
                    seen_ids.add(product.external_id)
                    chunk.append(product)
                    new_products += 1

            if chunk:
                yield chunk

            logger.info(f"Zooroyal page {page_num}: {new_products} new products")

            if new_products == 0:
                break

    async def scrape_reduced_products(self, brands: list[str] = None) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape reduced/sale products from Zooroyal."""
        seen_ids = set()

        # Zooroyal search doesn't reliably filter by brand, so we scrape
        # the category page sorted by discount and filter client-side
        url = f"{self.CATEGORY_URL}?sSort=7"  # Sort by discount/sale
        logger.info(f"Scraping Zooroyal reduced items")

        raw_products = await self._fetch_and_extract_products(url)

        chunk = []
        for data in raw_products:
            product = self._convert_to_scraped_product(data)
            if not product or product.external_id in seen_ids:
                continue

            # Only include if actually on sale
            if not product.is_on_sale:
                continue

            # Filter by brand if specified
            if brands and not self._matches_brand(product.brand, brands):
                continue

            seen_ids.add(product.external_id)
            chunk.append(product)

        if chunk:
            yield chunk

        logger.info(f"Zooroyal found {len(chunk)} reduced products")

    def _matches_brand(self, product_brand: str, watched_brands: list[str]) -> bool:
        """
        Check if product brand matches any watched brand (case-insensitive).

        Handles Zooroyal's brand naming differences:
        - "MAC's Cat" matches "MAC's"
        - "animonda Carny" / "animonda vom Feinsten" matches "Animonda"
        """
        if not product_brand:
            return False
        product_brand_lower = self.normalize_brand(product_brand)

        for watched in watched_brands:
            watched_lower = self.normalize_brand(watched)
            # Check both directions for partial matches
            if watched_lower in product_brand_lower or product_brand_lower in watched_lower:
                return True

            # Handle special cases: first word match (e.g., "animonda" in "animonda Carny")
            product_first_word = product_brand_lower.split()[0] if product_brand_lower else ""
            watched_first_word = watched_lower.split()[0] if watched_lower else ""
            if product_first_word and watched_first_word:
                if product_first_word == watched_first_word:
                    return True
                # Also check if watched brand starts with product's first word or vice versa
                if watched_lower.startswith(product_first_word) or product_first_word.startswith(watched_lower):
                    return True

        return False

    def _get_brand_slugs(self, brands: list[str], include_default_brands: bool = True) -> list[str]:
        """Get Zooroyal brand URL slugs, always including quality brands."""
        # Start with quality brand slugs
        slugs = list(self.QUALITY_BRAND_SLUGS) if include_default_brands else []

        # Add any additional watched brands from user
        for brand in (brands or []):
            brand_lower = self.normalize_brand(brand)
            # Direct match
            if brand_lower in self.BRAND_SLUGS:
                slugs.append(self.BRAND_SLUGS[brand_lower])
            else:
                # Try partial match
                for known_brand, slug in self.BRAND_SLUGS.items():
                    if brand_lower in known_brand or known_brand in brand_lower:
                        slugs.append(slug)
                        break
        return list(set(slugs))  # Remove duplicates

    async def _scrape_single_brand(self, slug: str, max_price_per_kg: float, max_pages: int, semaphore: asyncio.Semaphore) -> AsyncGenerator[list[ScrapedProduct], None]:
        """
        Scrape a single brand from Zooroyal (used for parallel execution).
        Yields chunks of products.
        """
        brand_url = f"{self.CATEGORY_URL}{slug}"

        async with semaphore:
            logger.info(f"Scraping Zooroyal brand: {slug}")

            for page_num in range(1, max_pages + 1):
                url = f"{brand_url}?p={page_num}" if page_num > 1 else brand_url

                raw_products = await self._fetch_and_extract_products(url)

                if not raw_products:
                    break

                chunk = []
                new_count = 0
                for data in raw_products:
                    product = self._convert_to_scraped_product(data)
                    if not product:
                        continue

                    # Apply price filter using max of user's price and default
                    scrape_price = max(max_price_per_kg or 0, self.DEFAULT_MAX_PRICE_PER_KG)
                    price_per_kg = product.reduced_price_per_kg or product.original_price_per_kg
                    if price_per_kg and price_per_kg > scrape_price * 1.3:  # 30% headroom
                        continue

                    chunk.append(product)
                    new_count += 1
                
                if chunk:
                    yield chunk

                if new_count == 0 and page_num > 1:
                    break

    async def scrape_brand_products(self, brands: list[str], max_price_per_kg: float = None, max_pages: int = 10, include_default_brands: bool = True) -> AsyncGenerator[list[ScrapedProduct], None]:
        """
        Scrape wet cat food for specific brands from Zooroyal.
        Yields chunks of products from all parallel brand scrapes.
        """
        # Get brand slugs
        brand_slugs = self._get_brand_slugs(brands, include_default_brands=include_default_brands)

        if not brand_slugs:
            logger.warning(f"No Zooroyal brand slugs found")
            return

        logger.info(f"Scraping Zooroyal for {len(brand_slugs)} brands ({self.MAX_CONCURRENT_BRANDS} concurrent)")

        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_BRANDS)
        queue = asyncio.Queue()
        
        # Producer function to drive a single brand scrape and push chunks to queue
        async def producer(slug):
            try:
                async for chunk in self._scrape_single_brand(slug, max_price_per_kg, max_pages, semaphore):
                    await queue.put(chunk)
            except Exception as e:
                logger.error(f"Error scraping Zooroyal brand {slug}: {e}")
            finally:
                await queue.put(None) # Signal completion for this producer

        # Start producers
        active_producers = 0
        for slug in brand_slugs:
            asyncio.create_task(producer(slug))
            active_producers += 1
            
        # Consume from queue
        seen_ids = set()
        while active_producers > 0:
            item = await queue.get()
            if item is None:
                active_producers -= 1
            else:
                # Deduplicate yielded items against this session
                chunk = []
                for p in item:
                    if p.external_id not in seen_ids:
                        seen_ids.add(p.external_id)
                        chunk.append(p)
                if chunk:
                    yield chunk


class Zoo24Scraper(BaseScraper):
    """
    Scraper for zoo24.de wet cat food.

    Zoo24 is a Shopify-based German pet shop. Uses search-based approach
    (no category URLs). Products are sorted by price ascending, enabling
    early termination when price exceeds threshold. Unit price per kg is
    directly provided in HTML via <unit-price> elements.
    """

    SITE_NAME = "zoo24"
    BASE_URL = "https://www.zoo24.de"
    SEARCH_URL = "https://www.zoo24.de/search"
    MAX_SEARCH_PAGES = 35

    MAX_SEARCH_PAGES = 35

    def __init__(self, browser=None):
        self.browser = browser

    def _is_wet_food(self, name: str) -> bool:
        """Check if product is wet cat food (exclude dry food, snacks, accessories)."""
        name_lower = name.lower()

        for kw in self.EXCLUDE_KEYWORDS:
            if kw in name_lower:
                return False

        # Must have at least one wet food indicator or weight pattern (dose/pouch products)
        if any(kw in name_lower for kw in self.WET_FOOD_KEYWORDS):
            return True

        # Products with weight patterns (e.g., 6x200g) from search "nassfutter katze" are likely wet food
        if re.search(r'\d+\s*x\s*\d+\s*g', name_lower):
            return True
        if re.search(r'\b\d+\s*g\b', name_lower):
            return True

        return False

    async def _fetch_page_with_js(self, url: str) -> Optional[str]:
        """Fetch page with JavaScript rendering using Playwright."""
        try:
            delay = random.uniform(settings.request_delay_min, settings.request_delay_max)
            await asyncio.sleep(delay)

            if self.browser:
                context = await self.browser.new_context(
                    locale='de-DE',
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = await context.new_page()
                try:
                    await page.goto(url, wait_until='domcontentloaded', timeout=30000)

                    # Wait for product cards to render
                    try:
                        await page.wait_for_selector('product-card', timeout=20000)
                    except Exception:
                        logger.warning(f"No product-card elements found on {url}")
                        return None

                    # Scroll to load lazy content
                    for _ in range(3):
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(1)

                    html = await page.content()
                    return html
                finally:
                    await page.close()
                    await context.close()
            else:
                 # Fallback
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.goto(url, wait_until='domcontentloaded')
                    html = await page.content()
                    await browser.close()
                    return html

        except Exception as e:
            logger.error(f"Zoo24: Failed to fetch {url}: {e}")
            return None

    def _parse_products_from_html(self, html: str) -> list[ScrapedProduct]:
        """Parse products from rendered Zoo24 HTML."""
        products = []
        soup = BeautifulSoup(html, 'lxml')

        cards = soup.find_all('product-card')
        logger.info(f"Zoo24: Found {len(cards)} product cards")

        for card in cards:
            try:
                product = self._parse_single_product(card)
                if product:
                    products.append(product)
            except Exception as e:
                logger.debug(f"Zoo24: Failed to parse card: {e}")

        return products

    def _parse_single_product(self, card) -> Optional[ScrapedProduct]:
        """Parse a single Zoo24 product card."""
        # Get handle for external ID
        handle = card.get('handle', '')
        if not handle:
            return None

        external_id = f"zoo24:{handle}"

        # Get product link
        link = card.find('a', href=re.compile(r'/products/'))
        if not link:
            return None

        href = link.get('href', '')
        url = urljoin(self.BASE_URL, href)

        # Get title
        title_elem = card.select_one('.product-card__title')
        name = title_elem.get_text(strip=True) if title_elem else None
        if not name:
            return None

        # Filter: only wet cat food
        if not self._is_wet_food(name):
            return None

        # Get current price from <sale-price>
        # Text format can be "Angebotab 1,39 €" or just "1,39 €"
        sale_price_elem = card.find('sale-price')
        if not sale_price_elem:
            return None
        sale_text = sale_price_elem.get_text(strip=True)
        # Extract price pattern (digits with comma/dot separator + optional €)
        price_match = re.search(r'(\d+[.,]\d+)\s*€', sale_text)
        if not price_match:
            return None
        current_price = self._parse_price(price_match.group(1))
        if not current_price:
            return None

        # Check for original price (compare-at-price = strikethrough price when on sale)
        compare_elem = card.find('compare-at-price')
        original_price = current_price
        is_on_sale = False
        sale_tag = None

        if compare_elem:
            compare_text = compare_elem.get_text(strip=True)
            if compare_text:
                compare_price = self._parse_price(compare_text)
                if compare_price and compare_price > current_price:
                    original_price = compare_price
                    is_on_sale = True
                    discount_pct = int((1 - current_price / original_price) * 100)
                    sale_tag = f"-{discount_pct}%"

        # Get unit price per kg from <unit-price>
        unit_price_elem = card.find('unit-price')
        unit_price_per_kg = None
        if unit_price_elem:
            unit_text = unit_price_elem.get_text(strip=True)
            # Extract number from "(X,XX €/kg)" or "X,XX €/kg"
            match = re.search(r'([\d.,]+)\s*€/kg', unit_text)
            if match:
                unit_price_per_kg = self._parse_price(match.group(1))

        # Determine price per kg values
        # unit-price shows the current (possibly reduced) price per kg
        original_price_per_kg = unit_price_per_kg
        reduced_price_per_kg = None

        if is_on_sale and unit_price_per_kg:
            # unit-price is the sale price per kg
            reduced_price_per_kg = unit_price_per_kg
            # Back-calculate original price per kg from the price ratio
            if current_price > 0:
                original_price_per_kg = round(unit_price_per_kg * (original_price / current_price), 2)

        # Extract brand
        brand = self._extract_brand(name)

        # Extract size and weight
        size = self._extract_size(name)
        weight_grams = self._parse_weight_grams(name)

        # If no unit price from HTML, calculate from weight
        if not original_price_per_kg and weight_grams:
            if is_on_sale:
                original_price_per_kg = self._calculate_price_per_kg(original_price, weight_grams)
                reduced_price_per_kg = self._calculate_price_per_kg(current_price, weight_grams)
            else:
                original_price_per_kg = self._calculate_price_per_kg(current_price, weight_grams)

        # Extract variant name
        variant_name = self._extract_variant_name(name, brand, size)

        return ScrapedProduct(
            external_id=external_id,
            name=name,
            brand=brand,
            size=size,
            current_price=current_price,
            original_price=original_price,
            is_on_sale=is_on_sale,
            sale_tag=sale_tag,
            url=url,
            site=self.SITE_NAME,
            weight_grams=weight_grams,
            original_price_per_kg=original_price_per_kg,
            reduced_price_per_kg=reduced_price_per_kg,
            base_product_id=handle,
            variant_name=variant_name,
        )

    async def scrape_brand_products(self, brands: list[str], max_price_per_kg: float = None, max_pages: int = None, include_default_brands: bool = True) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape wet cat food from Zoo24 search, filtered by brands and price."""
        all_brands = list(set((self.QUALITY_BRANDS if include_default_brands else []) + (brands or [])))
        brand_set = {self.normalize_brand(b) for b in all_brands}

        scrape_price = max(max_price_per_kg or 0, self.DEFAULT_MAX_PRICE_PER_KG)
        pages_limit = max_pages or self.MAX_SEARCH_PAGES

        seen_ids = set()
        exceeded_price_count = 0

        for page_num in range(1, pages_limit + 1):
            page_url = f"{self.SEARCH_URL}?q=nassfutter+katze&sort_by=price-ascending&page={page_num}"
            logger.info(f"Zoo24: Scraping page {page_num}")

            html = await self._fetch_page_with_js(page_url)
            if not html:
                break

            products = await asyncio.to_thread(self._parse_products_from_html, html)
            if not products:
                break

            chunk = []
            page_has_valid = False
            for p in products:
                # Filter by brand
                if p.brand and self.normalize_brand(p.brand) not in brand_set:
                    continue

                # If no brand detected, skip (search returns many unrelated items)
                if not p.brand:
                    continue

                price_per_kg = p.reduced_price_per_kg or p.original_price_per_kg

                # Early termination: results are sorted by price ascending
                # If price per kg exceeds threshold with headroom, stop
                if price_per_kg and price_per_kg > scrape_price * 1.3:
                    exceeded_price_count += 1
                    if exceeded_price_count >= 10:
                        logger.info(f"Zoo24: Price threshold exceeded consistently, stopping at page {page_num}")
                        if chunk: yield chunk
                        return 
                    continue
                else:
                    exceeded_price_count = 0

                if p.external_id not in seen_ids:
                    seen_ids.add(p.external_id)
                    chunk.append(p)
                    page_has_valid = True
            
            if chunk:
                yield chunk

            if not page_has_valid and page_num > 3:
                # No matching products on this page and past initial pages
                break

    async def scrape_reduced_products(self, brands: list[str] = None) -> AsyncGenerator[list[ScrapedProduct], None]:
        """Scrape reduced/sale products from Zoo24 search."""
        all_brands = list(set(self.QUALITY_BRANDS + (brands or [])))
        brand_set = {self.normalize_brand(b) for b in all_brands}

        seen_ids = set()

        # Scrape first few pages looking for sale items
        for page_num in range(1, min(self.MAX_SEARCH_PAGES, 15) + 1):
            page_url = f"{self.SEARCH_URL}?q=nassfutter+katze&sort_by=price-ascending&page={page_num}"
            logger.info(f"Zoo24: Scraping reduced items, page {page_num}")

            html = await self._fetch_page_with_js(page_url)
            if not html:
                break

            products = await asyncio.to_thread(self._parse_products_from_html, html)
            if not products:
                break

            chunk = []
            for p in products:
                if not p.is_on_sale:
                    continue

                # Filter by brand
                if p.brand and self.normalize_brand(p.brand) not in brand_set:
                    continue
                if not p.brand:
                    continue

                if p.external_id not in seen_ids:
                    seen_ids.add(p.external_id)
                    chunk.append(p)
            
            if chunk:
                yield chunk


async def _scrape_site(scraper, watched_brands: list[str], max_price_per_kg: float = None, include_default_brands: bool = True, callback: Callable[[list[ScrapedProduct]], None] = None) -> None:
    """Scrape a single site and report chunks via callback."""
    site_name = scraper.SITE_NAME
    logger.info(f"Scraping {site_name}...")

    try:
        # Scrape products from watched brands
        async for chunk in scraper.scrape_brand_products(watched_brands, max_price_per_kg=max_price_per_kg, include_default_brands=include_default_brands):
            if callback and chunk:
                await callback(chunk)

        # Also check for reduced items
        async for chunk in scraper.scrape_reduced_products(brands=watched_brands):
             if callback and chunk:
                await callback(chunk)

        logger.info(f"Finished scraping {site_name}")

    except Exception as e:
        logger.error(f"Error scraping {site_name}: {e}")


async def scrape_all_async(watched_brands: list[str] = None, max_price_per_kg: float = None, include_default_brands: bool = True, on_chunk_callback: Callable[[list[ScrapedProduct]], None] = None) -> None:
    """Main async function to scrape products from all sites in parallel.
    Products are reported via on_chunk_callback as they are found.
    """
    try:
        # Launch browser once for all scrapers
        async with async_playwright() as p:
            logger.info("Launching shared browser instance...")
            browser = await p.chromium.launch(headless=True)
            
            # Use the shared browser instance for all scrapers
            scrapers = [
                ZooplusScraper(browser=browser), 
                BitibaScraper(browser=browser), 
                ZooroyalScraper(browser=browser), 
                FressnapfScraper(browser=browser), 
                Zoo24Scraper(browser=browser)
            ]

            # Run all scrapers concurrently
            await asyncio.gather(
                *[_scrape_site(scraper, watched_brands, max_price_per_kg, include_default_brands, on_chunk_callback) for scraper in scrapers],
                return_exceptions=True
            )

            await browser.close()
            
    except Exception as e:
        logger.error(f"Fatal error during scraping: {e}")


# Legacy synchronous wrapper (modified to accumulate results for backward compatibility if needed, though mostly used via async now)
def scrape_all() -> list[ScrapedProduct]:
    """Synchronous wrapper for scrape_all_async."""
    results = []
    async def collector(chunk):
        results.extend(chunk)
    
    asyncio.run(scrape_all_async(on_chunk_callback=collector))
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    products = scrape_all()

    print(f"\nFound {len(products)} products total")

    on_sale = [p for p in products if p.is_on_sale]
    print(f"On sale: {len(on_sale)}")

    print("\nSample products:")
    for p in products[:5]:
        sale_info = f" [SALE: {p.sale_tag or f'{p.original_price}€ -> {p.current_price}€'}]" if p.is_on_sale else ""
        print(f"  {p.brand or 'Unknown'} - {p.name[:50]}")
        print(f"    {p.current_price}€{sale_info}")
        print()
