#!/usr/bin/env python3
"""
Migration script to move brands from comma-separated string to WatchedBrand table.
"""
import logging
from database import get_session, UserPreferences, WatchedBrand, init_db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_brands():
    """Migrate brands from string column to generic table."""
    logger.info("Initializing database (ensures new table exists)...")
    init_db()
    
    session = get_session()
    try:
        users = session.query(UserPreferences).all()
        logger.info(f"Found {len(users)} users to check for migration.")
        
        migrated_count = 0
        total_brands = 0
        
        for user in users:
            # Check if user already has relational brands
            if user.brands:
                logger.info(f"User {user.chat_id} already has relational brands. Skipping.")
                continue
                
            # Check legacy field
            if not user.watched_brands:
                logger.info(f"User {user.chat_id} has no brands to migrate.")
                continue
                
            # Parse legacy brands
            brands_list = [b.strip() for b in user.watched_brands.split(",") if b.strip()]
            if not brands_list:
                continue
                
            logger.info(f"Migrating {len(brands_list)} brands for user {user.chat_id}...")
            
            for brand_name in brands_list:
                # Create relational record
                wb = WatchedBrand(brand_name=brand_name)
                user.brands.append(wb)
                total_brands += 1
            
            migrated_count += 1
            
        session.commit()
        logger.info(f"Migration complete! Migrated {total_brands} brands across {migrated_count} users.")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    migrate_brands()
