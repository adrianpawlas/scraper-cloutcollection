"""
Configuration for Clout Collection Scraper
"""

# Supabase credentials
SUPABASE_URL = "https://yqawmzggcgpeyaaynrjk.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxYXdtemdnY2dwZXlhYXlucmprIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTAxMDkyNiwiZXhwIjoyMDcwNTg2OTI2fQ.XtLpxausFriraFJeX27ZzsdQsFv3uQKXBBggoz6P4D4"

# Site configuration
BASE_URL = "https://cloutcollection.shop"
COLLECTIONS_URL = "https://cloutcollection.shop/collections/view-all"
LANDING_PAGE = "https://cloutcollection.shop"

# Brand information
BRAND_NAME = "Clout Collection"
SOURCE = "scraper-cloutcollection"
SECOND_HAND = False
GENDER = "unisex"  # or NULL - Shopify stores typically sell unisex streetwear

# Embedding model
EMBEDDING_MODEL = "google/siglip-base-patch16-384"
EMBEDDING_DIM = 768

# Scraper settings
HEADLESS = True
TIMEOUT = 60000  # 60 seconds
LOAD_MORE_RETRIES = 10
SCROLL_DELAY = 2  # seconds between scrolls

# Database table
TABLE_NAME = "products"