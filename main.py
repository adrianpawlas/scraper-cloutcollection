"""
Main pipeline for Clout Collection scraper
Complete workflow: scrape -> embed -> import to Supabase
"""
import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlparse


from config import (
    SUPABASE_URL,
    SUPABASE_ANON_KEY,
    BASE_URL,
    COLLECTIONS_URL,
    BRAND_NAME,
    SOURCE,
    SECOND_HAND,
    GENDER,
    EMBEDDING_MODEL,
    EMBEDDING_DIM,
    HEADLESS,
    TABLE_NAME,
)
from database import create_supabase_client
from scraper import CloutCollectionScraper
from embeddings import SigLIPEmbedder


class CloutCollectionPipeline:
    """Complete pipeline for scraping Clout Collection"""
    
    def __init__(self, max_products: Optional[int] = None, test_mode: bool = False, dry_run: bool = False):
        self.max_products = max_products
        self.test_mode = test_mode
        self.dry_run = dry_run
        
        # Track seen URLs for stale detection
        self.seen_urls = self._load_seen_urls()
        
        # Initialize components
        self.db = create_supabase_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        self.embedder = SigLIPEmbedder(EMBEDDING_MODEL)
        
        # Track progress
        self.products_scraped = 0
        self.products_embedded = 0
        self.products_imported = 0
        self.errors = []
    
    async def run(self):
        """Run the complete pipeline"""
        print("=" * 60)
        print("CLOUT COLLECTION SCRAPER PIPELINE")
        print("=" * 60)
        print(f"Test mode: {self.test_mode}")
        print(f"Max products: {self.max_products or 'all'}")
        print()
        
        # Step 1: Scrape all product URLs
        print("[1/4] Scraping product URLs...")
        product_links = await self._scrape_product_urls()
        print(f"  Found {len(product_links)} products")
        print()
        
        # Step 2: Scrape each product details and HTML
        print("[2/4] Scraping product details...")
        products_data = await self._scrape_product_details(product_links)
        print(f"  Scraped details for {len(products_data)} products")
        print()
        
        # Step 3: Generate embeddings
        print("[3/4] Generating embeddings...")
        products_with_embeddings = await self._generate_embeddings(products_data)
        print(f"  Generated embeddings for {len(products_with_embeddings)} products")
        print()
        
        # Step 4: Import to Supabase
        print("[4/4] Importing to Supabase...")
        imported = await self._import_to_supabase(products_with_embeddings)
        print(f"  Imported {imported} products")
        
        # Summary
        print()
        print("=" * 60)
        print("PIPELINE COMPLETE")
        print(f"  Products scraped: {self.products_scraped}")
        print(f"  Products embedded: {self.products_embedded}")
        print(f"  Products imported: {self.products_imported}")
        print(f"  Errors: {len(self.errors)}")
        print("=" * 60)
        
        return {
            'scraped': self.products_scraped,
            'embedded': self.products_embedded,
            'imported': self.products_imported,
            'errors': self.errors
        }
    
    async def _scrape_product_urls(self) -> list[dict]:
        """Scrape all product URLs from collection page"""
        product_links = []
        
        async with CloutCollectionScraper(BASE_URL, HEADLESS) as scraper:
            # Navigate to collections page and click Load More until done
            await scraper.page.goto(COLLECTIONS_URL, wait_until='domcontentloaded', timeout=90000)
            await scraper.page.wait_for_selector(
                '.grid-view-item, .product-index, [data-product-handle]',
                timeout=30000
            )
            
            # Click Load More until all products loaded
            await scraper._click_load_more()
            
            # Extract product links
            product_links = await scraper._extract_product_links()
        
        # Optionally limit
        if self.max_products:
            product_links = product_links[:self.max_products]
        
        return product_links
    
    async def _scrape_product_details(
        self,
        product_links: list[dict]
    ) -> list[dict]:
        """Scrape detailed info for each product"""
        products_data = []
        
        async with CloutCollectionScraper(BASE_URL, HEADLESS) as scraper:
            for i, product_info in enumerate(product_links):
                print(f"  [{i+1}/{len(product_links)}] {product_info['handle']}")
                
                try:
                    # Get the product page HTML
                    await scraper.page.goto(
                        product_info['product_url'],
                        wait_until='domcontentloaded',
                        timeout=90000
                    )
                    
                    await scraper.page.wait_for_selector(
                        '[data-product-id], [data-product-handle]',
                        timeout=30000
                    )
                    
                    # Get page content
                    html = await scraper.page.content()
                    
                    # Parse the product data
                    product_data = self._parse_product_html(
                        product_info['product_url'],
                        html
                    )
                    
                    products_data.append(product_data)
                    self.products_scraped += 1
                    
                except Exception as e:
                    error_msg = f"Error scraping {product_info['handle']}: {str(e)}"
                    print(f"    ERROR: {error_msg}")
                    self.errors.append(error_msg)
                
                # Small delay to be nice to server
                await asyncio.sleep(0.3)
        
        return products_data
    
    def _parse_product_html(self, product_url: str, html: str) -> dict:
        """Parse product data from HTML"""
        data = {
            'product_url': product_url,
            'handle': '',
            'title': '',
            'description': '',
            'brand': BRAND_NAME,
            'price': '',
            'sale': None,
            'image_url': '',
            'additional_images': [],
            'sizes': [],
            'colors': [],
            'tags': [],
            'category': '',
            'metadata': {},
            'gender': GENDER,
            'source': SOURCE,
            'second_hand': SECOND_HAND,
        }
        
        # Extract handle from URL
        parsed = urlparse(product_url)
        path_parts = parsed.path.strip('/').split('/')
        if 'products' in path_parts:
            data['handle'] = path_parts[-1]
        
        # First try: Parse JSON-LD structured data (always valid JSON)
        json_ld_data = self._extract_json_ld_product(html)
        
        if json_ld_data:
            data['title'] = json_ld_data.get('name', '')
            data['description'] = json_ld_data.get('description', '')
            
            # Get price from offer
            offer = json_ld_data.get('offers', {})
            if offer:
                price = offer.get('price', 0)
                currency = offer.get('priceCurrency', 'USD')
                if price:
                    data['price'] = f"{price} {currency}"
            
            # Get images
            images = json_ld_data.get('image', [])
            if isinstance(images, list) and images:
                data['image_url'] = self._normalize_image_url(images[0])
                data['additional_images'] = [
                    self._normalize_image_url(img) for img in images[1:6]
                ]
        
        # Also try to parse KiwiSizing for more detailed data
        kiwisizing_match = re.search(
            r'KiwiSizing\.data\s*=\s*(\{.*?\});',
            html,
            re.DOTALL
        )
        
        if kiwisizing_match:
            try:
                raw_json = kiwisizing_match.group(1)
                
                # Fix JavaScript object to valid JSON
                kiwisizing = self._parse_js_object(raw_json)
                
                # Basic info - override if not set from JSON-LD
                if not data['title'] and kiwisizing.get('title'):
                    data['title'] = kiwisizing.get('title')
                if not data['description'] and kiwisizing.get('description'):
                    data['description'] = kiwisizing.get('description')
                
                # Category/type
                if kiwisizing.get('type'):
                    raw_type = kiwisizing.get('type')
                    categories = [c.strip() for c in re.split(r'[,/&]', raw_type) if c.strip()]
                    data['category'] = ', '.join(categories)
                
                # Images - override if better data in KiwiSizing
                if kiwisizing.get('images'):
                    images = kiwisizing.get('images', [])
                    data['image_url'] = self._normalize_image_url(images[0])
                    data['additional_images'] = [
                        self._normalize_image_url(img)
                        for img in images[1:6]
                    ]
                
                # Variants and prices
                variants = kiwisizing.get('variants', [])
                prices_list = []
                for variant in variants:
                    price = variant.get('price', 0)
                    usd_price = price / 100 if price else 0
                    
                    compare_at = variant.get('compare_at_price')
                    if compare_at and compare_at > price:
                        data['sale'] = str(usd_price)
                        original = compare_at / 100
                        prices_list.append(f"{original:.2f}USD")
                    else:
                        prices_list.append(f"{usd_price:.2f}USD")
                
                if prices_list and not data['price']:
                    data['price'] = prices_list[0]
                
                # Options
                options = kiwisizing.get('options', [])
                for option in options:
                    name = option.get('name', '')
                    values = option.get('values', [])
                    if name.lower() == 'color':
                        data['colors'] = values
                    elif name.lower() == 'size':
                        data['sizes'] = values
                
                data['tags'] = kiwisizing.get('tags', [])
                
                data['metadata'] = {
                    'product_id': str(kiwisizing.get('product', '')),
                    'vendor': kiwisizing.get('vendor', ''),
                    'tags': data['tags'],
                }
                
            except Exception as e:
                print(f"    Warning: KiwiSizing parse error: {e}")
        
        # Extract from JSON-LD for description if not already
        if not data['description']:
            json_ld_match = re.search(
                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                html,
                re.DOTALL
            )
            
            if json_ld_match:
                try:
                    json_ld = json.loads(json_ld_match.group(1))
                    if isinstance(json_ld, list):
                        for item in json_ld:
                            if item.get('@type') == 'Product':
                                json_ld = item
                                break
                    
                    if isinstance(json_ld, dict):
                        data['description'] = json_ld.get('description', '')
                        
                        # Get price from offer
                        offer = json_ld.get('offers', {})
                        if offer:
                            price = offer.get('price', 0)
                            currency = offer.get('priceCurrency', 'USD')
                            if price and not data['price']:
                                data['price'] = f"{price} {currency}"
                        
                        # Try to get images from JSON-LD if not set
                        if not data.get('image_url'):
                            image = json_ld.get('image')
                            if image:
                                if isinstance(image, list):
                                    image = image[0]
                                if isinstance(image, str):
                                    data['image_url'] = self._normalize_image_url(image)
                                    
                except (json.JSONDecodeError, TypeError):
                    pass
        
        # Extract meta description
        if not data['description']:
            meta_desc_match = re.search(
                r'<meta[^>]*name="description"[^>]*content="([^"]*)"',
                html
            )
            if meta_desc_match:
                data['description'] = meta_desc_match.group(1)
        
        # Fallback: Try to extract any product image from OG tags
        if not data.get('image_url'):
            og_image_match = re.search(
                r'<meta[^>]*property="og:image"[^>]*content="([^"]*)"',
                html
            )
            if og_image_match:
                data['image_url'] = og_image_match.group(1)
        
        return data
    
    async def _generate_embeddings(self, products_data: list[dict]) -> list[dict]:
        """Generate embeddings only for changed products"""
        # Load existing products first
        existing_products = self._load_existing_products()
        existing_by_url = {p['product_url']: p for p in existing_products}
        
        for i, product in enumerate(products_data):
            product_url = product['product_url']
            self.seen_urls.add(product_url)  # Track for stale detection
            
            existing = existing_by_url.get(product_url)
            
            # Check if we need new embeddings
            needs_embedding = True
            if existing:
                # Has existing record - check if image changed
                if existing.get('image_url') == product.get('image_url'):
                    # Same image - copy existing embeddings
                    product['image_embedding'] = existing.get('image_embedding')
                    product['info_embedding'] = existing.get('info_embedding')
                    needs_embedding = False
                    print(f"  [{i+1}/{len(products_data)}] {product['handle']} - skipped (unchanged)")
            
            if needs_embedding:
                print(f"  [{i+1}/{len(products_data)}] {product['handle']}")
                
                try:
                    # Generate image embedding
                    if product.get('image_url'):
                        image_emb = self.embedder.embed_image(product['image_url'])
                        product['image_embedding'] = self.embedder.to_numpy(image_emb)
                        self.products_embedded += 1
                    
                    # Generate info embedding (text)
                    info_text = self._build_info_text(product)
                    if info_text:
                        text_emb = self.embedder.embed_text(info_text)
                        product['info_embedding'] = self.embedder.to_numpy(text_emb)
                    
                    print(f"    ✓ Embeddings generated")
                    
                except Exception as e:
                    error_msg = f"Error embedding {product['handle']}: {str(e)}"
                    print(f"    ✗ Error: {error_msg}")
                    self.errors.append(error_msg)
            
            # Staggered embedding generation - 0.5s delay between API calls
            await asyncio.sleep(0.5)
        
        return products_data
    
    def _parse_js_object(self, js_str: str) -> dict:
        """Parse JavaScript object string to Python dict"""
        import re
        result = {}
        
        try:
            # Direct JSON parse attempt first
            return json.loads(js_str)
        except json.JSONDecodeError:
            pass
        
        # Manual parsing for JS objects
        # Handle string keys and values
        # Try extracting the JSON from script tag more carefully
        # First try: find product data in JSON-LD which is always valid JSON
        return {}
    
    def _extract_json_ld_product(self, html: str) -> dict:
        """Extract Product data from JSON-LD structured data in HTML"""
        import re
        
        # Find JSON-LD script blocks
        json_ld_matches = re.findall(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html,
            re.DOTALL
        )
        
        for match in json_ld_matches:
            try:
                data = json.loads(match)
                
                # Find the Product type in the graph/array
                if isinstance(data, list):
                    for item in data:
                        if item.get('@type') == 'Product':
                            return item
                elif isinstance(data, dict):
                    # Check if this is Product or in @graph
                    if data.get('@type') == 'Product':
                        return data
                    if data.get('@graph'):
                        for item in data['@graph']:
                            if item.get('@type') == 'Product':
                                return item
            except (json.JSONDecodeError, TypeError):
                continue
        
        return {}
    
    def _parse_javascript_object(self, js_str: str) -> dict:
        """Parse JavaScript-like object string more robustly"""
        import re
        result = {}
        
        try:
            # First: try direct JSON parsing
            return json.loads(js_str)
        except json.JSONDecodeError:
            # Try extracting JSON from HTML script tag
            pass
        
        # Try extracting each field with regex
        patterns = {
            'title': r"title:\s*'([^']*)'",
            'type': r"type:\s*'([^']*)'",
            'vendor': r"vendor:\s*'([^']*)'",
            'product': r"product:\s*'(\d+)'",
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, js_str)
            if match:
                result[key] = match.group(1)
        
        # Try to extract images array
        images_match = re.search(r'images:\s*\[(.*?)\]', js_str, re.DOTALL)
        if images_match:
            images_str = images_match.group(1)
            image_urls = re.findall(r'"([^"]+)"', images_str)
            if image_urls:
                result['images'] = image_urls
        
        # Try to extract variants array  
        variants_match = re.search(r'variants:\s*\[(.*?)\]', js_str, re.DOTALL)
        if variants_match:
            variants_str = variants_match.group(1)
            # Extract minimal variant info
            variant_ids = re.findall(r'id:\s*(\d+)', variants_str)
            if variant_ids:
                result['variants'] = [{'id': v} for v in variant_ids[:10]]
        
        # Try to extract options
        options_match = re.search(r'options:\s*\[(.*?)\]', js_str, re.DOTALL)
        if options_match:
            options = []
            for name_match in re.finditer(r'name:\s*"([^"]+)"', options_match.group(1)):
                name = name_match.group(1)
                # Find corresponding values
                start = name_match.end()
                values_match = re.search(r'values:\s*\[(.*?)\]', options_str[start:start+200])
                if values_match:
                    values = re.findall(r'"([^"]+)"', values_match.group(1))
                    options.append({'name': name, 'values': values})
            result['options'] = options
        
        return result

    def _build_info_text(self, product: dict) -> str:
        """Build combined text for info embedding"""
        parts = [
            product.get('title', ''),
            product.get('description', ''),
            product.get('category', ''),
            product.get('gender', ''),
            f"Brand: {product.get('brand', '')}",
            f"Price: {product.get('price', '')}",
            f"Colors: {', '.join(product.get('colors', []))}",
            f"Sizes: {', '.join(product.get('sizes', []))}",
        ]
        
        # Add metadata info
        metadata = product.get('metadata', {})
        if metadata:
            parts.append(f"Tags: {', '.join(metadata.get('tags', []))}")
        
        # Filter empty and join
        text = ' | '.join([p for p in parts if p])
        
        # Truncate if too long (model max is 64 tokens for this model)
        # Just take first ~500 chars to be safe
        return text[:500]
    
    async def _import_to_supabase(self, products: list[dict]) -> int:
        """Import products to Supabase with smart batching and upsert logic"""
        if self.dry_run:
            print("  [DRY RUN] Skipping database import")
            return 0
        
        # Get existing products for comparison
        print("  Loading existing products from database...")
        existing_products = self._load_existing_products()
        existing_by_url = {p['product_url']: p for p in existing_products}
        
        new_count = 0
        updated_count = 0
        unchanged_count = 0
        
        # Separate into categories: new, needs_update, skip
        products_to_insert = []
        
        for product in products:
            product_url = product['product_url']
            existing = existing_by_url.get(product_url)
            
            if existing is None:
                # New product
                new_count += 1
                products_to_insert.append(product)
            else:
                # Check if anything changed
                is_changed = self._is_product_changed(existing, product)
                
                if is_changed:
                    # Product changed - needs update with new embeddings
                    updated_count += 1
                    products_to_insert.append(product)
                else:
                    # Unchanged - skip
                    unchanged_count += 1
                    # Still track that we saw it
                    self.seen_urls.add(product_url)
        
        # Batch insert updated/new products
        print(f"  New: {new_count}, Updated: {updated_count}, Unchanged: {unchanged_count}")
        
        if products_to_insert:
            await self._batch_insert(products_to_insert)
        
        self.products_imported = new_count + updated_count
        
        # Find and delete stale products
        stale_count = await self._remove_stale_products()
        
        # Print summary
        print()
        print("=" * 60)
        print("RUN SUMMARY")
        print(f"  New products added: {new_count}")
        print(f"  Products updated: {updated_count}")
        print(f"  Products unchanged (skipped): {unchanged_count}")
        print(f"  Stale products deleted: {stale_count}")
        print("=" * 60)
        
        return self.products_imported
    
    def _load_existing_products(self) -> list[dict]:
        """Load all existing products for this source"""
        all_products = []
        page = 0
        page_size = 1000
        
        while True:
            products = self.db.select(
                TABLE_NAME,
                filters={'source': SOURCE},
                columns='id,product_url,title,description,category,price,image_url,additional_images,metadata,updated_at',
                limit=page_size,
                offset=page * page_size
            )
            if not products:
                break
            all_products.extend(products)
            page += 1
            if len(products) < page_size:
                break
        
        return all_products
    
    def _is_product_changed(self, existing: dict, new_product: dict) -> bool:
        """Check if product data has changed"""
        # Check key fields that matter for upsert
        if existing.get('title') != new_product.get('title'):
            return True
        if existing.get('description') != new_product.get('description'):
            return True
        if existing.get('category') != new_product.get('category'):
            return True
        if existing.get('price') != new_product.get('price'):
            return True
        if existing.get('image_url') != new_product.get('image_url'):
            return True
        if existing.get('sale') != new_product.get('sale'):
            return True
        
        # Check additional images
        existing_additional = existing.get('additional_images', '') or ''
        new_additional = new_product.get('additional_images', [])
        if isinstance(new_additional, list):
            new_additional = ' , '.join(new_additional)
        if existing_additional != new_additional:
            return True
        
        return False
    
    def _should_regenerate_embeddings(self, existing: dict, product: dict) -> bool:
        """Check if embeddings need to be regenerated"""
        # Always regenerate if there are no embeddings
        if not existing.get('image_embedding') and product.get('image_embedding'):
            return True
        if not existing.get('info_embedding') and product.get('info_embedding'):
            return True
        
        # Regenerate if image URL changed
        if existing.get('image_url') != product.get('image_url'):
            return True
        
        return False
    
    async def _batch_insert(self, products: list[dict]) -> int:
        """Insert products in batches with retry logic"""
        batch_size = 50
        total_inserted = 0
        
        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(products) + batch_size - 1) // batch_size
            
            print(f"  Inserting batch {batch_num}/{total_batches} ({len(batch)} products)...")
            
            for retry in range(3):  # 3 retries
                try:
                    self._insert_batch(batch)
                    total_inserted += len(batch)
                    break
                except Exception as e:
                    if retry < 2:
                        print(f"    Retry {retry + 1} after error: {e}")
                        await asyncio.sleep(2)
                    else:
                        # Log failed batch to file
                        self._log_failed_batch(batch, str(e))
                        print(f"    ✗ Batch failed after 3 retries")
        
        return total_inserted
    
    def _insert_batch(self, products: list[dict]) -> None:
        """Insert a batch of products"""
        records = []
        
        for product in products:
            record = self._prepare_record(product)
            records.append(record)
        
        # Use bulk insert
        for record in records:
            self.db.insert(TABLE_NAME, record)
        
        # Add small delay between API calls
        time.sleep(0.1)
    
    def _log_failed_batch(self, products: list[dict], error: str) -> None:
        """Log failed batch to file"""
        import os
        from datetime import datetime
        
        log_dir = os.path.dirname(os.path.abspath(__file__))
        log_file = os.path.join(log_dir, 'failed_imports.log')
        
        timestamp = datetime.now().isoformat()
        
        with open(log_file, 'a') as f:
            f.write(f"\n--- {timestamp} ---\n")
            f.write(f"Error: {error}\n")
            f.write(f"Products: {len(products)}\n")
            for p in products:
                f.write(f"  - {p.get('handle')}: {p.get('product_url')}\n")
    
    async def _remove_stale_products(self) -> int:
        """Remove products not seen in this run"""
        # Track consecutive misses
        seen_this_run = self.seen_urls
        
        # Load last run's seen URLs
        last_run_urls = self._load_seen_urls()
        
        # Products that were in last run but not in this run
        was_in_last_run = set(last_run_urls)
        stale_urls = was_in_last_run - seen_this_run
        
        # Count how many times we've seen these as stale
        stale_file = self._get_stale_tracker()
        stale_count = 0
        
        for url in stale_urls:
            stale_file[url] = stale_file.get(url, 0) + 1
            
            # Delete if missed 2+ times
            if stale_file[url] >= 2:
                # Find and delete product
                all_products = self.db.select(
                    TABLE_NAME,
                    filters={'product_url': url, 'source': SOURCE},
                    columns='id',
                    limit=1
                )
                if all_products:
                    self.db.delete(TABLE_NAME, {'id': all_products[0].get('id')})
                    stale_count += 1
        
        # Save updated stale tracker
        self._save_stale_tracker(stale_file)
        
        return stale_count
    
    def _get_stale_tracker(self) -> dict:
        """Get stale tracking data"""
        import os
        import json
        
        file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'stale_tracker.json'
        )
        
        if not os.path.exists(file_path):
            return {}
        
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    
    def _save_stale_tracker(self, tracker: dict) -> None:
        """Save stale tracking data"""
        import os
        import json
        
        file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'stale_tracker.json'
        )
        
        # Also update seen URLs
        self._save_seen_urls()
        
        # Save tracker
        with open(file_path, 'w') as f:
            json.dump(tracker, f)
    
    def _save_seen_urls(self) -> None:
        """Save seen URLs to file for stale detection"""
        import os
        import json
        
        file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'seen_products.json'
        )
        
        data = {
            'urls': list(self.seen_urls),
            'last_run': datetime.now().isoformat()
        }
        
        with open(file_path, 'w') as f:
            json.dump(data, f)
    
    def _load_seen_urls(self) -> set:
        """Load previously seen URLs"""
        import os
        import json
        
        file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'seen_products.json'
        )
        
        if not os.path.exists(file_path):
            return set()
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                return set(data.get('urls', []))
        except Exception:
            return set()
    
    def _prepare_record(self, product: dict) -> dict:
        """Prepare product record for database"""
        # Generate ID from handle
        product_id = f"{SOURCE}_{product['handle']}"
        
        # Format additional_images
        additional = product.get('additional_images', [])
        if additional:
            additional_images_str = ' , '.join(additional)
        else:
            additional_images_str = None
        
        # Format tags
        tags = product.get('tags', [])
        
        # Build metadata JSON
        metadata = product.get('metadata', {})
        metadata['scraped_at'] = datetime.now().isoformat()
        metadata['description'] = product.get('description', '')
        
        record = {
            'id': product_id,
            'source': SOURCE,
            'product_url': product['product_url'],
            'brand': BRAND_NAME,
            'title': product.get('title', ''),
            'description': product.get('description', ''),
            'category': product.get('category', ''),
            'gender': product.get('gender'),
            'image_url': product.get('image_url', ''),
            'additional_images': additional_images_str,
            'price': product.get('price', ''),
            'sale': product.get('sale'),
            'second_hand': SECOND_HAND,
            'tags': tags,
            'metadata': json.dumps(metadata),
            'image_embedding': product.get('image_embedding'),
            'info_embedding': product.get('info_embedding'),
            'created_at': datetime.now().isoformat(),
        }
        
        return record
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize Shopify image URL"""
        if not url:
            return ''
        
        if url.startswith('//'):
            return 'https:' + url
        elif url.startswith('/'):
            return BASE_URL + url
        
        return url


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Clout Collection Scraper')
    parser.add_argument(
        '--max-products',
        type=int,
        default=None,
        help='Maximum products to scrape (default: all)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in test mode with 5 products'
    )
    
    args = parser.parse_args()
    
    # Create pipeline
    max_products = args.max_products
    if args.test:
        max_products = 5
    
    pipeline = CloutCollectionPipeline(
        max_products=max_products,
        test_mode=args.test
    )
    
    # Run
    result = await pipeline.run()
    
    # Exit with error code if any errors
    if result['errors']:
        print(f"\nWarning: {len(result['errors'])} errors occurred")
        # Don't exit with error, just warn
    
    return result


if __name__ == '__main__':
    asyncio.run(main())