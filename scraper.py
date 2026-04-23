"""
Scraper for Clout Collection Shopify store
Uses Playwright to handle JavaScript rendering and "Load More" button
"""
import asyncio
import json
import re
import time
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, Browser, Page, BrowserContext


class CloutCollectionScraper:
    """Scraper for Clout Collection Shopify store"""
    
    def __init__(
        self,
        base_url: str = "https://cloutcollection.shop",
        headless: bool = True,
        timeout: int = 60000,
        load_more_retries: int = 10,
        scroll_delay: int = 2
    ):
        self.base_url = base_url
        self.headless = headless
        self.timeout = timeout
        self.load_more_retries = load_more_retries
        self.scroll_delay = scroll_delay
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
        # Track found products
        self.product_handles: list[str] = []
        self.seen_product_handles: set[str] = set()
    
    async def __aenter__(self):
        """Setup browser"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        self.page = await self.context.new_page()
        self.page.set_default_timeout(self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup browser"""
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
    
    async def scrape_all_products(self) -> list[dict]:
        """Scrape all products from the collections page"""
        print(f"Starting scraper for {self.base_url}")
        
        # Navigate to collections page
        collections_url = f"{self.base_url}/collections/view-all"
        print(f"Navigating to {collections_url}")
        
        await self.page.goto(collections_url, wait_until='networkidle')
        
        # Wait for initial products to load
        await self.page.wait_for_selector('.grid-view-item, .product-index, [data-product-handle]', timeout=30000)
        
        # Click "Load More" button until no more products load
        await self._click_load_more()
        
        # Extract all product links
        products = await self._extract_product_links()
        
        print(f"Found {len(products)} products")
        return products
    
    async def _click_load_more(self):
        """Click the Load More button repeatedly until it's gone or no new products load"""
        consecutive_no_new = 0
        last_count = 0
        
        for attempt in range(self.load_more_retries):
            # Try to find and click Load More button
            load_more_button = await self.page.query_selector('#gsloadmore, button:has-text("Load More")')
            
            if not load_more_button:
                print(f"No more 'Load More' button found after {attempt} attempts")
                break
            
            # Check if button is visible and enabled
            is_visible = await load_more_button.is_visible()
            is_disabled = await load_more_button.is_disabled()
            
            if not is_visible or is_disabled:
                print("Load More button is hidden or disabled")
                break
            
            print(f"Clicking Load More button (attempt {attempt + 1})...")
            
            # Click the button
            try:
                await load_more_button.click()
            except Exception as e:
                print(f"Error clicking button: {e}")
                break
            
            # Wait for new products to load
            await self.page.wait_for_timeout(self.scroll_delay * 1000)
            
            # Check for new products
            current_count = await self._count_products_on_page()
            
            if current_count > last_count:
                print(f"  New products loaded: {current_count} (was {last_count})")
                last_count = current_count
                consecutive_no_new = 0
            else:
                consecutive_no_new += 1
                print(f"  No new products ({current_count} total), consecutive: {consecutive_no_new}")
            
            # If no new products after multiple attempts, stop
            if consecutive_no_new >= 3:
                print("No new products after 3 consecutive attempts, stopping")
                break
        
        print(f"Finished loading. Total products: {last_count}")
    
    async def _count_products_on_page(self) -> int:
        """Count products currently visible on the page"""
        # Try multiple selectors used by Shopify themes
        selectors = [
            '.grid-view-item',
            '.product-index',
            '[data-product-handle]',
            '.grid-view-item a[href*="/products/"]',
            '.product-card a[href*="/products/"]',
            'a[href*="/products/"][data-product-id]'
        ]
        
        total = 0
        for selector in selectors:
            elements = await self.page.query_selector_all(selector)
            total += len(elements)
        
        return total
    
    async def _extract_product_links(self) -> list[dict]:
        """Extract all product URLs and handles from the collection page"""
        products = []
        
        # Product links on collection page typically have /products/ in the href
        product_links = await self.page.query_selector_all('a[href*="/products/"]')
        
        seen_urls = set()
        
        for link in product_links:
            try:
                href = await link.get_attribute('href')
                if not href:
                    continue
                
                # Make absolute URL
                full_url = urljoin(self.base_url, href)
                
                # Parse the product handle from URL
                parsed = urlparse(full_url)
                path_parts = parsed.path.strip('/').split('/')
                
                if 'products' in path_parts:
                    handle = path_parts[-1]  # Last part is the product handle
                    
                    if handle not in seen_urls:
                        seen_urls.add(handle)
                        products.append({
                            'handle': handle,
                            'product_url': full_url
                        })
            except Exception as e:
                print(f"Error extracting product link: {e}")
                continue
        
        # Deduplicate by handle
        unique_products = []
        seen_handles = set()
        
        for product in products:
            if product['handle'] not in seen_handles:
                seen_handles.add(product['handle'])
                unique_products.append(product)
        
        return unique_products
    
    async def scrape_product_details(self, product_url: str) -> Optional[dict]:
        """Scrape detailed information from a single product page"""
        try:
            await self.page.goto(product_url, wait_until='networkidle')
            
            # Wait for product info to load
            await self.page.wait_for_selector('[data-product-id], [data-product-handle]', timeout=30000)
            
            # Extract product data from page
            product_data = await self._extract_product_data()
            
            return product_data
            
        except Exception as e:
            print(f"Error scraping {product_url}: {e}")
            return None
    
    async def _extract_product_data(self) -> dict:
        """Extract all product data from current page"""
        data = {}
        
        # Extract from window.KiwiSizing.data if available
        kiwisizing_data = await self.page.evaluate('''
            () => {
                if (window.KiwiSizing && window.KiwiSizing.data) {
                    return window.KiwiSizing.data;
                }
                return null;
            }
        ''')
        
        if kiwisizing_data:
            data['kiwisizing'] = kiwisizing_data
        
        # Extract from Shopify product JSON
        product_json = await self.page.evaluate('''
            () => {
                const productEl = document.querySelector('[data-product-id]');
                if (productEl && productEl.dataset) {
                    return productEl.dataset;
                }
                return null;
            }
        ''')
        
        if product_json:
            data['product_json'] = product_json
        
        # Extract page title
        title = await self.page.title()
        data['page_title'] = title
        
        # Extract meta description
        meta_desc = await self.page.query_selector('meta[name="description"]')
        if meta_desc:
            data['description'] = await meta_desc.get_attribute('content')
        
        # Extract JSON-LD structured data
        json_ld = await self.page.evaluate('''
            () => {
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                const data = [];
                scripts.forEach(script => {
                    try {
                        data.push(JSON.parse(script.textContent));
                    } catch (e) {}
                });
                return data;
            }
        ''')
        
        if json_ld:
            data['json_ld'] = json_ld
        
        return data
    
    async def parse_product_page(self, product_url: str, html: str) -> dict:
        """Parse product data from harvested HTML (for batch processing)"""
        data = {
            'product_url': product_url,
            'handle': '',
            'title': '',
            'description': '',
            'brand': 'Clout Collection',
            'price': '',
            'sale': None,
            'image_url': '',
            'additional_images': [],
            'sizes': [],
            'colors': [],
            'category': '',
            'metadata': {}
        }
        
        # Extract handle from URL
        parsed = urlparse(product_url)
        path_parts = parsed.path.strip('/').split('/')
        if 'products' in path_parts:
            data['handle'] = path_parts[-1]
        
        # Extract product JSON from window.KiwiSizing.data
        kiwisizing_match = re.search(
            r'KiwiSizing\.data\s*=\s*(\{.*?\});',
            html,
            re.DOTALL
        )
        
        if kiwisizing_match:
            try:
                kiwisizing = json.loads(kiwisizing_match.group(1))
                
                data['title'] = kiwisizing.get('title', '')
                data['category'] = kiwisizing.get('type', '')
                
                # Extract images
                images = kiwisizing.get('images', [])
                if images:
                    data['image_url'] = self._normalize_image_url(images[0])
                    data['additional_images'] = [
                        self._normalize_image_url(img) for img in images[1:]
                    ]
                
                # Extract variants and prices
                variants = kiwisizing.get('variants', [])
                if variants:
                    prices = []
                    for variant in variants:
                        price = variant.get('price', 0)
                        compare_at = variant.get('compare_at_price')
                        
                        if compare_at and compare_at > price:
                            if not data.get('sale'):
                                data['sale'] = price / 100  # Convert cents
                            prices.append(compare_at / 100)
                        else:
                            prices.append(price / 100)
                    
                    if prices:
                        data['price'] = str(prices[0])
                
                # Extract options (sizes, colors)
                options = kiwisizing.get('options', [])
                for option in options:
                    name = option.get('name', '')
                    values = option.get('values', [])
                    
                    if name.lower() == 'color':
                        data['colors'] = values
                    elif name.lower() == 'size':
                        data['sizes'] = values
                
                data['metadata'] = {
                    'vendor': kiwisizing.get('vendor', ''),
                    'tags': kiwisizing.get('tags', []),
                    'product_id': kiwisizing.get('product', ''),
                    'collections': kiwisizing.get('collections', '')
                }
                
            except json.JSONDecodeError as e:
                print(f"Error parsing KiwiSizing data: {e}")
        
        # Extract JSON-LD script
        json_ld_match = re.search(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html,
            re.DOTALL
        )
        
        if json_ld_match:
            try:
                json_ld_content = json.loads(json_ld_match.group(1))
                
                # Find the Product type
                if isinstance(json_ld_content, list):
                    for item in json_ld_content:
                        if item.get('@type') == 'Product':
                            json_ld_content = item
                            break
                
                if isinstance(json_ld_content, dict):
                    data['description'] = json_ld_content.get('description', '')
                    
                    # Extract price from offer
                    offer = json_ld_content.get('offers', {})
                    if offer:
                        price = offer.get('price', 0)
                        currency = offer.get('priceCurrency', 'USD')
                        data['price'] = f"{price} {currency}"
                        
                        # Check for sale price
                        compare_at = offer.get('priceSpecification', [{}])[0].get('price') if offer.get('priceSpecification') else None
                        if compare_at and float(compare_at) > float(price):
                            data['sale'] = str(price)
                            data['price'] = str(compare_at)
                    
                    # Extract image from JSON-LD if not from KiwiSizing
                    if not data['image_url'] and json_ld_content.get('image'):
                        image = json_ld_content['image']
                        if isinstance(image, list):
                            image = image[0]
                        data['image_url'] = self._normalize_image_url(image)
                        
            except (json.JSONDecodeError, TypeError) as e:
                print(f"Error parsing JSON-LD: {e}")
        
        return data
    
    def _normalize_image_url(self, url: str) -> str:
        """Normalize Shopify image URL"""
        if not url:
            return ''
        
        # Remove protocol if relative URL
        if url.startswith('//'):
            return 'https:' + url
        elif url.startswith('/'):
            return self.base_url + url
        
        return url


async def scrape_all_products_batch(
    base_url: str = "https://cloutcollection.shop",
    headless: bool = True,
    max_products: Optional[int] = None
) -> list[dict]:
    """Scrape all products - batch version for efficiency"""
    products = []
    
    async with CloutCollectionScraper(base_url, headless) as scraper:
        # First collect all product links
        product_links = await scraper.scrape_all_products()
        
        if max_products:
            product_links = product_links[:max_products]
        
        print(f"Scraping details for {len(product_links)} products...")
        
        # Then get details for each product
        for i, product_info in enumerate(product_links):
            print(f"  [{i+1}/{len(product_links)}] {product_info['handle']}")
            
            product_data = await scraper.scrape_product_details(product_info['product_url'])
            
            if product_data:
                product_info.update(product_data)
                products.append(product_info)
            
            # Small delay to be nice to server
            await asyncio.sleep(0.5)
    
    return products