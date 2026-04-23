"""
CLI runner for scraper - can be used manually or by automation
"""
import asyncio
import sys
from main import CloutCollectionPipeline


def main():
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
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run - scrape but do not import to database'
    )
    
    args = parser.parse_args()
    
    max_products = args.max_products
    test_mode = args.test
    if args.test:
        max_products = 5
    
    pipeline = CloutCollectionPipeline(
        max_products=max_products,
        test_mode=test_mode,
        dry_run=args.dry_run
    )
    
    result = asyncio.run(pipeline.run())
    
    # Exit with error if no products scraped
    if result['scraped'] == 0:
        print("ERROR: No products scraped")
        sys.exit(1)
    
    # Exit with warning if errors
    if result['errors']:
        print(f"WARNING: {len(result['errors'])} errors occurred")
        
    print(f"\n✓ Successfully scraped {result['scraped']} products")
    print(f"✓ Embedded {result['embedded']} products")
    print(f"✓ Imported {result['imported']} products to Supabase")
    
    sys.exit(0)


if __name__ == '__main__':
    main()