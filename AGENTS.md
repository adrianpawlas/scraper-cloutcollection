# Clout Collection Scraper

Scraper for Fashion Store Clout Collection - scrapes products, generates SigLIP embeddings (768-dim), and imports to Supabase.

## Project Structure

```
scraper-cloutcollection/
├── config.py           # Configuration (Supabase credentials, site settings)
├── database.py         # Supabase database integration
├── scraper.py         # Playwright scraper for Shopify
├── embeddings.py     # SigLIP (google/siglip-base-patch16-384) embeddings
├── main.py           # Main pipeline
└── requirements.txt  # Python dependencies
```

## Installation

```bash
pip install -r requirements.txt
playwright install chromium
```

## Usage

### Full scrape (all products)
```bash
python main.py
```

### Test mode (5 products)
```bash
python main.py --test
```

### Limit products
```bash
python main.py --max-products 50
```

## Pipeline Steps

1. **Scrape URLs** - Navigate to collection page, click "Load More" until all products loaded
2. **Scrape Details** - Visit each product page, extract: title, description, price, images, sizes, colors, category
3. **Generate Embeddings** - Create both image and text embeddings using SigLIP (768-dim)
4. **Import to Supabase** - Insert/update records in `products` table with unique constraint on (source, product_url)

## Output Fields

| Field | Description |
|-------|-------------|
| id | Unique ID: `scraper-cloutcollection_{handle}` |
| source | `scraper-cloutcollection` |
| product_url | Full product URL |
| brand | `Clout Collection` |
| title | Product name |
| description | Product description |
| category | Category (e.g., "T-shirt, Sweaters") |
| gender | `unisex` or NULL |
| image_url | Main product image URL |
| additional_images | Additional images (comma-separated) |
| price | Original price (USD format like "45.00USD") |
| sale | Sale price if on sale, else NULL |
| second_hand | `false` (always new) |
| tags | Product tags array |
| metadata | JSON with extra info |
| image_embedding | 768-dim SigLIP image embedding |
| info_embedding | 768-dim SigLIP text embedding |
| created_at | Import timestamp |

## Embedding Model

- **Model**: `google/siglip-base-patch16-384`
- **Dimensions**: 768
- **Normalization**: L2 normalized

Both image and text embeddings are generated for each product.