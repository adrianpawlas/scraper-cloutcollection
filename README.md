# Clout Collection Scraper

Automated scraper for [Clout Collection](https://cloutcollection.shop) - a streetwear fashion store. Scrapes all products, generates SigLIP embeddings (768-dim), and imports to Supabase.

## Features

- **Automated Product Scraping**: Collects all ~440+ products from the collection page
- **Smart "Load More" Handling**: Automatically clicks load more button until all products loaded
- **Rich Data Extraction**: Title, description, price, images, sizes, colors, category
- **Dual Embeddings**: 
  - Image embeddings from product photos (google/siglip-base-patch16-384)
  - Text embeddings from product info
- **Supabase Integration**: Auto-upserts to database with conflict handling
- **Weekly Automation**: Runs automatically every Thursday at midnight
- **Manual Execution**: Can be run on-demand anytime

## Installation

```bash
# Clone the repository
git clone https://github.com/adrianpawlas/scraper-cloutcollection.git
cd scraper-cloutcollection

# Install dependencies
pip install -r requirements.txt

# Install Playwright browser
playwright install chromium
```

## Usage

### Full Scrape (All Products)

```bash
python main.py
```

### Test Mode (5 Products)

```bash
python main.py --test
```

### Limit Products

```bash
python main.py --max-products 50
```

### Manual Run

You can also run manually using the installed automation script:

```bash
# Run the scraper manually
python run_scraper.py

# Or just run main.py
python main.py
```

## Automated Schedule

The scraper is set up to run automatically every **Thursday at midnight** using GitHub Actions.

To configure or manually trigger the workflow, go to:
https://github.com/adrianpawlas/scraper-cloutcollection/actions

### Manual Trigger via GitHub

1. Go to the Actions tab
2. Select "Weekly Clout Collection Scraper"
3. Click "Run workflow"

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
| gender | `unisex` |
| image_url | Main product image URL |
| additional_images | Additional images (comma-separated) |
| price | Original price (USD format, e.g., "45.00USD") |
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

## Project Structure

```
scraper-cloutcollection/
├── .github/
│   └── workflows/
│       └── scrape.yml      # GitHub Actions workflow
├── config.py               # Configuration
├── database.py             # Supabase client
├── scraper.py              # Playwright scraper
├── embeddings.py           # SigLIP embedder
├── main.py                 # Main pipeline
├── run_scraper.py         # CLI runner
└── requirements.txt       # Dependencies
```

## Supabase Setup

The project uses these Supabase credentials (configured in config.py):
- URL: `https://yqawmzggcgpeyaaynrjk.supabase.co`
- Table: `products`
- Unique constraint: `(source, product_url)`

## Troubleshooting

### Embedding Errors

If you see huggingface rate limit warnings, set a HF_TOKEN:
```bash
export HF_TOKEN=your_token_here
```

### Playwright Issues

Reinstall Chromium:
```bash
playwright install chromium
```

### Database Conflicts

The scraper uses upsert - existing products will be updated with new embeddings.

## License

MIT