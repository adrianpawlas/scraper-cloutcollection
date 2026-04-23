#!/bin/bash
#
# Clout Collection Scraper - Automation Wrapper
# Usage: ./run_scraper.sh [max_products]
#
# This script:
# 1. Logs output to scraper.log
# 2. Sends email notifications on failure
# 3. Can be run via cron or manually
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/scraper.log"
MAX_PRODUCTS="$1"

cd "$SCRIPT_DIR"

echo "========================================" >> "$LOG_FILE"
echo "Clout Collection Scraper Log" >> "$LOG_FILE"
echo "Started: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Run the scraper
if [ -n "$MAX_PRODUCTS" ]; then
    python3 main.py --max-products "$MAX_PRODUCTS" 2>&1 | tee -a "$LOG_FILE"
else
    python3 main.py 2>&1 | tee -a "$LOG_FILE"
fi

EXIT_CODE=${PIPESTATUS[0]}

echo "Finished: $(date)" >> "$LOG_FILE"
echo "Exit code: $EXIT_CODE" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Send notification on failure
if [ $EXIT_CODE -ne 0 ]; then
    echo "Scraper failed with exit code $EXIT_CODE. Check log for details."
    
    # Optional: Send email notification
    # mail -s "Clout Collection Scraper FAILED" your@email.com < "$LOG_FILE"
fi

exit $EXIT_CODE