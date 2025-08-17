import logging
import os
from scrapers.caixa_scraper import CaixaScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def main():
    try:
        logger.info("Starting Caixa scraper...")
        debugger_address = os.getenv("DEBUGGER_ADDRESS")
        scraper = CaixaScraper(debugger_address=debugger_address)
        scraper.scrape()
        logger.info("Caixa scraper finished successfully.")
    except Exception as e:
        logger.critical(f"An unhandled error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
