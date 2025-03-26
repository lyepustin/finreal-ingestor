import logging
import os
import signal
import sys
import time
from scrapers.bbva_scraper_improved import BBVAScraperImproved
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def signal_handler(signum, frame):
    """Handle interrupt signals"""
    logger.info("Received interrupt signal. Force quitting...")
    sys.exit(0)

def main():
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info(f"Starting BBVA scraper")
        debugger_address = os.getenv("DEBUGGER_ADDRESS")
        # Initialize and run the scraper
        with BBVAScraperImproved(debugger_address) as scraper:
            logger.info("Starting BBVA login process")
            success_login = scraper.login()    
            if success_login:
                # First click on accounts overview
                success = scraper.click_accounts_overview()
                if success:
                    logger.info("Successfully clicked accounts overview")
                    
                    # Click on bank transactions
                    success = scraper.click_bank_transactions()
                    if success:
                        logger.info("Successfully clicked bank transactions")
                        # Wait a bit to capture transactions
                        time.sleep(2)
                        
                        # Go back to accounts overview
                        success = scraper.click_accounts_overview()
                        if success:
                            logger.info("Successfully went back to accounts overview")
                            
                            # Click on virtual card transactions
                            success = scraper.click_virtual_card_transactions()
                            if success:
                                logger.info("Successfully clicked virtual card transactions")
                                # Wait a bit to capture transactions
                                time.sleep(2)
                                
                                # Export transactions to CSV
                                logger.info("Exporting transactions to CSV")
                                if scraper.export_transactions_to_csv():
                                    logger.info("Successfully exported transactions to CSV")
                                else:
                                    logger.error("Failed to export transactions to CSV")
                            else:
                                logger.error("Failed to click virtual card transactions")
                        else:
                            logger.error("Failed to go back to accounts overview")
                    else:
                        logger.error("Failed to click bank transactions")
                else:
                    logger.error("Failed to click accounts overview")
            else:
                logger.error("Failed to log into BBVA")
                
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Exiting...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 