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

def display_transactions(transactions):
    """Display transactions in a formatted way"""
    if transactions:
        logger.info(f"Captured {len(transactions)} transactions:")
        for tx in transactions:
            logger.info(f"- {tx.description}: {tx.amount}€ ({tx.date})")

def display_financial_overview(overview):
    """Display financial overview in a formatted way"""
    if overview["accounts"]:
        logger.info("\nAccounts:")
        for account in overview["accounts"]:
            logger.info(f"- {account.account_type} ({account.account_number})")
            logger.info(f"  Available: {account.available_balance}€")
            logger.info(f"  Current: {account.current_balance}€")
    
    if overview["cards"]:
        logger.info("\nCards:")
        for card in overview["cards"]:
            logger.info(f"- {card.type} ({card.card_number})")
            logger.info(f"  Alias: {card.alias}")
            logger.info(f"  Status: {card.status}")
            logger.info(f"  Available: {card.available_balance}€")

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
            success = scraper.login()
            
            if success:
                logger.info("Successfully logged into BBVA")
                logger.info("WebSocket is ready to capture data")

                logger.info("Getting financial overview")
                # Check for financial overview
                overview = scraper.get_financial_overview()
                if overview["accounts"] or overview["cards"]:
                    display_financial_overview(overview)
                   
                logger.info("Navigate to your virtual card transactions and financial overview pages to capture data") 
                # Keep the browser open and monitor for data
                while True:
                    # Check for transactions
                    transactions = scraper.get_transactions()
                    if transactions:
                        display_transactions(transactions)
                    
                    # Clear processed data
                    scraper.clear_data()
                    time.sleep(1)  # Check every second
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