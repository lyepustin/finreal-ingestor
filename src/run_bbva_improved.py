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

def display_transactions(transactions, source: str):
    """Display transactions in a formatted way"""
    if transactions:
        logger.info(f"\nCaptured {len(transactions)} {source} transactions:")
        # Print header
        if source == "bank_account":
            logger.info("   Date/Time     |   Amount   |     Balance    |    Category         | Description")
            logger.info("-" * 90)
        else:
            logger.info("   Date/Time     |   Amount   |    Category         | Description")
            logger.info("-" * 80)
            
        for tx in transactions:
            # Format amount with sign and 2 decimal places
            amount_str = f"{tx.amount:+.2f}€"
            # Add color coding: red for negative, green for positive amounts
            if tx.amount < 0:
                amount_str = f"\033[91m{amount_str}\033[0m"  # Red
            else:
                amount_str = f"\033[92m{amount_str}\033[0m"  # Green
            
            if source == "bank_account":
                # Format balance with thousands separator and 2 decimal places
                balance_str = f"{tx.balance:,.2f}€"
                logger.info(f"{tx.date.strftime('%Y-%m-%d %H:%M')} | {amount_str:>10} | {balance_str:>13} | {tx.category:<18} | {tx.description}")
            else:
                logger.info(f"{tx.date.strftime('%Y-%m-%d %H:%M')} | {amount_str:>10} | {tx.category:<18} | {tx.description}")

def display_financial_overview(overview):
    """Display financial overview in a formatted way"""
    if overview["accounts"]:
        logger.info("\nAccounts:")
        for account in overview["accounts"]:
            logger.info(f"- {account.account_type} ({account.account_number})")
            logger.info(f"  Available: {account.available_balance:,.2f}€")
            logger.info(f"  Current: {account.current_balance:,.2f}€")
    
    if overview["cards"]:
        logger.info("\nCards:")
        for card in overview["cards"]:
            logger.info(f"- {card.type} ({card.card_number})")
            logger.info(f"  Alias: {card.alias}")
            logger.info(f"  Status: {card.status}")
            logger.info(f"  Available: {card.available_balance:,.2f}€")

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
                   
                logger.info("Navigate to your bank account and virtual card transactions pages to capture data") 
                # Keep the browser open and monitor for data
                while True:
                    # Check for bank account transactions
                    bank_transactions = scraper.get_bank_account_transactions()
                    if bank_transactions:
                        display_transactions(bank_transactions, "bank_account")
                    
                    # Check for virtual card transactions
                    virtual_transactions = scraper.get_virtual_card_transactions()
                    if virtual_transactions:
                        display_transactions(virtual_transactions, "virtual_card")
                    
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