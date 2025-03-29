import os
import logging
import argparse
from db.transaction_cleaner import TransactionCleaner
from dotenv import load_dotenv

load_dotenv()

def setup_logger():
    """Configure the logging system"""
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Set higher log level for HTTP-related loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    
    # Console handler only
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Add handler to root logger
    logger.addHandler(console_handler)

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Delete transactions and categories for the configured user')
    parser.add_argument('--only-2025', action='store_true', 
                        help='Delete only transactions from 2025 (default: False)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logger()
    logger = logging.getLogger(__name__)
    
    try:
        cleaner = TransactionCleaner()
        
        if args.only_2025:
            logger.info("Starting deletion of 2025 transactions and categories for configured user")
            cleaner.delete_2025_transactions()
            logger.info("2025 transaction and category deletion completed successfully")
        else:
            logger.info("Starting deletion of all transactions and categories for configured user")
            cleaner.delete_user_transactions_and_categories()
            logger.info("Transaction and category deletion completed successfully")
        
    except Exception as e:
        logger.error(f"Error during transaction deletion: {str(e)}")
        raise

if __name__ == "__main__":
    main() 