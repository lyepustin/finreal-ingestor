import os
import logging
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
    # Setup logging
    setup_logger()
    logger = logging.getLogger(__name__)
    
    try:
        cleaner = TransactionCleaner()
        
        logger.info("Starting deletion of all transactions and categories for configured user")
        cleaner.delete_user_transactions_and_categories()
        logger.info("Transaction and category deletion completed successfully")
        
    except Exception as e:
        logger.error(f"Error during transaction deletion: {str(e)}")
        raise

if __name__ == "__main__":
    main() 