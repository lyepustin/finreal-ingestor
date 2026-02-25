import os
import sys
import logging
import argparse
from pathlib import Path

# Allow importing db when run from project root (e.g. python src/manual/run_transaction_cleaner.py)
_src_dir = Path(__file__).resolve().parent.parent
if str(_src_dir) not in sys.path:
    sys.path.insert(0, str(_src_dir))

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
    parser.add_argument('--after-march-2026', action='store_true',
                        help='Delete only transactions with date >= 2026-04-01 (legacy wrong-year data) and their categories')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logger()
    logger = logging.getLogger(__name__)
    
    try:
        cleaner = TransactionCleaner()
        
        if args.after_march_2026:
            logger.info("Starting deletion of transactions after March 2026 and their categories")
            cleaner.delete_transactions_after_march_2026()
            logger.info("Legacy (after March 2026) transaction and category deletion completed successfully")
        elif args.only_2025:
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