import os
import logging
from pathlib import Path
from datetime import datetime
from db.transaction_ingester import TransactionIngester
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

def get_account_config(bank, file_path):
    """Get account configuration based on bank and file"""
    if bank == "bbva":
        if "virtual" in file_path.lower():
            return {
                "bank_id": int(os.getenv("BBVA_BANK_ID")),
                "account_number": os.getenv("BBVA_ACCOUNT_NUMBER_TYPE_VIRTUAL_ID"),
                "account_id": int(os.getenv("BBVA_ACCOUNT_ID_TYPE_VIRTUAL_ID")),
            }
        else:
            return {
                "bank_id": int(os.getenv("BBVA_BANK_ID")),
                "account_number": os.getenv("BBVA_ACCOUNT_NUMBER_TYPE_BANK_ID"),
                "account_id": int(os.getenv("BBVA_ACCOUNT_ID_TYPE_BANK_ID")),
            }
    elif bank == "ruralvia":
        if "virtual" in file_path.lower():
            return {
                "bank_id": int(os.getenv("RURALVIA_BANK_ID")),
                "account_number": os.getenv("RURALVIA_ACCOUNT_NUMBER_TYPE_VIRTUAL_ID"),
                "account_id": int(os.getenv("RURALVIA_ACCOUNT_ID_TYPE_VIRTUAL_ID"))
            }
        else:
            return {
                "bank_id": int(os.getenv("RURALVIA_BANK_ID")),
                "account_number": os.getenv("RURALVIA_ACCOUNT_NUMBER_TYPE_BANK_ID"),
                "account_id": int(os.getenv("RURALVIA_ACCOUNT_ID_TYPE_BANK_ID")),
            }
    elif bank == "santander":
        if "virtual" in file_path.lower():
            return {
                "bank_id": int(os.getenv("SANTANDER_BANK_ID")),
                "account_number": os.getenv("SANTANDER_ACCOUNT_NUMBER_TYPE_VIRTUAL_ID"),
                "account_id": int(os.getenv("SANTANDER_ACCOUNT_ID_TYPE_VIRTUAL_ID"))
            }
        else:
            return {
                "bank_id": int(os.getenv("SANTANDER_BANK_ID")),
                "account_number": os.getenv("SANTANDER_ACCOUNT_NUMBER_TYPE_BANK_ID"),
                "account_id": int(os.getenv("SANTANDER_ACCOUNT_ID_TYPE_BANK_ID")),
            }
    return None

def process_historical_files():
    """Process all historical CSV files"""
    csv_dir = Path("data/csv")
    if not csv_dir.exists():
        raise ValueError(f"CSV directory not found: {csv_dir}")
    
    # Initialize ingester
    ingester = TransactionIngester()
    logger = logging.getLogger(__name__)
    
    success_count = 0
    total_count = 0
    
    # Process all CSV files
    for file_path in csv_dir.glob("*.csv"):
        total_count += 1
        filename = file_path.name.lower()
        
        # Determine bank and account type from filename
        if "bbva" in filename:
            bank = "bbva"
        elif "ruralvia" in filename:
            bank = "ruralvia"
        elif "santander" in filename:
            bank = "santander"
        else:
            logger.warning(f"Unknown bank in filename: {filename}")
            continue
            
        try:
            account_config = get_account_config(bank, filename)
            if not account_config:
                logger.warning(f"No account configuration found for {filename}")
                continue
                
            logger.info(f"Processing {filename} for {account_config['account_number']}")
            ingester.ingest_transactions(
                csv_path=str(file_path),
                account_number=account_config["account_number"],
                bank_id=account_config["bank_id"],
                account_id=account_config["account_id"]
            )
            logger.info(f"Successfully processed {account_config['account_number']}")
            success_count += 1
            
        except ValueError as ve:
            logger.error(f"Account error for {filename}: {str(ve)}")
        except Exception as e:
            logger.error(f"Error processing {filename}: {str(e)}")
    
    logger.info(f"Processed {success_count} out of {total_count} files successfully")

def main():
    # Setup logging
    setup_logger()
    logger = logging.getLogger(__name__)
    
    try:
        process_historical_files()
        logger.info("Historical transaction ingestion completed")
    except Exception as e:
        logger.error(f"Error during historical transaction processing: {str(e)}")
        raise

if __name__ == "__main__":
    main() 