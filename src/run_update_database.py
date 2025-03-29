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

def get_latest_files_by_bank(exports_dir):
    """Get the most recent file for each bank and account type from the exports directory"""
    exports_path = Path(exports_dir)
    if not exports_path.exists():
        raise ValueError(f"Exports directory not found: {exports_dir}")
    
    # Dictionary to store latest files by bank and account type
    latest_files = {}
    
    # Process all CSV files
    for file_path in exports_path.glob("*.csv"):
        filename = file_path.name
        timestamp_str = filename.split("_")[0]
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y%m%d")
            
            # Extract bank name from filename
            if "bbva" in filename.lower():
                bank = "bbva"
            elif "ruralvia" in filename.lower():
                bank = "ruralvia"
            else:
                continue
            
            # Determine account type
            if "virtual_card" in filename.lower() or "tarjeta_virtual" in filename.lower():
                account_type = "virtual"
            else:
                account_type = "regular"
            
            # Create a unique key for each bank and account type combination
            key = f"{bank}_{account_type}"
                
            # Update latest file if this one is newer
            if key not in latest_files or timestamp > latest_files[key]["timestamp"]:
                latest_files[key] = {
                    "file": str(file_path),
                    "timestamp": timestamp,
                    "filename": filename,
                    "bank": bank,
                    "account_type": account_type
                }
        except ValueError:
            continue
    
    return latest_files

def get_account_config(bank, file_path):
    """Get account configuration based on bank and file"""
    if bank == "bbva":
        if "virtual_card" in file_path.lower():
            return {
                "bank_id": int(os.getenv("BBVA_BANK_ID")),
                "account_number": os.getenv("BBVA_ACCOUNT_NUMBER_TYPE_VIRTUAL_ID"),
                "account_id": int(os.getenv("BBVA_ACCOUNT_ID_TYPE_VIRTUAL_ID")),
            }
        elif "cuentas_personales" in file_path.lower():
            return {
                "bank_id": int(os.getenv("BBVA_BANK_ID")),
                "account_number": os.getenv("BBVA_ACCOUNT_NUMBER_TYPE_BANK_ID"),
                "account_id": int(os.getenv("BBVA_ACCOUNT_ID_TYPE_BANK_ID")),
            }
    elif bank == "ruralvia":
        if "tarjeta_virtual" in file_path.lower():
            return {
                "bank_id": int(os.getenv("RURALVIA_BANK_ID")),
                "account_number": os.getenv("RURALVIA_ACCOUNT_NUMBER_TYPE_VIRTUAL_ID"),
                "account_id": int(os.getenv("RURALVIA_ACCOUNT_ID_TYPE_VIRTUAL_ID"))
            }
        elif "ahorro_menores" in file_path.lower():
            return {
                "bank_id": int(os.getenv("RURALVIA_BANK_ID")),
                "account_number": os.getenv("RURALVIA_ACCOUNT_NUMBER_TYPE_BANK_ID"),
                "account_id": int(os.getenv("RURALVIA_ACCOUNT_ID_TYPE_BANK_ID")),
            }
    elif bank == "santander":
        if "tarjeta_virtual" in file_path.lower():
            return {
                "bank_id": int(os.getenv("SANTANDER_BANK_ID")),
                "account_number": os.getenv("SANTANDER_ACCOUNT_NUMBER_TYPE_VIRTUAL_ID"),
                "account_id": int(os.getenv("SANTANDER_ACCOUNT_ID_TYPE_VIRTUAL_ID"))
            }
        elif "cuenta_personal" in file_path.lower():
            return {
                "bank_id": int(os.getenv("SANTANDER_BANK_ID")),
                "account_number": os.getenv("SANTANDER_ACCOUNT_NUMBER_TYPE_BANK_ID"),
                "account_id": int(os.getenv("SANTANDER_ACCOUNT_ID_TYPE_BANK_ID")),
            }
    return None

def process_account_files():
    """Process all account transaction files"""
    exports_dir = "data/exports"
    latest_files = get_latest_files_by_bank(exports_dir)
    
    # Initialize ingester
    ingester = TransactionIngester()
    logger = logging.getLogger(__name__)
    
    success_count = 0
    total_count = len(latest_files)
    
    for bank_account_key, file_info in latest_files.items():
        try:
            account_config = get_account_config(file_info["bank"], file_info["file"])
            if not account_config:
                logger.warning(f"No account configuration found for {file_info['filename']}")
                continue
                
            logger.info(f"Processing {file_info['filename']} for {account_config['account_number']} ({file_info['account_type']})")
            ingester.ingest_transactions(
                csv_path=file_info["file"],
                account_number=account_config["account_number"],
                bank_id=account_config["bank_id"],
                account_id=account_config["account_id"]
            )
            logger.info(f"Successfully processed {account_config['account_number']} ({file_info['account_type']})")
            success_count += 1
            
        except ValueError as ve:
            logger.error(f"Account error for {file_info['filename']}: {str(ve)}")
        except Exception as e:
            logger.error(f"Error processing {file_info['filename']}: {str(e)}")
    
    logger.info(f"Processed {success_count} out of {total_count} accounts successfully")

def main():
    # Setup logging
    setup_logger()
    logger = logging.getLogger(__name__)
    
    try:
        process_account_files()
        logger.info("Transaction ingestion completed")
    except Exception as e:
        logger.error(f"Error during transaction processing: {str(e)}")
        raise

if __name__ == "__main__":
    main() 