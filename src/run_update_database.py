import os
import logging
import pandas as pd
import tempfile
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
            elif "caixa" in filename.lower():
                bank = "caixa"
            else:
                continue
            
            # Handle Caixa differently - all accounts in one file
            if bank == "caixa":
                # For Caixa, we'll process the file once and handle multiple accounts within it
                key = f"{bank}_all_accounts"
                if key not in latest_files or timestamp > latest_files[key]["timestamp"]:
                    latest_files[key] = {
                        "file": str(file_path),
                        "timestamp": timestamp,
                        "filename": filename,
                        "bank": bank,
                        "account_type": "all_accounts"
                    }
                continue
            
            # Determine account type for other banks
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
    elif bank == "caixa":
        # For Caixa, we need to handle multiple accounts in one file
        # This will be handled separately in process_caixa_transactions
        return None
    return None

def process_caixa_transactions(csv_path):
    """Process Caixa transactions by grouping them by account and processing each separately"""
    logger = logging.getLogger(__name__)
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Group transactions by account
        account_groups = df.groupby('account')
        
        # Initialize ingester
        ingester = TransactionIngester()
        
        success_count = 0
        total_accounts = len(account_groups)
        
        logger.info(f"Processing Caixa transactions for {total_accounts} accounts")
        
        for account_name, account_transactions in account_groups:
            try:
                # Get account configuration based on account name
                account_config = get_caixa_account_config(account_name)
                if not account_config:
                    logger.warning(f"No account configuration found for {account_name}")
                    continue
                
                # Create a temporary CSV file for this account
                temp_csv_path = create_temp_csv_for_account(account_transactions, account_name)
                
                logger.info(f"Processing {account_name} ({len(account_transactions)} transactions)")
                
                # Ingest transactions for this account
                ingester.ingest_transactions(
                    csv_path=temp_csv_path,
                    account_number=account_config["account_number"],
                    bank_id=account_config["bank_id"],
                    account_id=account_config["account_id"]
                )
                
                logger.info(f"Successfully processed {account_name}")
                success_count += 1
                
                # Clean up temporary file
                os.remove(temp_csv_path)
                
            except Exception as e:
                logger.error(f"Error processing account {account_name}: {str(e)}")
        
        logger.info(f"Processed {success_count} out of {total_accounts} Caixa accounts successfully")
        return success_count
        
    except Exception as e:
        logger.error(f"Error reading Caixa CSV file: {str(e)}")
        raise

def get_caixa_account_config(account_name):
    """Get Caixa account configuration based on account name from CSV"""
    # Map account names from CSV to environment variables
    account_mapping = {
        "Cuenta 1433": {
            "bank_id": int(os.getenv("CAIXA_BANK_ID")),
            "account_number": os.getenv("CAIXA_ACCOUNT_NUMBER_TYPE_BANK_CUENTA_1433"),
            "account_id": int(os.getenv("CAIXA_ACCOUNT_ID_TYPE_BANK_CUENTA_1433")),
        },
        "MyCard 3363": {
            "bank_id": int(os.getenv("CAIXA_BANK_ID")),
            "account_number": os.getenv("CAIXA_ACCOUNT_NUMBER_TYPE_CARD_DEN_3363"),
            "account_id": int(os.getenv("CAIXA_ACCOUNT_ID_TYPE_CARD_DEN_3363")),
        },
        "MyCard 5246": {
            "bank_id": int(os.getenv("CAIXA_BANK_ID")),
            "account_number": os.getenv("CAIXA_ACCOUNT_NUMBER_TYPE_CARD_PAU_5246"),
            "account_id": int(os.getenv("CAIXA_ACCOUNT_ID_TYPE_CARD_PAU_5246")),
        },
        "CYBERTARJETA 2526": {
            "bank_id": int(os.getenv("CAIXA_BANK_ID")),
            "account_number": os.getenv("CAIXA_ACCOUNT_NUMBER_TYPE_CARD_CYBER_2526"),
            "account_id": int(os.getenv("CAIXA_ACCOUNT_ID_TYPE_CARD_CYBER_2526")),
        }
    }
    
    return account_mapping.get(account_name)

def create_temp_csv_for_account(account_transactions, account_name):
    """Create a temporary CSV file for a specific account"""
    
    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    
    # Write header
    temp_file.write("date,description,category,amount,account\n")
    
    # Write transactions for this account
    for _, transaction in account_transactions.iterrows():
        temp_file.write(f"{transaction['date']},{transaction['description']},{transaction['category']},{transaction['amount']},{transaction['account']}\n")
    
    temp_file.close()
    return temp_file.name

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
            # Handle Caixa files differently
            if file_info["bank"] == "caixa":
                logger.info(f"Processing Caixa file: {file_info['filename']}")
                caixa_success_count = process_caixa_transactions(file_info["file"])
                success_count += 1 if caixa_success_count > 0 else 0
                continue
            
            # Handle other banks as before
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