import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import logging
from pathlib import Path
from datetime import datetime
from db.historical_transaction_ingester import HistoricalTransactionIngester
from dotenv import load_dotenv
import pandas as pd
import hashlib
from uuid import UUID

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

def get_caixa_account_config(account_name):
    """Get account configuration based on account name from CaixaBank CSV"""
    if account_name == "Cuenta 1433":
        return {
            "bank_id": int(os.getenv("CAIXA_BANK_ID")),
            "account_number": os.getenv("CAIXA_ACCOUNT_NUMBER_TYPE_BANK_CUENTA_1433"),
            "account_id": int(os.getenv("CAIXA_ACCOUNT_ID_TYPE_BANK_CUENTA_1433")),
        }
    elif account_name == "Den 3363":
        return {
            "bank_id": int(os.getenv("CAIXA_BANK_ID")),
            "account_number": os.getenv("CAIXA_ACCOUNT_NUMBER_TYPE_CARD_DEN_3363"),
            "account_id": int(os.getenv("CAIXA_ACCOUNT_ID_TYPE_CARD_DEN_3363")),
        }
    elif account_name == "Pau 5246":
        return {
            "bank_id": int(os.getenv("CAIXA_BANK_ID")),
            "account_number": os.getenv("CAIXA_ACCOUNT_NUMBER_TYPE_CARD_PAU_5246"),
            "account_id": int(os.getenv("CAIXA_ACCOUNT_ID_TYPE_CARD_PAU_5246")),
        }
    elif account_name == "CYBER 2526":
        return {
            "bank_id": int(os.getenv("CAIXA_BANK_ID")),
            "account_number": os.getenv("CAIXA_ACCOUNT_NUMBER_TYPE_CARD_CYBER_2526"),
            "account_id": int(os.getenv("CAIXA_ACCOUNT_ID_TYPE_CARD_CYBER_2526")),
        }
    return None

def create_caixa_transaction_hash(row_dict, account_name, user_id):
    """Create a unique hash for CaixaBank transaction including account information"""
    # Convert timestamp to string for consistent hashing
    if isinstance(row_dict['date'], pd.Timestamp):
        date_str = row_dict['date'].strftime('%Y-%m-%d %H:%M:%S')
    else:
        date_str = str(row_dict['date'])
    
    # Create concatenated string with all relevant fields INCLUDING account name
    concat_str = f"{user_id}_{date_str}_{row_dict['description']}_{row_dict.get('category', '')}_{row_dict['amount']}_{account_name}"
    if 'balance' in row_dict:
        concat_str += f"_{row_dict['balance']}"
    
    # Create SHA-256 hash
    hash_obj = hashlib.sha256(concat_str.encode())
    uuid_bytes = hash_obj.digest()[:16]
    transaction_uuid = str(UUID(bytes=uuid_bytes))
    
    return transaction_uuid

def process_caixa_csv_by_account(csv_file_path):
    """Process CaixaBank CSV file and split transactions by account"""
    logger = logging.getLogger(__name__)
    
    # Read the CSV file
    df = pd.read_csv(csv_file_path, sep=';')
    
    # Group transactions by account
    account_groups = df.groupby('Cuenta')
    
    results = {}
    
    for account_name, account_df in account_groups:
        logger.info(f"Found {len(account_df)} transactions for account: {account_name}")
        
        # Map account names to our standardized names
        if "MyCard" in account_name and "3363" in account_name:
            mapped_account = "Den 3363"
        elif "MyCard" in account_name and "5246" in account_name:
            mapped_account = "Pau 5246"
        elif "CYBERTARJETA" in account_name and "2526" in account_name:
            mapped_account = "CYBER 2526"
        elif "Cuenta" in account_name and "1433" in account_name:
            mapped_account = "Cuenta 1433"
        else:
            logger.warning(f"Unknown account type: {account_name}")
            continue
        
        # Get account configuration
        account_config = get_caixa_account_config(mapped_account)
        if not account_config:
            logger.warning(f"No configuration found for account: {mapped_account}")
            continue
        
        # Save account-specific CSV
        account_csv_path = csv_file_path.replace('.csv', f'_{mapped_account.replace(" ", "_")}.csv')
        
        # Remove the 'Cuenta' column for processing as it's not needed in the transaction data
        account_df_clean = account_df.drop('Cuenta', axis=1)
        account_df_clean.to_csv(account_csv_path, index=False, sep=';')
        
        results[mapped_account] = {
            "csv_path": account_csv_path,
            "config": account_config,
            "transaction_count": len(account_df_clean)
        }
        
        logger.info(f"Created account-specific CSV: {account_csv_path}")
    
    return results

def process_caixa_historical_files():
    """Process CaixaBank historical CSV files"""
    csv_dir = Path("data/csv")
    if not csv_dir.exists():
        raise ValueError(f"CSV directory not found: {csv_dir}")
    
    # Initialize historical ingester
    ingester = HistoricalTransactionIngester()
    logger = logging.getLogger(__name__)
    
    success_count = 0
    total_count = 0
    
    # Look for CaixaBank CSV files
    caixa_files = list(csv_dir.glob("caixabank-*.csv"))
    
    if not caixa_files:
        logger.warning("No CaixaBank CSV files found in data/csv directory")
        return
    
    for file_path in caixa_files:
        logger.info(f"Processing CaixaBank file: {file_path.name}")
        
        try:
            # Process the CSV and split by account
            account_results = process_caixa_csv_by_account(str(file_path))
            
            # Process each account's transactions
            for account_name, account_info in account_results.items():
                total_count += 1
                
                try:
                    logger.info(f"Processing {account_info['transaction_count']} transactions for {account_name}")
                    
                    # Use the enhanced CaixaBank-specific method
                    ingester.ingest_caixa_transactions(
                        csv_path=account_info["csv_path"],
                        account_number=account_info["config"]["account_number"],
                        bank_id=account_info["config"]["bank_id"],
                        account_id=account_info["config"]["account_id"],
                        account_name=account_name  # Pass account name for hash calculation
                    )
                    
                    logger.info(f"Successfully processed {account_name}")
                    success_count += 1
                    
                    # Clean up temporary account-specific CSV
                    os.remove(account_info["csv_path"])
                    
                except ValueError as ve:
                    logger.error(f"Account error for {account_name}: {str(ve)}")
                except Exception as e:
                    logger.error(f"Error processing {account_name}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error processing file {file_path.name}: {str(e)}")
    
    logger.info(f"Processed {success_count} out of {total_count} accounts successfully")

# Extend the HistoricalTransactionIngester to handle CaixaBank format
def add_caixa_support_to_ingester():
    """Add CaixaBank-specific processing to the ingester"""
    
    def ingest_caixa_transactions(self, csv_path: str, account_number: str, bank_id: int, account_id: int, account_name: str):
        """Ingest CaixaBank transactions from CSV file with account-specific hash calculation"""
        try:
            # Get or verify account ID
            account_id = self.get_account(account_number, bank_id, account_id)
            
            # Read CSV file
            df = pd.read_csv(csv_path, sep=';')
            
            # CaixaBank column mapping - only use Comercio as description
            column_mapping = {
                'Fecha del movimiento': 'date',
                'Importe': 'amount',
                'Comercio': 'description'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Convert date format to datetime (DD/MM/YYYY)
            df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
            
            # Replace any NaN values with appropriate defaults
            df['description'] = df['description'].fillna("No description")
            
            # Convert amount to float (already in correct format from our processing script)
            df['amount'] = df['amount'].astype(float)
            
            if df.empty:
                self.logger.info("No transactions to process")
                return
            
            # Sort transactions by date in ascending order
            df = df.sort_values('date', ascending=True)
            
            # Calculate hashes for all transactions INCLUDING account name
            df['uuid'] = df.apply(
                lambda row: create_caixa_transaction_hash(row.to_dict(), account_name, self.user_id), 
                axis=1
            )
            
            # Process in larger batches - optimized for empty database
            batch_size = 500
            total_transactions = len(df)
            self.logger.info(f"Starting bulk import of {total_transactions} CaixaBank transactions for {account_name}")
            
            for i in range(0, total_transactions, batch_size):
                batch_df = df.iloc[i:i+batch_size].copy()
                
                # Prepare all transaction data in batch
                transaction_data = []
                for _, row in batch_df.iterrows():
                    try:
                        row_dict = row.to_dict()
                        # Use custom hash that includes account name
                        transaction_data_item = self.prepare_transaction_data(row_dict, account_id)
                        # Override the UUID with our custom hash
                        transaction_data_item["uuid"] = row["uuid"]
                        transaction_data.append(transaction_data_item)
                    except Exception as e:
                        self.logger.warning(f"Failed to prepare transaction: {str(e)}")
                
                if not transaction_data:
                    continue
                
                # Bulk insert transactions
                self.logger.info(f"Inserting batch of {len(transaction_data)} transactions for {account_name}")
                result = self.supabase.table("transactions").insert(transaction_data).execute()
                
                # Prepare transaction categories for the inserted transactions
                if result.data:
                    category_data = []
                    for index, transaction in enumerate(result.data):
                        try:
                            amount = float(batch_df.iloc[index]["amount"])
                            category_data.append(self.prepare_transaction_category(transaction["id"], amount))
                        except Exception as e:
                            self.logger.warning(f"Failed to prepare category: {str(e)}")
                    
                    # Bulk insert transaction categories
                    if category_data:
                        self.logger.info(f"Inserting batch of {len(category_data)} transaction categories")
                        self.supabase.table("transaction_categories").insert(category_data).execute()
                
                self.logger.info(f"Processed batch {i//batch_size + 1}/{(total_transactions-1)//batch_size + 1} for {account_name}")
            
            self.logger.info(f"Completed bulk import of {total_transactions} CaixaBank transactions for {account_name}")
                
        except Exception as e:
            self.logger.error(f"Error in CaixaBank transaction ingestion for {account_name}: {str(e)}")
            raise
    
    # Add the method to the class
    HistoricalTransactionIngester.ingest_caixa_transactions = ingest_caixa_transactions

def main():
    # Setup logging
    setup_logger()
    logger = logging.getLogger(__name__)
    
    # Add CaixaBank support to the ingester
    add_caixa_support_to_ingester()
    
    try:
        process_caixa_historical_files()
        logger.info("CaixaBank historical transaction ingestion completed")
    except Exception as e:
        logger.error(f"Error during CaixaBank historical transaction processing: {str(e)}")
        raise

if __name__ == "__main__":
    main() 