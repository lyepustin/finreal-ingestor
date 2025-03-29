import os
from datetime import datetime
import pandas as pd
from uuid import UUID
import hashlib
from typing import List, Dict, Any
import logging
from .supabase import SupabaseClient
from .models import Transaction, TransactionCategory, AccountType
from dotenv import load_dotenv

load_dotenv()

class TransactionIngester:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.supabase = SupabaseClient().get_client()
        self.user_id = os.getenv("USER_ID")
        self.default_category_id = int(os.getenv("DEFAULT_CATEGORY_ID"))
        self.default_subcategory_id = int(os.getenv("DEFAULT_SUBCATEGORY_ID"))

    def get_account(self, account_number: str, bank_id: int, account_id: int) -> int:
        """Get existing account ID and verify bank_id matches
        
        Args:
            account_number: Not used for verification, kept for error messages
            bank_id: The bank ID that should match the account
            account_id: The account ID to verify
        """
        result = self.supabase.table("accounts")\
            .select("id, bank_id")\
            .eq("id", account_id)\
            .execute()
            
        if not result.data:
            raise ValueError(f"Account {account_number} not found")
        
        account = result.data[0]
        
        if account["bank_id"] != bank_id:
            raise ValueError(f"Account {account_number} exists but with different bank_id")
        
        return account["id"]

    def get_existing_transaction_hashes(self, hashes: List[str]) -> List[str]:
        """Get list of transaction hashes that already exist in the database"""
        result = self.supabase.table("transactions")\
            .select("uuid")\
            .in_("uuid", hashes)\
            .execute()
        
        return [row["uuid"] for row in result.data]

    def create_transaction_hash(self, row: Dict[str, Any]) -> str:
        """Create a unique hash for transaction deduplication using all relevant fields"""
        # Convert timestamp to string for consistent hashing, preserving full datetime if available
        if isinstance(row['date'], pd.Timestamp):
            date_str = row['date'].strftime('%Y-%m-%d %H:%M:%S')
        else:
            date_str = str(row['date'])
        
        # Create concatenated string with all relevant fields, making balance optional
        concat_str = f"{self.user_id}_{date_str}_{row['description']}_{row.get('category', '')}_{row['amount']}"
        if 'balance' in row:
            concat_str += f"_{row['balance']}"
        
        # Create SHA-256 hash of the concatenated string
        hash_obj = hashlib.sha256(concat_str.encode())
        
        # Convert the first 16 bytes of the hash to a UUID
        uuid_bytes = hash_obj.digest()[:16]
        transaction_uuid = str(UUID(bytes=uuid_bytes))
        
        return transaction_uuid

    def prepare_transaction_data(self, row: Dict[str, Any], account_id: int) -> Dict[str, Any]:
        """Prepare transaction data for insertion"""
        # Convert pandas Timestamp to ISO format string
        operation_date = row["date"].isoformat() if isinstance(row["date"], pd.Timestamp) else row["date"]
        
        # Ensure description is not NaN
        description = row["description"]
        if pd.isna(description):
            description = "No description"
        
        # Convert description to lowercase
        description = description.lower()
        
        return {
            "uuid": self.create_transaction_hash(row),
            "account_id": account_id,
            "operation_date": operation_date,
            "value_date": operation_date,
            "inserted_at": datetime.now().isoformat(),
            "description": description,
            "user_description": None
        }

    def prepare_transaction_category(self, transaction_id: int, amount: float) -> Dict[str, Any]:
        """Prepare transaction category data"""
        return {
            "transaction_id": transaction_id,
            "category_id": self.default_category_id,
            "subcategory_id": self.default_subcategory_id,
            "amount": amount
        }

    def ingest_transactions(self, csv_path: str, account_number: str, bank_id: int, account_id: int):
        """Ingest transactions from CSV file using batch processing"""
        try:
            # Get or verify account ID
            account_id = self.get_account(account_number, bank_id, account_id)
            
            # Read CSV file
            df = pd.read_csv(csv_path, sep=',')
            
            # Map Spanish column names to English based on bank
            if 'Fecha del movimiento' in df.columns:  # Ruralvia virtual card format
                column_mapping = {
                    'Fecha del movimiento': 'date',
                    'Concepto': 'description',
                    'Importe': 'amount',
                    'Comercio': 'merchant'
                }
            elif 'Fecha Ejecución' in df.columns:  # Ruralvia format
                column_mapping = {
                    'Fecha Ejecución': 'date',
                    'Descripcion': 'description',
                    'Importe': 'amount',
                    'Saldo': 'balance'
                }
            elif 'FECHA OPERACIÓN' in df.columns:  # Santander format
                column_mapping = {
                    'FECHA OPERACIÓN': 'date',
                    'CONCEPTO': 'description',
                    'IMPORTE EUR': 'amount',
                    'SALDO': 'balance'
                }
            elif 'more_info' in df.columns:  # BBVA format with more_info column
                # BBVA columns are already in English, no need to rename
                column_mapping = {}
            else:  # Default BBVA format or other formats
                column_mapping = {
                    'Fecha': 'date',
                    'Concepto': 'description',
                    'Importe': 'amount',
                    'Disponible': 'balance'
                }
            
            # Only rename columns if there's a mapping
            if column_mapping:
                df = df.rename(columns=column_mapping)
            
            # Convert date format to datetime - handle ISO format dates
            df['date'] = pd.to_datetime(df['date'])
            
            # For virtual cards, combine Concepto and Comercio for better description
            if 'merchant' in df.columns:
                df['description'] = df.apply(
                    lambda row: f"{row['description']} - {row['merchant']}" 
                    if not pd.isna(row['merchant']) else row['description'],
                    axis=1
                )
                df = df.drop('merchant', axis=1)
            
            # For BBVA transactions, handle special description concatenation
            if 'more_info' in df.columns:
                # List of values to exclude from more_info
                excluded_info = ["PAGO CON TARJETA", ""]
                
                # Combine description and more_info unless more_info is in excluded list
                def combine_descriptions(row):
                    desc = row['description'] if not pd.isna(row['description']) else ""
                    more = row['more_info'] if not pd.isna(row['more_info']) else ""
                    
                    # Only append more_info if it's not in excluded list
                    if more.strip() and more.strip() not in excluded_info:
                        combined = f"{desc.strip()} {more.strip()}".strip().lower()
                    else:
                        combined = desc.strip().lower()
                    
                    return combined if combined else "No description"
                
                df['description'] = df.apply(combine_descriptions, axis=1)
                
                # Remove the more_info column as we've combined it with description
                df = df.drop('more_info', axis=1)
            
            # Replace any NaN values with appropriate defaults
            df['description'] = df['description'].fillna("No description")
            
            # Convert number values to float
            def convert_number(x):
                if pd.isna(x):
                    return 0.0
                return float(x)
            
            df['amount'] = df['amount'].apply(convert_number)
            if 'balance' in df.columns:
                df['balance'] = df['balance'].apply(convert_number)
            
            if df.empty:
                self.logger.info("No transactions to process")
                return
            
            # Sort transactions by date in ascending order (oldest first)
            df = df.sort_values('date', ascending=True)
            
            # Calculate hashes for all transactions
            df['uuid'] = df.apply(lambda row: self.create_transaction_hash(row.to_dict()), axis=1)
            
            # For large files, we need to check for existing hashes in small batches
            # to avoid URL too long errors
            self.logger.info(f"Checking for existing transactions in {len(df)} records")
            
            hash_batch_size = 100  # Small batch size for hash checking
            all_hashes = []
            
            # Check for existing transactions in smaller batches
            for i in range(0, len(df), hash_batch_size):
                batch_hashes = df['uuid'].iloc[i:i+hash_batch_size].tolist()
                try:
                    existing_batch = self.get_existing_transaction_hashes(batch_hashes)
                    all_hashes.extend(existing_batch)
                except Exception as e:
                    self.logger.warning(f"Error checking batch {i//hash_batch_size}: {str(e)}")
                    # If we can't check, assume none exist and let later duplicate checks handle it
                    pass
            
            self.logger.info(f"Found {len(all_hashes)} existing transactions that will be skipped")
            
            # Process in smaller batches to avoid URL too long errors
            batch_size = 10  # Even smaller batch size for processing
            total_processed = 0
            total_success = 0
            
            # Process the DataFrame in batches
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size].copy()
                
                # Filter out existing transactions using the pre-fetched hashes
                new_transactions = batch_df[~batch_df['uuid'].isin(all_hashes)]
                
                if new_transactions.empty:
                    continue
                
                self.logger.info(f"Processing batch {i//batch_size + 1}: Found {len(new_transactions)} new transactions")
                
                # Process new transactions one by one to handle potential duplicate errors
                successful_transactions = []
                
                for _, row in new_transactions.iterrows():
                    try:
                        row_dict = row.to_dict()
                        transaction_data = self.prepare_transaction_data(row_dict, account_id)
                        
                        # Try to insert one transaction at a time
                        result = self.supabase.table("transactions").insert(transaction_data).execute()
                        
                        if result.data:
                            transaction = result.data[0]
                            # Add this hash to our known hashes to avoid future duplication attempts
                            all_hashes.append(transaction["uuid"])
                            
                            # Create category data
                            category_data = self.prepare_transaction_category(
                                transaction["id"],
                                float(row["amount"])
                            )
                            
                            # Insert category immediately
                            self.supabase.table("transaction_categories").insert(category_data).execute()
                            
                            successful_transactions.append(transaction)
                            total_success += 1
                    except Exception as e:
                        error_msg = str(e)
                        if "duplicate key value" in error_msg:
                            # Silently skip duplicates
                            pass
                        elif "Token \"NaN\"" in error_msg:
                            self.logger.warning(f"NaN value detected in transaction: {row_dict}")
                        else:
                            self.logger.warning(f"Failed to process transaction: {error_msg}")
                
                self.logger.info(f"Successfully ingested {len(successful_transactions)} transactions in this batch")
                total_processed += len(new_transactions)
            
            if total_processed == 0:
                self.logger.info("No new transactions to ingest")
            else:
                self.logger.info(f"Completed processing {total_success} out of {total_processed} transactions")
                
        except Exception as e:
            self.logger.error(f"Error in transaction ingestion: {str(e)}")
            raise