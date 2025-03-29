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

class HistoricalTransactionIngester:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.supabase = SupabaseClient().get_client()
        self.user_id = os.getenv("USER_ID")
        self.default_category_id = int(os.getenv("DEFAULT_CATEGORY_ID"))
        self.default_subcategory_id = int(os.getenv("DEFAULT_SUBCATEGORY_ID"))

    def get_account(self, account_number: str, bank_id: int, account_id: int) -> int:
        """Get existing account ID and verify bank_id matches"""
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
        """Create a unique hash for transaction deduplication"""
        # Convert timestamp to string for consistent hashing
        if isinstance(row['date'], pd.Timestamp):
            date_str = row['date'].strftime('%Y-%m-%d %H:%M:%S')
        else:
            date_str = str(row['date'])
        
        # Create concatenated string with all relevant fields
        concat_str = f"{self.user_id}_{date_str}_{row['description']}_{row.get('category', '')}_{row['amount']}"
        if 'balance' in row:
            concat_str += f"_{row['balance']}"
        
        # Create SHA-256 hash
        hash_obj = hashlib.sha256(concat_str.encode())
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
        """Ingest transactions from historical CSV file using bulk processing for empty database"""
        try:
            # Get or verify account ID
            account_id = self.get_account(account_number, bank_id, account_id)
            
            # Read CSV file
            df = pd.read_csv(csv_path, sep=';')
            
            # Map column names based on bank
            if 'bbva' in csv_path.lower():
                if 'virtual' in csv_path.lower():
                    column_mapping = {
                        'Fecha': 'date',
                        'Concepto': 'description',
                        'Importe': 'amount',
                        'Tarjeta': 'card_number'
                    }
                else:
                    column_mapping = {
                        'Fecha': 'date',
                        'Concepto': 'description',
                        'Movimiento': 'movement',
                        'Importe': 'amount',
                        'Disponible': 'balance'
                    }
            elif 'ruralvia' in csv_path.lower():
                if 'virtual' in csv_path.lower():
                    column_mapping = {
                        'Fecha del movimiento': 'date',
                        'Importe': 'amount',
                        'Concepto': 'description',
                        'Comercio': 'merchant'
                    }
                else:
                    column_mapping = {
                        'Fecha Ejecución': 'date',
                        'Descripcion': 'description',
                        'Importe': 'amount',
                        'Saldo': 'balance'
                    }
            elif 'santander' in csv_path.lower():
                if 'virtual' in csv_path.lower():
                    column_mapping = {
                        'FECHA OPERACIÓN': 'date',
                        'CONCEPTO': 'description',
                        'IMPORTE EUR': 'amount'
                    }
                else:
                    column_mapping = {
                        'FECHA OPERACIÓN': 'date',
                        'CONCEPTO': 'description',
                        'IMPORTE EUR': 'amount',
                        'SALDO': 'balance'
                    }
            else:
                raise ValueError(f"Unknown bank format in file: {csv_path}")
            
            df = df.rename(columns=column_mapping)
            
            # Convert date format to datetime - handle different date formats
            if 'bbva' in csv_path.lower() and 'virtual' in csv_path.lower():
                # Handle ISO8601 format for BBVA virtual accounts (2024-10-24T15:58:59.000+0200)
                df['date'] = pd.to_datetime(df['date'], format='ISO8601')
            else:
                # Handle Spanish date format (DD/MM/YYYY) for other files
                df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
            
            # Replace any NaN values with appropriate defaults
            df['description'] = df['description'].fillna("No description")
            
            # For BBVA files, merge Concepto and Movimiento for better description
            if 'bbva' in csv_path.lower() and 'movement' in df.columns:
                # Only add Movimiento if it's not 'Otros', 'Pago con tarjeta', or empty
                def merge_description(row):
                    if pd.isna(row['movement']) or row['movement'] == '' or 'Otros' in row['movement'] or 'Pago con tarjeta' in row['movement']:
                        return row['description']
                    return f"{row['description']} - {row['movement']}"
                
                df['description'] = df.apply(merge_description, axis=1)
                df = df.drop('movement', axis=1)
            
            # For virtual accounts, combine Concepto and Comercio for better description
            if 'virtual' in csv_path.lower() and 'merchant' in df.columns:
                df['description'] = df['description'] + ' - ' + df['merchant'].fillna('')
                df = df.drop('merchant', axis=1)
            
            # Convert Spanish number format to float
            def convert_spanish_number(x):
                if pd.isna(x):
                    return 0.0
                return float(str(x).replace('.', '').replace(',', '.'))
            
            df['amount'] = df['amount'].apply(convert_spanish_number)
            if 'balance' in df.columns:
                df['balance'] = df['balance'].apply(convert_spanish_number)
            
            if df.empty:
                self.logger.info("No transactions to process")
                return
            
            # Sort transactions by date in ascending order
            df = df.sort_values('date', ascending=True)
            
            # Calculate hashes for all transactions
            df['uuid'] = df.apply(lambda row: self.create_transaction_hash(row.to_dict()), axis=1)
            
            # Process in larger batches - optimized for empty database
            batch_size = 500  # Increased batch size for better performance
            total_transactions = len(df)
            self.logger.info(f"Starting bulk import of {total_transactions} transactions")
            
            for i in range(0, total_transactions, batch_size):
                batch_df = df.iloc[i:i+batch_size].copy()
                
                # Prepare all transaction data in batch
                transaction_data = []
                for _, row in batch_df.iterrows():
                    try:
                        row_dict = row.to_dict()
                        transaction_data.append(self.prepare_transaction_data(row_dict, account_id))
                    except Exception as e:
                        self.logger.warning(f"Failed to prepare transaction: {str(e)}")
                
                if not transaction_data:
                    continue
                
                # Bulk insert transactions
                self.logger.info(f"Inserting batch of {len(transaction_data)} transactions")
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
                
                self.logger.info(f"Processed batch {i//batch_size + 1}/{(total_transactions-1)//batch_size + 1}")
            
            self.logger.info(f"Completed bulk import of {total_transactions} transactions")
                
        except Exception as e:
            self.logger.error(f"Error in transaction ingestion: {str(e)}")
            raise 