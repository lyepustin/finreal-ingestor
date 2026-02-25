import os
from typing import List, Dict
import logging
from .supabase import SupabaseClient
from dotenv import load_dotenv
import time
from datetime import datetime

load_dotenv()

class TransactionCleaner:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.supabase = SupabaseClient().get_client()
        self.user_id = os.getenv('USER_ID')
        if not self.user_id:
            raise ValueError("USER_ID not found in environment variables")
    
    def get_account_ids_for_user(self) -> List[int]:
        """Get all account IDs associated with the configured user"""
        # First get all bank IDs for the user
        bank_result = self.supabase.table("banks")\
            .select("id")\
            .eq("user_id", self.user_id)\
            .execute()
        
        if not bank_result.data:
            self.logger.info(f"No banks found for user {self.user_id}")
            return []
            
        bank_ids = [row["id"] for row in bank_result.data]
        
        # Then get all account IDs for these banks
        account_result = self.supabase.table("accounts")\
            .select("id")\
            .in_("bank_id", bank_ids)\
            .execute()
        
        if not account_result.data:
            self.logger.info(f"No accounts found for banks of user {self.user_id}")
            return []
        
        return [row["id"] for row in account_result.data]
    
    def get_transaction_count(self, account_ids: List[int]) -> int:
        """Get total transaction count for given account IDs"""
        if not account_ids:
            return 0
            
        result = self.supabase.table("transactions")\
            .select("count", count="exact")\
            .in_("account_id", account_ids)\
            .execute()
            
        return result.count if hasattr(result, 'count') else 0

    def delete_transaction_categories_for_accounts(self, account_ids: List[int]):
        """Delete transaction categories for transactions in specified accounts"""
        if not account_ids:
            return
            
        # Get transaction IDs with pagination
        self.logger.info("Getting transaction IDs for category deletion with pagination...")
        all_transaction_ids = []
        page = 0
        page_size = 1000  # Maximum allowed by Supabase
        
        while True:
            self.logger.info(f"Fetching transaction IDs - page {page+1}")
            transactions = self.supabase.table("transactions")\
                .select("id")\
                .in_("account_id", account_ids)\
                .range(page * page_size, (page + 1) * page_size - 1)\
                .execute()
            
            if not transactions.data:
                break
                
            page_transaction_ids = [row["id"] for row in transactions.data]
            all_transaction_ids.extend(page_transaction_ids)
            
            self.logger.info(f"Found {len(page_transaction_ids)} transactions on page {page+1}")
            
            if len(transactions.data) < page_size:
                break
                
            page += 1
            
        if not all_transaction_ids:
            self.logger.info("No transactions found for these accounts")
            return
            
        self.logger.info(f"Found total of {len(all_transaction_ids)} transactions to process categories for")
        
        # Delete categories in batches to avoid query size limitations
        batch_size = 1000
        total_deleted = 0
        
        for i in range(0, len(all_transaction_ids), batch_size):
            batch = all_transaction_ids[i:i + batch_size]
            self.logger.info(f"Deleting categories for batch of {len(batch)} transactions")
            
            self.supabase.table("transaction_categories")\
                .delete()\
                .in_("transaction_id", batch)\
                .execute()
                
            total_deleted += len(batch)
            self.logger.info(f"Deleted categories for {total_deleted}/{len(all_transaction_ids)} transactions")
    
    def delete_user_transactions_and_categories(self):
        """Delete all transactions and categories for the configured user using the optimized approach"""
        try:
            # Get all account IDs for the user
            account_ids = self.get_account_ids_for_user()
            if not account_ids:
                self.logger.info(f"No accounts found for user {self.user_id}")
                return True
            
            # Get transaction count for logging
            transaction_count = self.get_transaction_count(account_ids)
            if transaction_count == 0:
                self.logger.info(f"No transactions found for accounts of user {self.user_id}")
                return True
                
            self.logger.info(f"Found approximately {transaction_count} transactions to process")
            
            # Delete transaction categories first (using batch approach)
            self.delete_transaction_categories_for_accounts(account_ids)
            self.logger.info(f"Deletion of all transaction categories completed")
            
            # Delete all transactions
            self.logger.info("Deleting all transactions at once...")
            transactions_query = self.supabase.table("transactions")\
                .delete()\
                .in_("account_id", account_ids)\
                .execute()
            self.logger.info(f"Deletion of all transactions completed")
            
            # Verify no transactions remain
            remaining_count = self.get_transaction_count(account_ids)
            
            if remaining_count > 0:
                self.logger.warning(f"Found {remaining_count} remaining transactions after deletion")
                return False
            
            self.logger.info(f"Successfully deleted all transactions and categories for user {self.user_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting user transactions and categories: {str(e)}")
            raise

    def delete_2025_transactions(self):
        """Delete all transactions and their categories from 2025 for the configured user"""
        try:
            # Get all account IDs for the user
            account_ids = self.get_account_ids_for_user()
            if not account_ids:
                self.logger.info(f"No accounts found for user {self.user_id}")
                return True
            
            # Define date range for 2025
            start_date = "2025-01-01T00:00:00"
            end_date = "2025-12-31T23:59:59"
            
            # Get transaction IDs from 2025 with pagination
            self.logger.info("Getting 2025 transaction IDs with pagination...")
            all_transaction_ids = []
            page = 0
            page_size = 1000  # Maximum allowed by Supabase
            
            while True:
                self.logger.info(f"Fetching 2025 transaction IDs - page {page+1}")
                transactions = self.supabase.table("transactions")\
                    .select("id")\
                    .in_("account_id", account_ids)\
                    .gte("operation_date", start_date)\
                    .lte("operation_date", end_date)\
                    .range(page * page_size, (page + 1) * page_size - 1)\
                    .execute()
                
                if not transactions.data:
                    break
                    
                page_transaction_ids = [row["id"] for row in transactions.data]
                all_transaction_ids.extend(page_transaction_ids)
                
                self.logger.info(f"Found {len(page_transaction_ids)} transactions from 2025 on page {page+1}")
                
                if len(transactions.data) < page_size:
                    break
                    
                page += 1
                
            if not all_transaction_ids:
                self.logger.info("No transactions found from 2025")
                return True
                
            self.logger.info(f"Found total of {len(all_transaction_ids)} transactions from 2025")
            
            # Delete transaction categories for 2025 transactions
            self.logger.info("Deleting transaction categories for 2025 transactions...")
            
            # Delete in batches to avoid query size limitations
            batch_size = 1000
            total_deleted = 0
            
            for i in range(0, len(all_transaction_ids), batch_size):
                batch = all_transaction_ids[i:i + batch_size]
                self.logger.info(f"Deleting categories for batch of {len(batch)} transactions")
                
                self.supabase.table("transaction_categories")\
                    .delete()\
                    .in_("transaction_id", batch)\
                    .execute()
                    
                total_deleted += len(batch)
                self.logger.info(f"Deleted categories for {total_deleted}/{len(all_transaction_ids)} 2025 transactions")
            
            # Delete 2025 transactions directly
            self.logger.info("Deleting 2025 transactions...")
            transactions_query = self.supabase.table("transactions")\
                .delete()\
                .in_("account_id", account_ids)\
                .gte("operation_date", start_date)\
                .lte("operation_date", end_date)\
                .execute()
            
            self.logger.info(f"Deletion of 2025 transactions completed")
            
            # Verify deletion
            remaining_query = self.supabase.table("transactions")\
                .select("count", count="exact")\
                .in_("account_id", account_ids)\
                .gte("operation_date", start_date)\
                .lte("operation_date", end_date)\
                .execute()
                
            remaining_count = remaining_query.count if hasattr(remaining_query, 'count') else 0
            
            if remaining_count > 0:
                self.logger.warning(f"Found {remaining_count} remaining 2025 transactions after deletion")
                return False
            
            self.logger.info(f"Successfully deleted all transactions and categories from 2025 for user {self.user_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting 2025 transactions and categories: {str(e)}")
            raise

    def delete_transactions_after_march_2026(self) -> bool:
        """Delete all transactions with operation_date > 2026-03-31 and their categories (legacy wrong-year data)."""
        try:
            account_ids = self.get_account_ids_for_user()
            if not account_ids:
                self.logger.info(f"No accounts found for user {self.user_id}")
                return True

            # Fecha mínima: todo lo estrictamente posterior a marzo 2026 (>= 2026-04-01)
            from_date = "2026-04-01T00:00:00"

            self.logger.info("Getting transaction IDs with operation_date >= 2026-04-01 (pagination)...")
            all_transaction_ids = []
            page = 0
            page_size = 1000

            while True:
                self.logger.info(f"Fetching transaction IDs - page {page + 1}")
                transactions = self.supabase.table("transactions")\
                    .select("id")\
                    .in_("account_id", account_ids)\
                    .gte("operation_date", from_date)\
                    .range(page * page_size, (page + 1) * page_size - 1)\
                    .execute()

                if not transactions.data:
                    break

                page_ids = [row["id"] for row in transactions.data]
                all_transaction_ids.extend(page_ids)
                self.logger.info(f"Found {len(page_ids)} transactions on page {page + 1}")

                if len(transactions.data) < page_size:
                    break
                page += 1

            if not all_transaction_ids:
                self.logger.info("No transactions found with operation_date >= 2026-04-01")
                return True

            self.logger.info(f"Found total of {len(all_transaction_ids)} transactions to delete (and their categories)")

            # Borrar categorías en batches
            batch_size = 1000
            for i in range(0, len(all_transaction_ids), batch_size):
                batch = all_transaction_ids[i:i + batch_size]
                self.supabase.table("transaction_categories")\
                    .delete()\
                    .in_("transaction_id", batch)\
                    .execute()
                self.logger.info(f"Deleted categories for {min(i + batch_size, len(all_transaction_ids))}/{len(all_transaction_ids)} transactions")

            # Borrar transacciones
            self.logger.info("Deleting transactions with operation_date >= 2026-04-01...")
            self.supabase.table("transactions")\
                .delete()\
                .in_("account_id", account_ids)\
                .gte("operation_date", from_date)\
                .execute()

            self.logger.info("Successfully deleted legacy transactions (after March 2026) and their categories")
            return True

        except Exception as e:
            self.logger.error(f"Error deleting transactions after March 2026: {str(e)}")
            raise
