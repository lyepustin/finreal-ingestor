import os
from typing import List, Dict
import logging
from .supabase import SupabaseClient
from dotenv import load_dotenv
import time

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
    
    def get_transaction_ids(self, account_ids: List[int]) -> List[int]:
        """Get all transaction IDs for given account IDs with pagination"""
        if not account_ids:
            return []
            
        all_ids = []
        page = 0
        page_size = 1000  # Maximum allowed by Supabase
        
        while True:
            result = self.supabase.table("transactions")\
                .select("id")\
                .in_("account_id", account_ids)\
                .range(page * page_size, (page + 1) * page_size - 1)\
                .execute()
            
            if not result.data:
                break
                
            all_ids.extend([row["id"] for row in result.data])
            
            if len(result.data) < page_size:
                break
                
            page += 1
            time.sleep(0.5)  # Small delay between pages
            
        return all_ids
    
    def delete_all_transaction_categories(self, transaction_ids: List[int]):
        """Delete all transaction categories for given transaction IDs"""
        try:
            if not transaction_ids:
                return
            
            self.logger.info(f"Found {len(transaction_ids)} transactions with categories to delete")
            
            # Delete all transaction categories for these transactions
            total_deleted = 0
            for i in range(0, len(transaction_ids), self.batch_size):
                batch = transaction_ids[i:i + self.batch_size]
                self.supabase.table("transaction_categories")\
                    .delete()\
                    .in_("transaction_id", batch)\
                    .execute()
                total_deleted += len(batch)
                self.logger.info(f"Deleted transaction categories for {len(batch)} transactions (Total: {total_deleted})")
                time.sleep(0.5)  # Small delay to avoid rate limiting
            
            # Verify no categories remain (with pagination)
            remaining_count = 0
            page = 0
            page_size = 1000
            
            while True:
                remaining = self.supabase.table("transaction_categories")\
                    .select("id")\
                    .in_("transaction_id", transaction_ids)\
                    .range(page * page_size, (page + 1) * page_size - 1)\
                    .execute()
                
                remaining_count += len(remaining.data)
                
                if len(remaining.data) < page_size:
                    break
                    
                page += 1
                time.sleep(0.5)
            
            if remaining_count > 0:
                raise ValueError(f"Found {remaining_count} remaining transaction categories")
            
            self.logger.info("Successfully deleted all transaction categories")
            
        except Exception as e:
            self.logger.error(f"Error deleting transaction categories: {str(e)}")
            raise
    
    def delete_all_transactions(self, account_ids: List[int]):
        """Delete all transactions for given account IDs"""
        try:
            if not account_ids:
                return
                
            # First get all transaction IDs with pagination
            transaction_ids = self.get_transaction_ids(account_ids)
            if not transaction_ids:
                return
            
            self.logger.info(f"Found {len(transaction_ids)} transactions to delete")
            
            # Delete transactions in batches
            total_deleted = 0
            for i in range(0, len(transaction_ids), self.batch_size):
                batch = transaction_ids[i:i + self.batch_size]
                self.supabase.table("transactions")\
                    .delete()\
                    .in_("id", batch)\
                    .execute()
                total_deleted += len(batch)
                self.logger.info(f"Deleted {len(batch)} transactions (Total: {total_deleted})")
                time.sleep(0.5)  # Small delay to avoid rate limiting
            
            # Verify no transactions remain (with pagination)
            remaining_count = 0
            page = 0
            page_size = 1000
            
            while True:
                remaining = self.supabase.table("transactions")\
                    .select("id")\
                    .in_("account_id", account_ids)\
                    .range(page * page_size, (page + 1) * page_size - 1)\
                    .execute()
                
                remaining_count += len(remaining.data)
                
                if len(remaining.data) < page_size:
                    break
                    
                page += 1
                time.sleep(0.5)
            
            if remaining_count > 0:
                raise ValueError(f"Found {remaining_count} remaining transactions")
            
            self.logger.info("Successfully deleted all transactions")
            
        except Exception as e:
            self.logger.error(f"Error deleting transactions: {str(e)}")
            raise
    
    def delete_user_transactions_and_categories(self):
        """Delete all transactions and categories for the configured user"""
        try:
            # First get all bank IDs for the user
            bank_result = self.supabase.table("banks")\
                .select("id")\
                .eq("user_id", self.user_id)\
                .execute()
            
            if not bank_result.data:
                self.logger.info(f"No banks found for user {self.user_id}")
                return True
                
            bank_ids = [row["id"] for row in bank_result.data]
            
            # Get all account IDs for these banks
            account_result = self.supabase.table("accounts")\
                .select("id")\
                .in_("bank_id", bank_ids)\
                .execute()
            
            if not account_result.data:
                self.logger.info(f"No accounts found for banks of user {self.user_id}")
                return True
                
            account_ids = [row["id"] for row in account_result.data]
            
            # Get all transaction IDs for these accounts
            transaction_result = self.supabase.table("transactions")\
                .select("id")\
                .in_("account_id", account_ids)\
                .execute()
            
            if not transaction_result.data:
                self.logger.info(f"No transactions found for accounts of user {self.user_id}")
                return True
                
            transaction_ids = [row["id"] for row in transaction_result.data]
            
            # Delete all transaction categories for these transactions
            self.logger.info("Deleting transaction categories...")
            self.supabase.table("transaction_categories")\
                .delete()\
                .in_("transaction_id", transaction_ids)\
                .execute()
            
            # Delete all transactions for these accounts
            self.logger.info("Deleting transactions...")
            self.supabase.table("transactions")\
                .delete()\
                .in_("account_id", account_ids)\
                .execute()
            
            self.logger.info(f"Successfully deleted all transactions and categories for user {self.user_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting user transactions and categories: {str(e)}")
            raise
