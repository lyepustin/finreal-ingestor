from datetime import datetime, timedelta
import logging
from typing import Optional
from scrapers.ruralvia_scraper import RuralviaScraper
import os
import csv
import re
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename by removing invalid characters
    
    Args:
        filename (str): Original filename
        
    Returns:
        str: Sanitized filename
    """
    # Replace asterisks with 'x'
    filename = filename.replace('*', 'x')
    
    # Remove any other invalid characters
    invalid_chars = r'[<>:"/\\|?]'
    return re.sub(invalid_chars, '', filename)

def save_transactions_to_csv(accounts, output_dir: str = "data/exports") -> None:
    """
    Save account transactions to CSV files
    
    Args:
        accounts (list): List of account dictionaries containing transactions
        output_dir (str): Directory to save CSV files
    """
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for account in accounts:
        account_name = account['name'].replace(" ", "_").lower()
        account_number = account['account_number'].replace(" ", "")
        if len(account_number) > 10:  # Truncate long account numbers
            account_number = account_number[-10:]
            
        # Sanitize the filename
        raw_filename = f"{timestamp}_ruralvia_{account_name}_{account_number}.csv"
        filename = sanitize_filename(raw_filename)
        filepath = os.path.join(output_dir, filename)
        
        transactions = account.get('transactions', [])
        if not transactions:
            continue
            
        # Define CSV headers based on available transaction data
        headers = ['date', 'description', 'category', 'amount']
        if 'balance' in transactions[0]:
            headers.append('balance')
            
        logger.info(f"Saving {len(transactions)} transactions to {filename}")
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for trans in sorted(transactions, key=lambda x: x['date'], reverse=True):
                row = {
                    'date': trans['date'].strftime('%Y-%m-%d %H:%M:%S'),
                    'description': trans['description'],
                    'category': trans['category'],
                    'amount': trans['amount']
                }
                if 'balance' in headers:
                    row['balance'] = trans.get('balance', '')
                    
                writer.writerow(row)
        
        logger.info(f"Successfully saved transactions to {filepath}")

def run_scraper(start_date: Optional[datetime] = None) -> bool:
    """
    Run the Ruralvia scraper to fetch accounts and transactions
    
    Args:
        start_date (datetime, optional): Start date for transaction fetch. 
            Defaults to 30 days ago if not provided.
    
    Returns:
        bool: True if scraping was successful, False otherwise
    """
    try:
        # If no start date provided, default to 30 days ago
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
            
        logger.info(f"Starting Ruralvia scraper from date: {start_date}")
        debugger_address = os.getenv("DEBUGGER_ADDRESS")
        
        with RuralviaScraper(debugger_address) as scraper:
            
            # Attempt login
            if not scraper.login():
                logger.error("Failed to login to Ruralvia")
                return False
            
            # Get accounts and their transactions
            accounts = scraper.get_accounts()
            if not accounts:
                logger.warning("No accounts found")
                return False
            
            logger.info(f"Found {len(accounts)} accounts")
            
            # Save transactions to CSV files
            save_transactions_to_csv(accounts)
            
            # Print account information
            for i, account in enumerate(accounts, 1):
                print(f"\n{'='*100}")
                print(f"Account {i}: {account['name']}")
                print(f"{'='*100}")
                print(f"Number: {account['account_number']}")
                print(f"Type: {account['type'].value}")
                print(f"Balance: {account['balance']}€")
                
                transactions = account.get('transactions', [])
                if transactions:
                    print(f"\nTransactions ({len(transactions)}):")
                    print(f"{'-'*100}")
                    print(f"{'Date':<20} {'Description':<40} {'Category':<20} {'Amount':>10} {'Balance':>12}")
                    print(f"{'-'*100}")
                    
                    for trans in sorted(transactions, key=lambda x: x['date'], reverse=True):
                        date_str = trans['date'].strftime('%Y-%m-%d %H:%M:%S')
                        desc = trans['description'][:37] + '...' if len(trans['description']) > 37 else trans['description']
                        category = trans['category'][:17] + '...' if len(trans['category']) > 17 else trans['category']
                        amount = f"{trans['amount']:,.2f}€"
                        balance = f"{trans.get('balance', 0):,.2f}€" if 'balance' in trans else ''
                        
                        print(f"{date_str:<20} {desc:<40} {category:<20} {amount:>10} {balance:>12}")
                else:
                    print("\nNo transactions found for this account")
                
                print(f"\n{'='*100}")
        
        logger.info("Scraping completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")
        logger.exception("Detailed error:")
        return False

def main():
    """Main entry point"""
    try:
        # Example: Get transactions from the last 60 days
        start_date = datetime.now() - timedelta(days=60)
        success = run_scraper(start_date)
        
        if success:
            print("\nScraping completed successfully!")
        else:
            print("\nScraping failed. Check the logs for details.")
            
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")

if __name__ == "__main__":
    main() 