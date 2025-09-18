from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver import Remote
from selenium.common.exceptions import TimeoutException
from datetime import datetime
import pandas as pd
import logging
import sys
import time
import os
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class AccountType(str, Enum):
    BANK_ACCOUNT = "BANK_ACCOUNT"
    VIRTUAL_CARD = "VIRTUAL_CARD"

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class RuralviaScraper:
    def __init__(self, debugger_address: str = None):
        logger.info("Initializing RuralviaScraper")
        self.base_url = str(os.getenv("RURALVIA_BASE_URL"))
        self.username = str(os.getenv("RURALVIA_USERNAME"))
        self.password = str(os.getenv("RURALVIA_PASSWORD"))
        self.driver = None
        self.debugger_address = debugger_address # Store debugger address

    def setup_driver(self):
        """Initialize the Edge WebDriver with appropriate options"""
        logger.info("Setting up Edge WebDriver")
        edge_options = Options()
        edge_options.add_argument("--start-maximized")  # Start with maximized window
        edge_options.use_chromium = True
        
        # Use local Edge driver
        edge_driver_path = os.path.join(os.path.dirname(__file__), "..", "edge_driver", "msedgedriver.exe")
        service = Service(executable_path=edge_driver_path)
        
        if self.debugger_address:
            logger.info(f"Connecting to existing Edge instance at {self.debugger_address}")
            # This is the key change - use debuggerAddress in experimental options instead of Remote
            edge_options.add_experimental_option("debuggerAddress", self.debugger_address)
            self.driver = webdriver.Edge(service=service, options=edge_options)
        else:
            logger.info("Creating new Edge WebDriver instance")
            self.driver = webdriver.Edge(service=service, options=edge_options)
        
        # Wait for the body to be present
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        logger.info("Edge WebDriver setup completed")

    def login(self) -> bool:
        """Login to Ruralvia banking portal"""
        try:
            self.driver.switch_to.new_window('tab')
            
            # Create a new tab and switch to it
            logger.info("Creating new tab")
            self.driver.switch_to.new_window('tab')
            
            # Get the current window handle (this will be our new tab)
            current_handle = self.driver.current_window_handle
            logger.info(f"New tab created with handle: {current_handle}")
            
            logger.info(f"Navigating to {self.base_url}")
            self.driver.get(self.base_url)

            logger.info("Looking for username field")
            username_field = self.driver.find_element(By.NAME, "dniNie")
            logger.info("Looking for password field")
            password_field = self.driver.find_element(By.NAME, "Alias")

            logger.info("Entering username")
            username_field.send_keys(self.username)
            WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.NAME, "Alias"))
            )
            logger.info("Entering password")
            password_field.send_keys(self.password)

            # login_button = self.driver.find_element(By.ID, "login-button")
            login_button = self.driver.find_element(
                By.XPATH, "//button[@data-qa='login--button--volver']"
            )
            logger.info("Clicking login button")
            login_button.click()

            logger.info("Waiting for dashboard to load")
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "nbe-web-view-dashboard"))
            )

            logger.info("Login successful")
            return True

        except TimeoutException:
            logger.error("Login timeout - check credentials or website availability")
            return False
        except Exception as e:
            logger.error(f"Login failed with error: {str(e)}")
            return False

    def get_accounts(self) -> List[Dict]:
        """Fetch list of accounts and cards with their transactions"""
        accounts = []
        try:
            # Get bank accounts
            bank_accounts = self.driver.find_elements(By.CSS_SELECTOR, '[data-qa="global-accounts-cards--table--mis-cuentas"]')
            
            for account in bank_accounts:
                # Get account name and button reference
                account_button = account.find_element(By.CLASS_NAME, "nbe-web-view-global-accounts-cards__button")
                account_name = account_button.text
                
                # Get IBAN
                iban = account.find_element(By.CLASS_NAME, "text-style--primary-light-normal").text
                
                # Get balance information
                balance_element = account.find_element(By.CLASS_NAME, "rsi-ui-money--default")
                balance = balance_element.text.replace('\n', '').replace('€', '').strip()
                
                # Click on account to get transactions
                account_button.click()
                
                # Get transactions for this account
                transactions = self._get_bank_account_transactions()
                
                account_data = {
                    "name": account_name,
                    "account_number": iban,
                    "balance": balance,
                    "type": AccountType.BANK_ACCOUNT,
                    "transactions": transactions
                }
                accounts.append(account_data)
                
                # Go back to main page
                self.driver.back()
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "nbe-web-view-global-accounts-cards"))
                )

            # Get cards information
            cards = self.driver.find_elements(By.CSS_SELECTOR, '[data-qa="global-accounts-cards--table--mis-tarjetas"]')

            for card in cards:
                # Get card name and button reference
                card_button = card.find_element(By.CLASS_NAME, "nbe-web-view-global-accounts-cards__button")
                card_name = card_button.text
                
                # Check if it's a virtual card
                is_virtual = "VIRTUAL" in card_name.upper()
                if not is_virtual:
                    # Skip debit cards as their transactions are already in bank account
                    continue
                    
                card_number = card.find_element(By.CSS_SELECTOR, ".text-style--primary-light-normal .lowercase").text.strip()
                
                # Try to get available balance where available
                try:
                    balance_element = card.find_element(By.CSS_SELECTOR, ".rsi-ui-money--default")
                    card_balance = balance_element.text.replace('\n', '').replace('€', '').strip()
                except:
                    card_balance = "0,00"
                
                # Click on card to get transactions
                card_button.click()
                
                # Get transactions for this card
                transactions = self._get_virtual_card_transactions()
                
                account_data = {
                    "name": card_name,
                    "account_number": card_number,
                    "balance": card_balance,
                    "type": AccountType.VIRTUAL_CARD,
                    "transactions": transactions
                }
                accounts.append(account_data)
                
                # Go back to main page
                self.driver.back()
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "nbe-web-view-global-accounts-cards"))
                )

        except Exception as e:
            logger.error(f"Failed to fetch accounts: {str(e)}")
            logger.exception("Detailed error:")

        return accounts

    def _get_bank_account_transactions(self) -> List[Dict]:
        """Fetch transactions specifically for bank accounts"""
        transactions = []
        try:
            # Wait for transactions to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "nbe-web-movement"))
            )

            # Get all transaction elements - both buttons and divs
            transaction_elements = self.driver.find_elements(By.CSS_SELECTOR, '.nbe-web-movement')

            # Keep track of seen transactions to avoid duplicates
            seen_transactions = set()

            for element in transaction_elements:
                try:
                    # Get the wrapper element that contains the transaction info
                    wrapper = element.find_element(By.CLASS_NAME, "nbe-web-movement__wrapper")
                    
                    # Get date
                    date_str = wrapper.find_element(By.CLASS_NAME, "nbe-web-movement__time").get_attribute("datetime")
                    
                    # Get description
                    description = wrapper.find_element(By.CLASS_NAME, "nbe-web-movement__title").text
                    category = wrapper.find_element(By.CLASS_NAME, "nbe-web-movement__info").text
                    
                    # Get amount and balance
                    amount_element = wrapper.find_element(By.CSS_SELECTOR, '[data-qa="account-movement-list--money--cantidad-movimiento"]')
                    balance_element = wrapper.find_element(By.CSS_SELECTOR, '[data-qa="account-movement-list--money--cantidad-cuenta"]')
                    
                    # Clean up amount and balance (remove € symbol and convert , to .)
                    def clean_number(text: str) -> float:
                        # Remove € symbol, spaces, and newlines
                        cleaned = text.replace('€', '').replace(' ', '').replace('\n', '')
                        # Handle thousands separator and decimal point
                        cleaned = cleaned.replace('.', '').replace(',', '.')
                        return float(cleaned)
                    
                    amount = clean_number(amount_element.text)
                    balance = clean_number(balance_element.text)
                    
                    # Create a unique key for this transaction
                    transaction_key = f"{date_str}_{description}_{amount}"
                    
                    # Only add if we haven't seen this exact transaction before
                    if transaction_key not in seen_transactions:
                        transaction_data = {
                            "date": datetime.strptime(date_str, "%Y-%m-%d"),
                            "value_date": datetime.strptime(date_str, "%Y-%m-%d"),
                            "description": description,
                            "category": category,
                            "amount": amount,
                            "balance": balance
                        }
                        transactions.append(transaction_data)
                        seen_transactions.add(transaction_key)
                except Exception as e:
                    logger.warning(f"Failed to process a bank transaction: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Failed to fetch bank transactions: {str(e)}")
            logger.exception("Detailed error:")

        return transactions

    def _get_virtual_card_transactions(self) -> List[Dict]:
        """Fetch transactions specifically for virtual cards"""
        transactions = []
        try:
            # Wait for transactions to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "nbe-web-movement"))
            )

            # Get all transaction elements
            transaction_elements = self.driver.find_elements(By.CSS_SELECTOR, '.nbe-web-movement')

            # Keep track of seen transactions to avoid duplicates
            seen_transactions = set()

            for element in transaction_elements:
                try:
                    # Get the wrapper element that contains the transaction info
                    wrapper = element.find_element(By.CLASS_NAME, "nbe-web-movement__wrapper")
                    
                    # Get date
                    date_str = wrapper.find_element(By.CLASS_NAME, "nbe-web-movement__time").get_attribute("datetime")
                    
                    # Get description and category
                    description = wrapper.find_element(By.CLASS_NAME, "nbe-web-movement__title").text
                    category = wrapper.find_element(By.CLASS_NAME, "nbe-web-movement__info").text
                    
                    # Get amount
                    amount_element = wrapper.find_element(By.CLASS_NAME, "rsi-ui-money")
                    
                    # Clean up amount (remove € symbol and convert , to .)
                    def clean_number(text: str) -> float:
                        # Remove € symbol, spaces, and newlines
                        cleaned = text.replace('€', '').replace(' ', '').replace('\n', '')
                        # Handle thousands separator and decimal point
                        cleaned = cleaned.replace('.', '').replace(',', '.')
                        return float(cleaned)
                    
                    amount = clean_number(amount_element.text)
                    
                    # Create a unique key for this transaction
                    transaction_key = f"{date_str}_{description}_{amount}"
                    
                    # Only add if we haven't seen this exact transaction before
                    if transaction_key not in seen_transactions:
                        transaction_data = {
                            "date": datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S"),
                            "value_date": datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S"),
                            "description": description,
                            "category": category,
                            "amount": amount
                        }
                        transactions.append(transaction_data)
                        seen_transactions.add(transaction_key)
                except Exception as e:
                    logger.warning(f"Failed to process a card transaction: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Failed to fetch card transactions: {str(e)}")
            logger.exception("Detailed error:")

        return transactions

    def get_transactions(
        self, account_data: Dict, start_date: Optional[datetime] = None
    ) -> List[Dict]:
        """Fetch transactions for a specific account or card"""
        if account_data["type"] == AccountType.BANK_ACCOUNT:
            return self._get_bank_account_transactions()
        elif account_data["type"] == AccountType.VIRTUAL_CARD:
            return self._get_virtual_card_transactions()
        else:
            logger.error(f"Unknown account type: {account_data['type']}")
            return []

    def close(self):
        """Close the browser session"""
        if self.driver:
            self.driver.quit()

    def __enter__(self):
        self.setup_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
