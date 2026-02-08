from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import TimeoutException
import logging
import os
from typing import Optional, List, Dict, Any
import time
import json
import websocket
import requests
from threading import Event
from dotenv import load_dotenv
from dataclasses import dataclass
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

@dataclass
class Transaction:
    date: datetime
    description: str
    category: str
    amount: float
    source: str = "unknown"  # Added source field to distinguish between bank and virtual card
    balance: float = 0.0  # Added balance field for bank account transactions
    more_info: str = ""  # Added more info field for extended description

@dataclass
class AccountBalance:
    account_number: str
    account_type: str
    available_balance: float
    current_balance: float
    currency: str = "EUR"

@dataclass
class CardInfo:
    card_number: str
    alias: str
    type: str
    status: str
    available_balance: float
    currency: str = "EUR"

class ResponseHandler:
    """Base class for handling different types of responses"""
    def __init__(self):
        self.data = []

    def process(self, response_data: Dict[str, Any]) -> List[Any]:
        """Process the response data and return structured information"""
        raise NotImplementedError

class VirtualCardTransactionHandler(ResponseHandler):
    """Handler for virtual card transaction responses"""
    def process(self, response_data: Dict[str, Any]) -> List[Transaction]:
        transactions = []
        try:
            for tx in response_data.get("cardsTransactions", []):
                try:
                    # Get category name or use default if missing
                    category = "Uncategorized"
                    if "humanCategory" in tx and tx["humanCategory"] is not None:
                        category = tx["humanCategory"].get("name", "Uncategorized")
                    
                    # Get shop name or default value
                    description = "Unknown transaction"
                    if "shop" in tx and tx["shop"] is not None:
                        description = tx["shop"].get("name", "Unknown shop").title()
                    
                    # Parse transactionDate with full time precision
                    # Example format: "2025-03-11T00:00:00.000+0100"
                    if "transactionDate" not in tx or not tx["transactionDate"]:
                        logger.warning(f"Missing transactionDate in transaction: {tx}")
                        continue  # Skip transactions without a date
                    
                    # Replace Z with +00:00 if present, otherwise keep the timezone info
                    date_str = tx["transactionDate"]
                    if date_str.endswith('Z'):
                        date_str = date_str[:-1] + '+00:00'
                    
                    # Handle different timezone formats
                    if '+' in date_str and len(date_str.split('+')[1]) == 4:
                        # Convert +0100 format to +01:00 format
                        timezone_part = date_str.split('+')[1]
                        hours = timezone_part[:2]
                        minutes = timezone_part[2:]
                        date_str = date_str.split('+')[0] + '+' + hours + ':' + minutes
                    
                    transaction_date = datetime.fromisoformat(date_str)
                    
                    # Get amount safely
                    amount = 0.0
                    if "amount" in tx and tx["amount"] is not None:
                        amount = float(tx["amount"].get("amount", 0.0))
                    
                    transaction = Transaction(
                        date=transaction_date,
                        description=description,
                        category=category,
                        amount=amount,
                        source="virtual_card"
                    )
                    transactions.append(transaction)
                    
                except Exception as e:
                    logger.warning(f"Failed to process virtual card transaction: {str(e)}")
                    logger.debug(f"Problematic transaction data: {tx}")
                    continue
        except Exception as e:
            logger.error(f"Error processing virtual card transactions: {str(e)}")
        return transactions

class BankAccountTransactionHandler(ResponseHandler):
    """Handler for bank account transaction responses"""
    def process(self, response_data: Dict[str, Any]) -> List[Transaction]:
        transactions = []
        try:
            for tx in response_data.get("accountTransactions", []):
                try:
                    # Check if this is an account balance entry rather than a transaction
                    # Account balance entries have 'contract' and 'account' but no transaction data
                    if "contract" in tx and "account" in tx and "valueDate" not in tx:
                        # This is an account balance entry, not a transaction - silently skip
                        continue
                    
                    # Parse valueDate with full time precision
                    # Example format: "2025-03-11T00:00:00.000+0100"
                    if "valueDate" not in tx or not tx["valueDate"]:
                        logger.warning(f"Missing valueDate in what appears to be a transaction: {tx}")
                        continue  # Skip transactions without a date
                    
                    # Replace Z with +00:00 if present, otherwise keep the timezone info
                    date_str = tx["valueDate"]
                    if date_str.endswith('Z'):
                        date_str = date_str[:-1] + '+00:00'
                    
                    # Handle different timezone formats
                    if '+' in date_str and len(date_str.split('+')[1]) == 4:
                        # Convert +0100 format to +01:00 format
                        timezone_part = date_str.split('+')[1]
                        hours = timezone_part[:2]
                        minutes = timezone_part[2:]
                        date_str = date_str.split('+')[0] + '+' + hours + ':' + minutes
                    
                    transaction_date = datetime.fromisoformat(date_str)
                    
                    # Get description safely
                    description = tx.get("humanConceptName", "").strip()
                    if not description:
                        description = "Unknown transaction"
                    
                    # Get category name or use default if missing
                    category = "Uncategorized"
                    if "humanCategory" in tx and tx["humanCategory"] is not None:
                        category = tx["humanCategory"].get("name", "Uncategorized")
                    
                    # Get amount safely
                    amount = 0.0
                    if "amount" in tx and tx["amount"] is not None:
                        amount = float(tx["amount"].get("amount", 0.0))
                    
                    # Get balance safely
                    balance = 0.0
                    if "balance" in tx and tx["balance"] is not None:
                        balance_obj = tx["balance"].get("accountingBalance", {})
                        if balance_obj and "amount" in balance_obj:
                            balance = float(balance_obj.get("amount", 0.0))
                    
                    # Get more info safely
                    more_info = tx.get("humanExtendedConceptName", "").strip()
                    
                    transaction = Transaction(
                        date=transaction_date,
                        description=description,
                        category=category,
                        amount=amount,
                        source="bank_account",
                        balance=balance,
                        more_info=more_info
                    )
                    transactions.append(transaction)
                except Exception as e:
                    logger.warning(f"Failed to process bank account transaction: {e}")
                    logger.debug(f"Problematic transaction data: {tx}")
                    continue
        except Exception as e:
            logger.error(f"Error processing bank account transactions: {e}")
        return transactions

class FinancialOverviewHandler(ResponseHandler):
    """Handler for financial overview responses"""
    def process(self, response_data: Dict[str, Any]) -> Dict[str, List[Any]]:
        result = {
            "accounts": [],
            "cards": []
        }
        try:
            for contract in response_data.get("data", {}).get("contracts", []):               
                if contract.get("productType") == "ACCOUNT":
                    try:
                        account = AccountBalance(
                            account_number=contract.get("number", ""),
                            account_type=contract.get("product", {}).get("name", ""),
                            available_balance=float(contract.get("detail", {}).get("specificAmounts", [{}])[0].get("amounts", [{}])[0].get("amount", 0)),
                            current_balance=float(contract.get("detail", {}).get("specificAmounts", [{}])[1].get("amounts", [{}])[0].get("amount", 0))
                        )
                        result["accounts"].append(account)
                    except Exception as e:
                        logger.warning(f"Failed to process account: {str(e)}")
                        continue

                # Process cards
                elif contract.get("productType") == "CARD" and contract.get("product", {}).get("name", "") == "TARJETAS VIRTUALES":
                    try:
                        card = CardInfo(
                            card_number=contract.get("number", ""),
                            alias=contract.get("alias", ""),
                            type=contract.get("product", {}).get("name", ""),
                            status=contract.get("detail", {}).get("status", {}).get("id", ""),
                            available_balance=float(contract.get("detail", {}).get("specificAmounts", [{}])[0].get("amounts", [{}])[0].get("amount", 0))
                        )
                        result["cards"].append(card)
                    except Exception as e:
                        logger.warning(f"Failed to process card: {str(e)}")
                        continue

        except Exception as e:
            logger.error(f"Error processing financial overview: {str(e)}")
        
        return result

class BBVAScraperImproved:
    def __init__(self, debugger_address: Optional[str] = None):
        logger.info("Initializing BBVAScraperImproved")
        self.base_url = str(os.getenv("BBVA_BASE_URL"))
        self.username = str(os.getenv("BBVA_USERNAME"))
        self.password = str(os.getenv("BBVA_PASSWORD"))
        self.driver = None
        self.debugger_address = debugger_address
        self.ws = None
        self.ws_ready = Event()
        self.network_enabled = False
        
        # Initialize response handlers
        self.response_handlers = {
            "listIntegratedCardTransactions": VirtualCardTransactionHandler(),
            "financial-overview": FinancialOverviewHandler(),
            "accountTransactions": BankAccountTransactionHandler()
        }
        
        # Store captured data
        self.virtual_card_transactions: List[Transaction] = []
        self.bank_account_transactions: List[Transaction] = []
        self.financial_overview: Dict[str, List[Any]] = {
            "accounts": [],
            "cards": []
        }

    def _on_ws_message(self, ws, message):
        """Handle WebSocket messages"""
        try:
            msg = json.loads(message)
            
            # Check if Network.enable was successful
            if msg.get("id") == 1 and "error" not in msg:
                self.network_enabled = True
                self.ws_ready.set()
                logger.info("WebSocket network monitoring ready")
                return
            
            # Handle response body
            if msg.get("id") == 999 and "result" in msg:
                try:
                    response_data = json.loads(msg["result"]["body"])
                    logger.debug(f"Received response data: {json.dumps(response_data, indent=2)}")
                    
                    # Determine response type and process accordingly
                    if "cardsTransactions" in response_data:
                        handler = self.response_handlers["listIntegratedCardTransactions"]
                        self.virtual_card_transactions = handler.process(response_data)
                        logger.info(f"Processed {len(self.virtual_card_transactions)} virtual card transactions")
                    
                    elif "accountTransactions" in response_data:
                        handler = self.response_handlers["accountTransactions"]
                        self.bank_account_transactions = handler.process(response_data)
                        logger.info(f"Processed {len(self.bank_account_transactions)} bank account transactions")
                    
                    elif "data" in response_data and "contracts" in response_data["data"]:
                        handler = self.response_handlers["financial-overview"]
                        self.financial_overview = handler.process(response_data)
                        logger.info(f"Processed financial overview with {len(self.financial_overview['accounts'])} accounts and {len(self.financial_overview['cards'])} cards")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse response data: {str(e)}")

            # Handle responses
            elif msg.get("method") == "Network.responseReceived":
                response = msg["params"]["response"]
                url = response.get("url", "")
                logger.debug(f"Received response for URL: {url}")
                
                # Check for different types of responses
                if "listIntegratedCardTransactions" in url:
                    logger.info("Detected virtual card transactions response")
                    request_id = msg["params"]["requestId"]
                    ws.send(json.dumps({
                        "id": 999,
                        "method": "Network.getResponseBody",
                        "params": {"requestId": request_id}
                    }))
                elif "accountTransactions" in url:
                    logger.info("Detected bank account transactions response")
                    request_id = msg["params"]["requestId"]
                    ws.send(json.dumps({
                        "id": 999,
                        "method": "Network.getResponseBody",
                        "params": {"requestId": request_id}
                    }))
                elif "financial-overview" in url:
                    logger.info("Detected financial overview response")
                    request_id = msg["params"]["requestId"]
                    ws.send(json.dumps({
                        "id": 999,
                        "method": "Network.getResponseBody",
                        "params": {"requestId": request_id}
                    }))
                else:
                    logger.debug(f"Received response for URL: {url}")

            # Handle request will be sent
            elif msg.get("method") == "Network.requestWillBeSent":
                request = msg["params"]["request"]
                url = request.get("url", "")
                logger.debug(f"Request will be sent to: {url}")

        except Exception as e:
            logger.error(f"Error in WebSocket message handler: {str(e)}")
            logger.error(f"Message content: {message}")

    def _on_ws_open(self, ws):
        """Handle WebSocket connection open"""
        logger.info("WebSocket connected, enabling network monitoring...")
        ws.send(json.dumps({"id": 1, "method": "Network.enable"}))
        # Also enable request interception
        ws.send(json.dumps({"id": 2, "method": "Network.setBypassServiceWorker", "params": {"bypass": True}}))

    def setup_driver(self):
        """Initialize the WebDriver and WebSocket connection"""
        if self.debugger_address:
            logger.info("Setting up Chrome WebDriver (connecting to existing browser)")
            chrome_options = ChromeOptions()
            chrome_options.add_argument("--start-maximized")
            chrome_options.add_argument("--remote-allow-origins=*")
            chrome_options.add_experimental_option("debuggerAddress", self.debugger_address)
            self.driver = webdriver.Chrome(options=chrome_options)
            logger.info(f"Connected to existing browser at {self.debugger_address}")
        else:
            logger.info("Setting up Edge WebDriver")
            edge_options = EdgeOptions()
            edge_options.add_argument("--start-maximized")
            edge_options.use_chromium = True
            self.driver = webdriver.Edge(options=edge_options)
        
        # Crear nueva pestaña y navegar al portal (igual que Caixa)
        logger.info("Creating new tab")
        self.driver.switch_to.new_window('tab')
        current_handle = self.driver.current_window_handle
        logger.info(f"New tab created with handle: {current_handle}")
        logger.info(f"Navigating to {self.base_url}")
        self.driver.get(self.base_url)

        # Initialize WebSocket connection
        try:
            debugger_url = f"http://{self.debugger_address}/json" if self.debugger_address else "http://localhost:59222/json"
            targets = requests.get(debugger_url).json()
            
            # Find the page/tab that matches our current window handle
            target_page = None
            for target in targets:
                if target.get("id") == current_handle:
                    target_page = target
                    break
            
            if not target_page:
                logger.error(f"Could not find WebSocket debugger URL for tab handle: {current_handle}")
                raise Exception("Failed to find WebSocket debugger URL for new tab")
            
            ws_url = target_page["webSocketDebuggerUrl"]
            logger.info(f"Connecting WebSocket to: {ws_url}")
            
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_message=self._on_ws_message,
                on_open=self._on_ws_open
            )
            
            # Start WebSocket in a separate thread
            import threading
            ws_thread = threading.Thread(target=self.ws.run_forever)
            ws_thread.daemon = True
            ws_thread.start()
            
            # Wait for WebSocket to be ready
            if not self.ws_ready.wait(timeout=10):
                logger.warning("WebSocket initialization timed out")
            
        except Exception as e:
            logger.error(f"Failed to initialize WebSocket: {str(e)}")
            raise  # Re-raise the exception to handle it in the calling code
        
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        logger.info("WebDriver and WebSocket setup completed")

    def get_virtual_card_transactions(self) -> List[Transaction]:
        """Get the list of virtual card transactions captured by the WebSocket"""
        return self.virtual_card_transactions

    def get_bank_account_transactions(self) -> List[Transaction]:
        """Get the list of bank account transactions captured by the WebSocket"""
        return self.bank_account_transactions

    def get_financial_overview(self) -> Dict[str, List[Any]]:
        """Get the financial overview data"""
        return self.financial_overview

    def clear_data(self):
        """Clear all captured data"""
        self.virtual_card_transactions = []
        self.bank_account_transactions = []
        self.financial_overview = {
            "accounts": [],
            "cards": []
        }

    def close(self):
        """Close the browser session and WebSocket connection"""
        if self.ws:
            self.ws.close()
        if self.driver:
            self.driver.quit()

    def export_transactions_to_csv(self) -> bool:
        """Export transactions to CSV files for each account and virtual card"""
        try:
            logger.info("Starting CSV export process")
            
            # Create data/exports directory if it doesn't exist
            os.makedirs("data/exports", exist_ok=True)
            
            # Get current timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Process each account's transactions
            for account in self.financial_overview["accounts"]:
                try:
                    # Create filename
                    account_name = "cuentas_personales"  # Default name
                    if "pau" in account.account_type.lower():
                        account_name = "pau"
                    
                    filename = f"{timestamp}_bbva_{account_name}_{account.account_number}.csv"
                    filepath = os.path.join("data/exports", filename)
                    
                    # Filter transactions for this account
                    account_transactions = [
                        tx for tx in self.bank_account_transactions
                        if tx.source == "bank_account"
                    ]
                    
                    # Sort transactions by date (newest first)
                    account_transactions.sort(key=lambda x: x.date, reverse=True)
                    
                    # Write to CSV
                    with open(filepath, 'w', newline='', encoding='utf-8') as f:
                        import csv
                        writer = csv.writer(f)
                        writer.writerow(['date', 'description', 'more_info', 'category', 'amount', 'balance'])
                        
                        for tx in account_transactions:
                            writer.writerow([
                                tx.date.strftime("%Y-%m-%d %H:%M:%S"),
                                tx.description,
                                tx.more_info,
                                tx.category,
                                tx.amount,
                                tx.balance
                            ])
                    
                    logger.info(f"Successfully exported transactions to {filename}")
                    
                except Exception as e:
                    logger.error(f"Failed to export transactions for account {account.account_number}: {str(e)}")
                    continue

            # Process virtual card transactions
            for card in self.financial_overview["cards"]:
                try:
                    # Create filename for virtual card
                    filename = f"{timestamp}_bbva_virtual_card_{card.card_number}.csv"
                    filepath = os.path.join("data/exports", filename)
                    
                    # Filter transactions for this virtual card
                    card_transactions = [
                        tx for tx in self.virtual_card_transactions
                        if tx.source == "virtual_card"
                    ]
                    
                    # Sort transactions by date (newest first)
                    card_transactions.sort(key=lambda x: x.date, reverse=True)
                    
                    # Write to CSV
                    with open(filepath, 'w', newline='', encoding='utf-8') as f:
                        import csv
                        writer = csv.writer(f)
                        writer.writerow(['date', 'description', 'category', 'amount'])
                        
                        for tx in card_transactions:
                            writer.writerow([
                                tx.date.strftime("%Y-%m-%d %H:%M:%S"),
                                tx.description,
                                tx.category,
                                tx.amount
                            ])
                    
                    logger.info(f"Successfully exported virtual card transactions to {filename}")
                    
                except Exception as e:
                    logger.error(f"Failed to export transactions for virtual card {card.card_number}: {str(e)}")
                    continue
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to export transactions to CSV: {str(e)}")
            return False

    def __enter__(self):
        self.setup_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def click_accounts_overview(self) -> bool:
        """Navigate to accounts and cards overview page"""
        try:
            logger.info("Navigating to accounts overview page")
            self.driver.get("https://web.bbva.es/index.html#subhome-cuentas-tarjetas")
            
            # Wait for the accounts section to load
            logger.info("Waiting for accounts section to load")
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "cuentasTarjetasProductos"))
            )
            
            logger.info("Successfully loaded accounts overview page")
            return True

        except Exception as e:
            logger.error(f"Failed to load accounts overview page: {str(e)}")
            return False

    def click_bank_transactions(self) -> bool:
        """Click on the first bank account row to view transactions"""
        try:
            logger.info("Waiting for first account row to be clickable")
            # Find the first account row
            account_row = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "tr.filaCuentasIban"))
            )
            
            logger.info("Found account row, attempting to click")
            # Try multiple click methods
            try:
                # Scroll into view first
                self.driver.execute_script("arguments[0].scrollIntoView(true);", account_row)
                time.sleep(0.5)
                
                # Try JavaScript click
                self.driver.execute_script("arguments[0].click();", account_row)
            except Exception as e:
                logger.warning(f"JavaScript click failed: {str(e)}, trying regular click")
                try:
                    account_row.click()
                except Exception as e:
                    logger.warning(f"Regular click failed: {str(e)}, trying action chains")
                    from selenium.webdriver.common.action_chains import ActionChains
                    actions = ActionChains(self.driver)
                    actions.move_to_element(account_row).click().perform()
            
            # Wait for the transactions page to load
            logger.info("Waiting for transactions page to load")
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "c-tablas-producto"))
            )
            
            logger.info("Successfully clicked account row")
            return True

        except Exception as e:
            logger.error(f"Failed to click account row: {str(e)}")
            return False

    def click_virtual_card_transactions(self) -> bool:
        """Click on the virtual card transactions row"""
        try:
            logger.info("Waiting for virtual card row to be clickable")
            # Find the virtual card row by looking for the text "TARJETAS VIRTUALES"
            virtual_card_row = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//span[contains(@class, 'nombreComercial') and contains(text(), 'TARJETAS VIRTUALES')]/ancestor::tr"))
            )
            
            logger.info("Found virtual card row, attempting to click")
            # Try multiple click methods
            try:
                # Scroll into view first
                self.driver.execute_script("arguments[0].scrollIntoView(true);", virtual_card_row)
                time.sleep(0.5)
                
                # Try JavaScript click
                self.driver.execute_script("arguments[0].click();", virtual_card_row)
            except Exception as e:
                logger.warning(f"JavaScript click failed: {str(e)}, trying regular click")
                try:
                    virtual_card_row.click()
                except Exception as e:
                    logger.warning(f"Regular click failed: {str(e)}, trying action chains")
                    from selenium.webdriver.common.action_chains import ActionChains
                    actions = ActionChains(self.driver)
                    actions.move_to_element(virtual_card_row).click().perform()
            
            # Wait for the transactions page to load
            logger.info("Waiting for transactions page to load")
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='cards-main-transactions']"))
            )
            
            logger.info("Successfully clicked virtual card row")
            return True

        except Exception as e:
            logger.error(f"Failed to click virtual card row: {str(e)}")
            return False

    def login(self) -> bool:
        """Login to BBVA banking portal (ya estamos en una pestaña con base_url desde setup_driver)"""
        try:
            # Wait for either username field or password-only form
            logger.info("Checking login form type")
            try:
                # First try to find the stored form (cookie-based login)
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='login-form-token']"))
                )
                logger.info("Cookie-based login form detected")
                cookie_based_login = True
            except TimeoutException:
                # If not found, look for the regular login form
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='login-form']"))
                )
                logger.info("Regular login form detected")
                cookie_based_login = False

            if not cookie_based_login:
                # Regular login flow - need both username and password
                logger.info("Looking for username field")
                username_field = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "input-user"))
                )
                logger.info("Entering username")
                username_field.clear()  # Clear any existing value
                username_field.send_keys(self.username)
                time.sleep(1)  # Small delay after username

            # Password field is needed in both cases
            logger.info("Looking for password field")
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "input-password"))
            )
            logger.info("Entering password")
            password_field.clear()  # Clear any existing value
            password_field.send_keys(self.password)
            time.sleep(1)  # Small delay after password

            # Click outside the password field to trigger blur event
            logger.info("Clicking outside password field")
            body = self.driver.find_element(By.TAG_NAME, "body")
            body.click()
            time.sleep(1)  # Wait for blur event to process

            # Find and wait for login button to be clickable
            logger.info("Waiting for login button to be clickable")
            # Try to find button by data-testid first
            try:
                login_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "span[data-testid='login-form-submit']"))
                )
            except TimeoutException:
                # If that fails, try finding by class and text content
                login_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//span[contains(@class, 'c-button--secondary') and .//span[contains(text(), 'Entrar')]]")
                    )
                )
            
            logger.info("Found Entrar button, attempting to click")
            time.sleep(1)  # Small delay before clicking
            
            # Try multiple click methods
            try:
                # Try moving to element first
                self.driver.execute_script("arguments[0].scrollIntoView(true);", login_button)
                time.sleep(0.5)
                
                # Try JavaScript click first
                logger.info("Attempting to click via JavaScript")
                self.driver.execute_script("arguments[0].click();", login_button)
            except Exception as e:
                logger.warning(f"JavaScript click failed: {str(e)}, trying regular click")
                time.sleep(1)
                try:
                    login_button.click()
                except Exception as e:
                    logger.warning(f"Regular click failed: {str(e)}, trying action chains")
                    # If regular click fails, try action chains
                    from selenium.webdriver.common.action_chains import ActionChains
                    actions = ActionChains(self.driver)
                    actions.move_to_element(login_button).click().perform()

            # Check for and handle security modal if it appears
            logger.info("Checking for security modal")
            try:
                modal = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='dialog']"))
                )
                logger.info("Security modal detected")
                
                # Find and click the "Entendido" button with wait
                logger.info("Waiting for 'Entendido' button")
                entendido_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "entendido"))
                )
                logger.info("Clicking 'Entendido' button")
                self.driver.execute_script("arguments[0].click();", entendido_button)
                time.sleep(1)  # Wait for modal to close
            except TimeoutException:
                logger.info("No security modal detected")
                pass

            # Wait for dashboard to load by checking for welcome message
            logger.info("Waiting for dashboard to load")
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "c-data-amount"))
            )

            logger.info("Login successful")
            return True

        except TimeoutException:
            logger.error("Login timeout - check credentials or website availability")
            return False
        except Exception as e:
            logger.error(f"Login failed with error: {str(e)}")
            return False 