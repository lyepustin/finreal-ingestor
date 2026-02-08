from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging
import os
from typing import Optional, List, Dict, Any
import time
import json
import websocket
import requests
from threading import Thread, Event
from dotenv import load_dotenv
from dataclasses import dataclass, asdict
from datetime import datetime
import traceback

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
    source: str = "unknown"
    balance: float = 0.0
    more_info: str = ""
    account: str = "Unknown"  # Added account field for CaixaBank's multiple account structure

@dataclass
class AccountBalance:
    account_number: str
    account_type: str
    available_balance: float
    current_balance: float
    currency: str = "EUR"

class CaixaScraper:
    def __init__(self, debugger_address: Optional[str] = None):
        logger.info("Initializing CaixaScraper")
        self.base_url = str(os.getenv("CAIXA_BASE_URL"))
        self.username = str(os.getenv("CAIXA_USERNAME"))
        self.password = str(os.getenv("CAIXA_PASSWORD"))
        self.ver_mas_pages = int(os.getenv("CAIXA_VER_MAS_PAGES", "2"))
        self.driver = None
        self.debugger_address = debugger_address
        self.ws = None
        self.ws_thread = None
        self.ws_ready = Event()
        self.network_enabled = False
        self.captured_responses = {}
        
        # Store extracted transactions
        self.transactions: List[Transaction] = []

    def setup_driver(self):
        if self.debugger_address:
            # Conectar a un navegador existente (Chrome/Chromium) vía remote debugging
            logger.info("Setting up Chrome WebDriver (connecting to existing browser)")
            chrome_options = ChromeOptions()
            chrome_options.add_argument("--remote-allow-origins=*")
            chrome_options.add_experimental_option("debuggerAddress", self.debugger_address)
            self.driver = webdriver.Chrome(options=chrome_options)
            logger.info(f"Connected to existing browser at {self.debugger_address}")
        else:
            # Iniciar nueva instancia de Edge
            logger.info("Setting up Edge WebDriver")
            edge_options = EdgeOptions()
            edge_options.use_chromium = True
            edge_options.add_argument("--remote-allow-origins=*")
            edge_options.add_experimental_option("detach", True)
            edge_options.add_argument("--remote-debugging-port=0")
            self.driver = webdriver.Edge(options=edge_options)
            self.debugger_address = self.driver.capabilities['ms:edgeOptions']['debuggerAddress']
            logger.info(f"Started new Edge instance with debugger address: {self.debugger_address}")

        # Create a new tab and switch to it
        logger.info("Creating new tab")
        self.driver.switch_to.new_window('tab')
        
        # Get the current window handle (this will be our new tab)
        current_handle = self.driver.current_window_handle
        logger.info(f"New tab created with handle: {current_handle}")
        
        # Navigate to CaixaBank in the new tab
        self.driver.get(self.base_url)
        logger.info("Switched to new tab and navigated to CaixaBank")

    def _on_ws_message(self, ws, message):
        try:
            msg = json.loads(message)
            if msg.get("id") == 1 and "result" in msg:
                self.network_enabled = True
                self.ws_ready.set()
                logger.info("WebSocket network monitoring ready")
                return

            if msg.get("method") == "Network.responseReceived":
                request_id = msg["params"]["requestId"]
                url = msg["params"]["response"]["url"]
                # TODO: Identify the correct URL for transactions and capture it
                if "my-finances" in url: # This is a placeholder
                    logger.info(f"Capturing response for URL: {url}")
                    self.captured_responses[request_id] = None # Placeholder for now

            if msg.get("id") in self.captured_responses and "result" in msg:
                request_id = msg["id"]
                self.captured_responses[request_id] = msg["result"]["body"]
                logger.info(f"Got response body for request {request_id}")

        except Exception as e:
            logger.error(f"Error in WebSocket message handler: {e}")

    def _on_ws_open(self, ws):
        logger.info("WebSocket connected, enabling network monitoring...")
        ws.send(json.dumps({"id": 1, "method": "Network.enable"}))

    def _start_ws_listener(self):
        # Find the correct target page
        res = requests.get(f"http://{self.debugger_address}/json/list")
        targets = res.json()
        target = next((t for t in targets if t.get('url') and 'caixabank' in t.get('url')), None)

        if not target:
            raise Exception("Could not find CaixaBank target page for WebSocket connection")

        ws_url = target['webSocketDebuggerUrl']
        logger.info(f"Connecting WebSocket to: {ws_url}")
        self.ws = websocket.WebSocketApp(ws_url, on_open=self._on_ws_open, on_message=self._on_ws_message)
        self.ws_thread = Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        self.ws_ready.wait(10) # Wait for network to be enabled

    def login(self):
        logger.info("Starting login process")
        try:
            wait = WebDriverWait(self.driver, 20)
            try:
                # Find username field
                user_field = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="lineaabierta-login"]')))
                user_field.send_keys(self.username)
                # Find password field
                # The recorded flow uses Tab, let's simulate that
                user_field.send_keys(webdriver.common.keys.Keys.TAB)
                time.sleep(0.5) # small delay
                pass_field = self.driver.switch_to.active_element
                pass_field.send_keys(self.password)
                # Find and click login button
                login_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="lolopo-template"]/div[2]/div[6]/form/div[3]/input')))
                login_button.click()
                logger.info("Login submitted")
                logger.info("Login successful")

            except TimeoutException:
                logger.error("Login elements not found. Page structure might have changed.")
                return False
        except Exception as e:
            logger.error(f"An error occurred during login: {e}")
            return False
        return True

    def navigate_to_finances(self):
        logger.info("Navigating to finances section")
        try:
            wait = WebDriverWait(self.driver, 20)

            # Step 1: Navigate to the outer iframe (third iframe on page)
            logger.info("Waiting for outer iframe...")
            outer_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[3]"))  # iframe index 2 -> third iframe
            )
            self.driver.switch_to.frame(outer_iframe)

            # Step 2: Navigate to the inner iframe (first iframe inside the outer)
            logger.info("Waiting for inner iframe...")
            inner_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[1]"))  # iframe index 0 inside parent
            )
            self.driver.switch_to.frame(inner_iframe)

            # Step 3: Click on "Cuentas y Tarjetas" first to navigate to the accounts section
            logger.info("Waiting for 'Cuentas y Tarjetas' link...")
            cuentas_y_tarjetas = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//*[@id='pestanya0']/a/span"))
            )
            logger.info("Clicking on 'Cuentas y Tarjetas'...")
            cuentas_y_tarjetas.click()
            
            # Step 4: Now we need to navigate to the Navbar iframe to click on "Mis finanzas"
            # First, go back to the main frame
            self.driver.switch_to.default_content()
            
            # Navigate to the outer iframe again (third iframe)
            logger.info("Re-navigating to outer iframe...")
            outer_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[3]"))
            )
            self.driver.switch_to.frame(outer_iframe)
            
            # Now look for the Navbar iframe (it should be visible after clicking Cuentas y Tarjetas)
            logger.info("Looking for Navbar iframe...")
            navbar_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "//iframe[@name='Navbar' or @id='Navbar']"))
            )
            self.driver.switch_to.frame(navbar_iframe)
            
            # Step 5: Click on "Mis finanzas"
            logger.info("Waiting for 'Mis finanzas' link...")
            mis_finanzas = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//*[@id='pestanya1']//span[text()='Mis Finanzas']"))
            )
            logger.info("Clicking on 'Mis finanzas'...")
            mis_finanzas.click()
            
            # Step 6: Navigate to the Cos iframe to access the dashboard content
            # First, go back to the main frame
            self.driver.switch_to.default_content()
            
            # Navigate to the outer iframe again (third iframe)
            logger.info("Re-navigating to outer iframe for Cos...")
            outer_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[3]"))
            )
            self.driver.switch_to.frame(outer_iframe)
            time.sleep(1)  # Give the page time to load
            # Now look for the Cos iframe
            logger.info("Looking for Cos iframe...")
            cos_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "//iframe[@name='Cos' or @title='Cuerpo']"))
            )
            self.driver.switch_to.frame(cos_iframe)
            
            # Step 7: Click on "Últimos movimientos"
            logger.info("Waiting for 'Últimos movimientos' section...")
            ultimos_movimientos = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[@id='ACTIVIDAD_titulo']//a[@class='general_dashboard__grid__item__link general_dashboard__grid__item__handle']"))
            )
            time.sleep(1)
            logger.info("Clicking on 'Últimos movimientos'...")
            ultimos_movimientos.click()
            
            logger.info("Successfully navigated to transactions page")
            time.sleep(1)

            # Step 8: Cambiar filtro Período de "Febrero" (mes actual) a "6 meses" para cargar más movimientos
            try:
                logger.info("Opening period filter (Período)...")
                period_filter_label = wait.until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'filter_title_label') and contains(.,'Período')]"))
                )
                period_data_big = period_filter_label.find_element(By.XPATH, "./following-sibling::div[contains(@class,'filter_title_data_big')]")
                self.driver.execute_script("arguments[0].scrollIntoView(true);", period_data_big)
                time.sleep(0.3)
                period_data_big.click()
                logger.info("Period filter opened, selecting '6 meses'...")
                seis_meses = wait.until(
                    EC.element_to_be_clickable((By.ID, "filtro_periodo_3"))
                )
                seis_meses.click()
                time.sleep(0.5)
                logger.info("Clicking 'Filtrar' to apply period filter...")
                filtrar_btn = wait.until(
                    EC.element_to_be_clickable((By.ID, "filtro_enlaceAceptar"))
                )
                filtrar_btn.click()
                time.sleep(2)  # Esperar a que la página recargue los movimientos con el nuevo período
                logger.info("Period set to 6 months and filter applied")
            except (TimeoutException, NoSuchElementException) as e:
                logger.warning(f"Could not set period filter to 6 months: {e}. Continuing with default period.")
            except Exception as e:
                logger.warning(f"Error setting period filter: {e}. Continuing with default period.")

            # Step 9: Click "Ver más movimientos" N veces para cargar más transacciones (N = CAIXA_VER_MAS_PAGES)
            logger.info("Looking for 'Ver más movimientos' button...")
            
            # Scroll down to ensure the button is visible
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            try:
                pagination_container = wait.until(
                    EC.presence_of_element_located((By.ID, "paginacionAcumulativa01"))
                )
                logger.info(f"Found pagination_container {pagination_container.text}")
                
                self.driver.switch_to.default_content()
                
                logger.info("Looking for Inferior iframe...")
                inferior_iframe = wait.until(
                    EC.presence_of_element_located((By.XPATH, "//iframe[@name='Inferior' or @id='Inferior' or @title='Inferior']"))
                )
                self.driver.switch_to.frame(inferior_iframe)
                logger.info("Switched to Inferior iframe")
                
                logger.info("Looking for Cos iframe inside Inferior...")
                cos_iframe = wait.until(
                    EC.presence_of_element_located((By.XPATH, "//iframe[@name='Cos' or @title='Cuerpo']"))
                )
                self.driver.switch_to.frame(cos_iframe)
                logger.info("Switched to Cos iframe")
                
                for page_num in range(self.ver_mas_pages):
                    try:
                        pagination_container = self.driver.find_element(By.ID, "paginacionAcumulativa01")
                        ver_mas_button = pagination_container.find_element(By.CLASS_NAME, "c-pagination__custom__pageListCumulative__inner__link")
                        logger.info(f"Clicking 'Ver más movimientos' ({page_num + 1}/{self.ver_mas_pages})...")
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", ver_mas_button)
                        ver_mas_button.click()
                        time.sleep(1)
                    except NoSuchElementException:
                        logger.warning("'Ver más movimientos' button not found - no more pages or structure changed")
                        break
                    except Exception as e:
                        logger.warning(f"Error clicking 'Ver más movimientos' (page {page_num + 1}): {e}")
                        break
                logger.info("Finished loading additional transaction pages")
                    
            except NoSuchElementException:
                logger.warning("Pagination container not found - continuing with available transactions")
            except Exception as e:
                logger.warning(f"Error setting up pagination: {e}")
            
            logger.info("Finished loading transactions, ready for extraction")

        except TimeoutException as e:
            logger.error(f"Timeout while navigating to finances: {e}")
        except Exception as e:
            logger.error(f"An error occurred during navigation: {e}")
            logger.error(f"Full traceback: {traceback.format_exc()}")

    def scrape(self):
        self.setup_driver()
        self._start_ws_listener()
        if self.login():
            logger.info("Login successful, navigating to finances...")
            self.navigate_to_finances()
            
            # Extract transactions from the page
            logger.info("Extracting transactions from the page...")
            self.transactions = self.extract_transactions_from_page()
            
            if self.transactions:
                logger.info(f"Successfully extracted {len(self.transactions)} transactions")
                
                # Export to CSV
                if self.export_transactions_to_csv():
                    logger.info("Transactions exported to CSV successfully")
                else:
                    logger.error("Failed to export transactions to CSV")
            else:
                logger.warning("No transactions were extracted")
        else:
            logger.error("Login failed, cannot proceed with scraping")
        self.teardown_driver()

    def __enter__(self):
        """Context manager entry"""
        self.setup_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.teardown_driver()

    def close(self):
        """Close the scraper and cleanup resources"""
        self.teardown_driver()

    def teardown_driver(self):
        if self.ws:
            self.ws.close()
        if self.driver:
            # self.driver.quit() # Uncomment if you want the browser to close automatically
            pass
        logger.info("Scraper finished.")

    def parse_date(self, date_text):
        """Parse Spanish date format to ISO datetime format"""
        # Clean the date text
        date_text = date_text.strip()
        
        # Handle "Hoy" (Today)
        if "Hoy" in date_text:
            today = datetime.now()
            return today.strftime("%Y-%m-%d 00:00:00")
        
        # Handle other Spanish date formats like "Sáb 28 Jun", "Vie 27 Jun", etc.
        # Extract day and month
        import re
        day_month_match = re.search(r'(\d{1,2})\s+(\w{3})', date_text)
        if day_month_match:
            day = day_month_match.group(1).zfill(2)
            month_abbr = day_month_match.group(2)
            
            # Spanish month abbreviations to numbers
            month_map = {
                'Ene': '01', 'Feb': '02', 'Mar': '03', 'Abr': '04',
                'May': '05', 'Jun': '06', 'Jul': '07', 'Ago': '08',
                'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dic': '12'
            }
            
            month = month_map.get(month_abbr, '01')
            year = str(datetime.now().year)  # Use current year
            
            return f"{year}-{month}-{day} 00:00:00"
        
        # If no pattern matches, return today's date
        today = datetime.now()
        return today.strftime("%Y-%m-%d 00:00:00")

    def parse_amount(self, amount_text):
        """Parse amount text and convert to float"""
        # Remove € symbol and strip whitespace
        amount_text = amount_text.replace('€', '').strip()
        
        # Handle negative amounts
        is_negative = amount_text.startswith('-')
        if is_negative:
            amount_text = amount_text[1:]
        
        # Handle European number format (thousands separator with dot, decimal with comma)
        # Examples: "3.335,15" -> 3335.15, "1.000,00" -> 1000.00, "26,46" -> 26.46
        if ',' in amount_text:
            # Split by comma to separate decimal part
            parts = amount_text.split(',')
            if len(parts) == 2:
                integer_part = parts[0].replace('.', '')  # Remove thousands separators
                decimal_part = parts[1]
                amount_text = f"{integer_part}.{decimal_part}"
            else:
                # If multiple commas, just remove dots (thousands separators) and replace last comma with dot
                amount_text = amount_text.replace('.', '').replace(',', '.')
        else:
            # No comma, but might have dots as thousands separators
            # If more than one dot or if dot is not in last 3 positions, treat as thousands separator
            dot_count = amount_text.count('.')
            if dot_count > 1:
                # Multiple dots - treat all as thousands separators
                amount_text = amount_text.replace('.', '')
            elif dot_count == 1:
                dot_pos = amount_text.rfind('.')
                # If dot is not in decimal position (last 1-3 chars), treat as thousands separator
                if len(amount_text) - dot_pos > 4:
                    amount_text = amount_text.replace('.', '')
        
        try:
            result = float(amount_text)
            return -result if is_negative else result
        except ValueError:
            logger.warning(f"Could not parse amount '{amount_text}'")
            return 0.0

    def extract_merchant_name(self, categoria_cell):
        """Extract merchant name from the categoria cell"""
        # Try to find text within span elements
        spans = categoria_cell.find_elements(By.TAG_NAME, 'span')
        for span in spans:
            try:
                span_classes = span.get_attribute('class') or ''
                if 'margin-right10' in span_classes:
                    text = span.text.strip()
                    if text and not text.startswith('tipomov'):
                        return text
            except:
                continue
        
        # Fallback: get all text and clean it
        try:
            text = categoria_cell.text.strip()
            # Remove extra whitespace and clean up
            import re
            text = re.sub(r'\s+', ' ', text)
            
            # Extract meaningful merchant name (usually the last meaningful part)
            lines = text.split('\n')
            for line in reversed(lines):
                line = line.strip()
                if line and not line.startswith('c-category') and len(line) > 2:
                    return line
            
            return text[:50] if text else "Unknown"
        except:
            return "Unknown"

    def extract_account_info(self, service_cell):
        """Extract account information from the service cell"""
        if not service_cell:
            return "Unknown"
        
        try:
            # Get the text content using Selenium's text property
            text = service_cell.text.strip()
            
            # Clean up HTML entities and extra whitespace
            text = text.replace('\xa0', ' ').replace('&nbsp;', ' ')
            import re
            text = re.sub(r'\s+', ' ', text).strip()
            
            # Extract account patterns
            # Pattern 1: "Cuenta ...1433" or "MyCard ...5246"
            account_match = re.search(r'(Cuenta|MyCard|CYBERTARJETA)\s*\.{0,3}\s*(\d+)', text)
            if account_match:
                account_type = account_match.group(1)
                account_number = account_match.group(2)
                return f"{account_type} {account_number}"
            
            # If no pattern matches, return cleaned text
            return text if text else "Unknown"
        except:
            return "Unknown"

    def categorize_transaction(self, merchant_name, amount):
        """Categorize transaction based on merchant name and amount"""
        merchant_lower = merchant_name.lower()
        
        # Income transactions
        if amount > 0:
            if 'nomina' in merchant_lower or 'salary' in merchant_lower:
                return 'nomina'
            elif 'bizum recibido' in merchant_lower:
                return 'bizum recibido'
            elif 'transf' in merchant_lower and 'favor' in merchant_lower:
                return 'transferencia recibida'
            else:
                return 'ingreso'
        
        # Expense transactions
        else:
            if any(word in merchant_lower for word in ['mercadona', 'alcampo', 'super']):
                return 'compra supermercado'
            elif any(word in merchant_lower for word in ['zara', 'primark', 'lefties', 'massimo']):
                return 'compra ropa'
            elif any(word in merchant_lower for word in ['ikea', 'leroy']):
                return 'compra hogar'
            elif 'bizum enviado' in merchant_lower:
                return 'bizum enviado'
            elif any(word in merchant_lower for word in ['restaurante', 'gelateria']):
                return 'restaurante'
            elif 'farmacia' in merchant_lower:
                return 'farmacia'
            else:
                return 'gasto varios'

    def extract_transactions_from_page(self):
        """Extract transactions from the current page"""
        logger.info("Starting transaction extraction...")
        
        transactions = []
        
        try:
            # Wait for the page to load
            wait = WebDriverWait(self.driver, 20)
            
            # Look for the transaction container (same approach as dev runner)
            logger.info("Looking for transaction container...")
            
            # Wait for the container to be present
            table_container = None
            try:
                table_container = wait.until(
                    EC.presence_of_element_located((By.ID, "divListaMovimientos"))
                )
                logger.info("Found transaction container divListaMovimientos")
            except TimeoutException:
                logger.warning("Transaction container not found, trying alternative selectors...")
                # Try alternative selectors
                alternative_selectors = [
                    'div#divListaMovimientos',
                    'div[class*="movimientos"]',
                    'div[class*="actividades"]'
                ]
                
                for selector in alternative_selectors:
                    try:
                        table_container = self.driver.find_element(By.CSS_SELECTOR, selector)
                        logger.info(f"Found container using selector: {selector}")
                        break
                    except:
                        continue
                
                if not table_container:
                    logger.error("Could not find transaction container")
                    return transactions
            
            # Find all transaction elements (same approach as dev runner)
            transaction_elements = table_container.find_elements(By.CLASS_NAME, "noLeido")
            logger.info(f"Found {len(transaction_elements)} transaction elements")
            
            if not transaction_elements:
                logger.warning("No transaction elements found")
                return transactions
            
            # Process each transaction element
            for i, elem in enumerate(transaction_elements, start=1):
                try:
                    logger.info(f"Processing transaction {i}")
                    
                    # Extract date from fecha-cell
                    try:
                        fecha_cell = elem.find_element(By.CSS_SELECTOR, 'td.fecha-cell')
                        date_span = fecha_cell.find_element(By.CSS_SELECTOR, 'span.text_ellipsis')
                        date_text = date_span.text.strip()
                        if not date_text:
                            # Try alternative approach
                            date_text = fecha_cell.text.strip()
                    except:
                        logger.warning(f"No date found for transaction {i}")
                        continue
                    
                    formatted_date = self.parse_date(date_text)
                    logger.info(f"Date: {date_text} -> {formatted_date}")
                    
                    # Extract merchant name from categoria-cell
                    try:
                        categoria_cell = elem.find_element(By.CSS_SELECTOR, 'td.categoria-cell')
                        # Look for the merchant name text (avoid category icons and hidden elements)
                        merchant_spans = categoria_cell.find_elements(By.CSS_SELECTOR, 'span.text_ellipsis')
                        merchant_name = "Unknown"
                        
                        for span in merchant_spans:
                            text = span.text.strip()
                            # Skip if it's just a category icon or empty
                            if text and not text.startswith('c-category') and len(text) > 2:
                                # Check if this span contains the actual merchant name
                                if not any(word in text.lower() for word in ['tipomov', 'hidden', 'margin']):
                                    merchant_name = text
                                    break
                        
                        # If no merchant name found in spans, try to get it from the cell text
                        if merchant_name == "Unknown":
                            cell_text = categoria_cell.text.strip()
                            # Clean up the text by removing category-related content
                            lines = cell_text.split('\n')
                            for line in lines:
                                line = line.strip()
                                if line and not line.startswith('c-category') and len(line) > 2:
                                    if not any(word in line.lower() for word in ['tipomov', 'hidden', 'margin']):
                                        merchant_name = line
                                        break
                        
                    except Exception as e:
                        logger.warning(f"Error extracting merchant name for transaction {i}: {e}")
                        merchant_name = "Unknown"
                    
                    logger.info(f"Merchant: {merchant_name}")
                    
                    # Extract account information from activities__cell_service
                    try:
                        service_cell = elem.find_element(By.CSS_SELECTOR, 'td.activities__cell_service')
                        account_info = self.extract_account_info(service_cell)
                    except:
                        account_info = "Unknown"
                    
                    logger.info(f"Account: {account_info}")
                    
                    # Extract amount from precio-cell
                    try:
                        precio_cell = elem.find_element(By.CSS_SELECTOR, 'td.precio-cell')
                        amount_text = precio_cell.text.strip()
                        amount = self.parse_amount(amount_text)
                    except:
                        logger.warning(f"No amount found for transaction {i}")
                        amount = 0.0
                    
                    logger.info(f"Amount: {amount}")
                    
                    # Categorize the transaction
                    concepto = self.categorize_transaction(merchant_name, amount)
                    logger.info(f"Category: {concepto}")
                    
                    # Create Transaction object
                    from datetime import datetime
                    try:
                        # Parse the ISO date string back to datetime object
                        tx_date = datetime.strptime(formatted_date, "%Y-%m-%d %H:%M:%S")
                    except:
                        tx_date = datetime.now()
                    
                    transaction = Transaction(
                        date=tx_date,
                        description=merchant_name,
                        category=concepto,
                        amount=amount,
                        source="caixa_bank",
                        account=account_info
                    )
                    
                    transactions.append(transaction)
                    
                    logger.info(f"Successfully processed transaction {i}")
                    
                except Exception as e:
                    logger.error(f"Error processing transaction {i}: {e}")
                    continue
            
            logger.info(f"Successfully extracted {len(transactions)} transactions")
            
        except Exception as e:
            logger.error(f"Error during transaction extraction: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
        
        return transactions

    def export_transactions_to_csv(self, filename=None) -> bool:
        """Export transactions to CSV file in RuralVia format with additional account column"""
        if not self.transactions:
            logger.warning("No transactions to save")
            return False
        
        try:
            # Create data/exports directory if it doesn't exist
            os.makedirs("data/exports", exist_ok=True)
            
            # Generate filename if not provided
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{timestamp}_caixa_transactions.csv"
            
            filepath = os.path.join("data/exports", filename)
            
            # Create CSV with format: date,description,category,amount,account
            import csv
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerow(['date', 'description', 'category', 'amount', 'account'])
                
                for tx in self.transactions:
                    writer.writerow([
                        tx.date.strftime("%Y-%m-%d %H:%M:%S"),
                        tx.description,
                        tx.category,
                        tx.amount,
                        tx.account
                    ])
            
            logger.info(f"Successfully saved {len(self.transactions)} transactions to {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save transactions to CSV: {e}")
            return False

    def get_transactions(self) -> List[Transaction]:
        """Get the extracted transactions"""
        return self.transactions

    def clear_transactions(self):
        """Clear the stored transactions"""
        self.transactions.clear()
        logger.info("Transactions cleared")

    def get_transaction_summary(self) -> Dict[str, Any]:
        """Get a summary of the extracted transactions"""
        if not self.transactions:
            return {"total": 0, "message": "No transactions available"}
        
        total_transactions = len(self.transactions)
        total_amount = sum(tx.amount for tx in self.transactions)
        income_amount = sum(tx.amount for tx in self.transactions if tx.amount > 0)
        expense_amount = sum(tx.amount for tx in self.transactions if tx.amount < 0)
        
        # Count by category
        category_counts = {}
        for tx in self.transactions:
            category_counts[tx.category] = category_counts.get(tx.category, 0) + 1
        
        # Count by account
        account_counts = {}
        for tx in self.transactions:
            account_counts[tx.account] = account_counts.get(tx.account, 0) + 1
        
        return {
            "total_transactions": total_transactions,
            "total_amount": total_amount,
            "income_amount": income_amount,
            "expense_amount": expense_amount,
            "category_breakdown": category_counts,
            "account_breakdown": account_counts,
            "date_range": {
                "earliest": min(tx.date for tx in self.transactions).strftime("%Y-%m-%d"),
                "latest": max(tx.date for tx in self.transactions).strftime("%Y-%m-%d")
            }
        }
