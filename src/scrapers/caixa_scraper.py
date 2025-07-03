from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.options import Options
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
        self.driver = None
        self.debugger_address = debugger_address
        self.ws = None
        self.ws_thread = None
        self.ws_ready = Event()
        self.network_enabled = False
        self.captured_responses = {}

    def setup_driver(self):
        logger.info("Setting up Edge WebDriver")
        edge_options = Options()
        edge_options.use_chromium = True
        edge_options.add_argument("--remote-allow-origins=*")
        if self.debugger_address:
            logger.info(f"Connecting to existing Edge instance at {self.debugger_address}")
            edge_options.add_experimental_option("debuggerAddress", self.debugger_address)
        else:
            # To keep the browser open after the script finishes, you can detach it.
            edge_options.add_experimental_option("detach", True)
            # A new remote debugging port will be assigned.
            edge_options.add_argument("--remote-debugging-port=0")

        self.driver = webdriver.Edge(options=edge_options)
        if not self.debugger_address:
            self.debugger_address = self.driver.capabilities['ms:edgeOptions']['debuggerAddress']
            logger.info(f"Started new Edge instance with debugger address: {self.debugger_address}")

        self.driver.get(self.base_url)

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

            # Accept cookies if the button is present
            # try:
            #     cookie_button = wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
            #     cookie_button.click()
            #     logger.info("Accepted cookies.")
            #     time.sleep(1) # wait for banner to disappear
            # except TimeoutException:
            #     logger.info("Cookie banner not found or already accepted.")

            # The login form might be inside an iframe, let's check
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
            wait = WebDriverWait(self.driver, 10)

            logger.info("Waiting for dashboard to load")
            logger.info("Waiting for first iframe to load")

            # Espera a que el primer iframe esté presente y cambia a él
            outer_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[3]"))  # iframe index 2 -> third iframe
            )
            self.driver.switch_to.frame(outer_iframe)
            
            logger.info("Waiting for second iframe to load")
            # Ahora espera al iframe interno (dentro del outer)
            inner_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[1]"))  # iframe index 0 inside parent
            )
            self.driver.switch_to.frame(inner_iframe)
            

            logger.info("Waiting for finances link to load")
            # Ahora ya puedes interactuar con el span
            finances_link = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//*[@id='pestanya0']/a/span"))
            )
            finances_link.click()

            # Cambiar al iframe externo (índice 2 => 3er iframe)
            outer_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[3]"))
            )
            self.driver.switch_to.frame(outer_iframe)

            # Cambiar al iframe interno (índice 1 => 2do iframe dentro del anterior)
            inner_iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            if len(inner_iframes) < 2:
                raise Exception("No se encontraron suficientes iframes dentro del iframe externo.")
            self.driver.switch_to.frame(inner_iframes[1])

            logger.info("Clicking on 'Mis Finanzas'")
            mis_finanzas = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//*[@id='pestanya1']/a/span"))
            )
            mis_finanzas.click()

            # Wait for the finances page to load
            self.driver.switch_to.default_content() # Switch back to main content
            wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, "//iframe[contains(@class, 'MAIN')]"))) # Placeholder selector
            wait.until(EC.frame_to_be_available_and_switch_to_it(2)) # Placeholder selector
            wait.until(EC.frame_to_be_available_and_switch_to_it(2)) # Placeholder selector

            # Click on 'Ingresos y gastos'
            income_expenses_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(., 'Ingresos y gastos')]")))
            income_expenses_link.click()
            logger.info("Clicked on 'Ingresos y gastos'")

        except TimeoutException as e:
            logger.error(f"Timeout while navigating to finances: {e}")
        except Exception as e:
            logger.error(f"An error occurred during navigation: {e}")

    def scrape(self):
        self.setup_driver()
        self._start_ws_listener()
        if self.login():
            self.navigate_to_finances()
            # Wait for data to be captured
            time.sleep(10) # Adjust as needed
            self.process_captured_data()
        self.teardown_driver()

    def process_captured_data(self):
        # This is where you would parse the captured JSON responses
        # For now, we just save them to a file for inspection
        logger.info(f"Captured {len(self.captured_responses)} responses.")
        for i, (req_id, body) in enumerate(self.captured_responses.items()):
            if body:
                try:
                    data = json.loads(body)
                    with open(f"caixa_response_{i}.json", "w") as f:
                        json.dump(data, f, indent=2)
                    logger.info(f"Saved response {i} to caixa_response_{i}.json")
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse response {i} as JSON.")

    def teardown_driver(self):
        if self.ws:
            self.ws.close()
        if self.driver:
            # self.driver.quit() # Uncomment if you want the browser to close automatically
            pass
        logger.info("Scraper finished.")
