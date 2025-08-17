import os
import logging
import time
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    """
    Generic runner for development and testing.
    Connects to an existing browser session to allow for live interaction
    with a web page.

    Prerequisites:
    1. A browser with the target website open.
    2. The browser must be started with remote debugging enabled on a specific port.
       Example for MS Edge:
       "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe" --remote-debugging-port=9222
    3. The DEBUGGER_ADDRESS environment variable must be set in your .env file.
       e.g., DEBUGGER_ADDRESS=localhost:9222
    """
    driver = None
    try:
        logger.info("Starting generic dev runner...")
        debugger_address = os.getenv("DEBUGGER_ADDRESS")
        if not debugger_address:
            logger.error(
                "DEBUGGER_ADDRESS environment variable not set. Please set it in your .env file."
            )
            return

        logger.info(f"Connecting to existing Edge instance at {debugger_address}")
        edge_options = Options()
        edge_options.use_chromium = True
        edge_options.add_experimental_option("debuggerAddress", debugger_address)
        
        driver = webdriver.Edge(options=edge_options)

        logger.info(f"Successfully connected to the browser. Initial tab: {driver.title}")

        # --- Find the correct tab ---
        # The browser might have multiple tabs open. We need to find the one
        # with our target application.
        target_url_part = "caixabank.es"  # <-- IMPORTANT: Change if your URL is different
        found_target_tab = False

        # Get all open tab handles
        window_handles = driver.window_handles
        logger.info(f"Found {len(window_handles)} open tabs. Searching for '{target_url_part}'...")

        for handle in window_handles:
            driver.switch_to.window(handle)
            logger.info(f"Switched to tab with URL: {driver.current_url}")
            if target_url_part in driver.current_url:
                logger.info(f"Found target tab: {driver.title}")
                found_target_tab = True
                break

        if not found_target_tab:
            logger.error(f"Could not find any open tab with URL containing '{target_url_part}'.")
            logger.error("Please make sure the target page is open in the browser.")
            return

        # --- Now you can interact with the correct page ---
        logger.info("Now interacting with the correct page.")

        try:
            wait = WebDriverWait(driver, 20)
            
            # Step 1: Navigate to the outer iframe (third iframe on page)
            logger.info("Waiting for outer iframe...")
            outer_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[3]"))  # iframe index 2 -> third iframe
            )
            driver.switch_to.frame(outer_iframe)

            # Step 2: Navigate to the inner iframe (first iframe inside the outer)
            logger.info("Waiting for inner iframe...")
            inner_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[1]"))  # iframe index 0 inside parent
            )
            driver.switch_to.frame(inner_iframe)

            # Step 3: Click on "Cuentas y Tarjetas" first to navigate to the accounts section
            logger.info("Waiting for 'Cuentas y Tarjetas' link...")
            cuentas_y_tarjetas = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//*[@id='pestanya0']/a/span"))
            )
            logger.info("Clicking on 'Cuentas y Tarjetas'...")
            cuentas_y_tarjetas.click()
            
            # Step 4: Now we need to navigate to the Navbar iframe to click on "Mis finanzas"
            # First, go back to the main frame
            driver.switch_to.default_content()
            
            # Navigate to the outer iframe again (third iframe)
            logger.info("Re-navigating to outer iframe...")
            outer_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[3]"))
            )
            driver.switch_to.frame(outer_iframe)
            
            # Now look for the Navbar iframe (it should be visible after clicking Cuentas y Tarjetas)
            logger.info("Looking for Navbar iframe...")
            navbar_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "//iframe[@name='Navbar' or @id='Navbar']"))
            )
            driver.switch_to.frame(navbar_iframe)
            
            # Step 5: Click on "Mis finanzas"
            logger.info("Waiting for 'Mis finanzas' link...")
            mis_finanzas = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//*[@id='pestanya1']//span[text()='Mis Finanzas']"))
            )
            logger.info("Clicking on 'Mis finanzas'...")
            mis_finanzas.click()
            
            # Step 6: Navigate to the Cos iframe to access the dashboard content
            # First, go back to the main frame
            driver.switch_to.default_content()
            
            # Navigate to the outer iframe again (third iframe)
            logger.info("Re-navigating to outer iframe for Cos...")
            outer_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "(//iframe)[3]"))
            )
            driver.switch_to.frame(outer_iframe)
            
            # Now look for the Cos iframe
            logger.info("Looking for Cos iframe...")
            cos_iframe = wait.until(
                EC.presence_of_element_located((By.XPATH, "//iframe[@name='Cos' or @title='Cuerpo']"))
            )
            driver.switch_to.frame(cos_iframe)
            
            # Step 7: Click on "Últimos movimientos"
            logger.info("Waiting for 'Últimos movimientos' section...")
            ultimos_movimientos = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[@id='ACTIVIDAD_titulo']//a[@class='general_dashboard__grid__item__link general_dashboard__grid__item__handle']"))
            )
            logger.info("Clicking on 'Últimos movimientos'...")
            ultimos_movimientos.click()

        except TimeoutException as e:
            logger.error(f"Timeout occurred: {e}")
            logger.error("Check if the page structure has changed or if elements are loading slowly")
        except Exception as e:
            logger.error(f"An error occurred during interaction: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")

        logger.info("Script finished. The browser session remains open.")
    except Exception as e:
        logger.critical(f"An unhandled error occurred: {e}", exc_info=True)
    finally:
        # We don't call driver.quit() because we want to leave the browser open.
        logger.info("Dev runner finished.")


if __name__ == "__main__":
    main()
