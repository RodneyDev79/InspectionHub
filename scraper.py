import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup

# --- Configuration ---
BASE_URL = "https://inspections.knoxcounty.org/Search.aspx"
GECKODRIVER_PATH = "../Dynamic Content Scraper/drivers/geckodriver"
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "inspections.csv")


class HealthScraper:
    """
    A scraper for Knox County health inspection data.
    Handles ASP.NET __doPostBack pagination.
    """

    def __init__(self, url: str, driver_path: str, output_file: str):
        self.url = url
        self.driver_path = driver_path
        self.output_file = output_file
        self.driver = self._setup_driver()

    def _setup_driver(self) -> webdriver.Firefox:
        """Initializes and returns a headless Firefox WebDriver."""
        print("Setting up Selenium WebDriver...")
        firefox_options = Options()
        firefox_options.add_argument("-headless")
        service = Service(executable_path=self.driver_path)
        driver = webdriver.Firefox(service=service, options=firefox_options)
        return driver

    def _perform_initial_search(self):
        """Navigates to the site and performs an empty search to get all results."""
        print(f"Navigating to {self.url}...")
        self.driver.get(self.url)
        wait = WebDriverWait(self.driver, 10)

        print("Performing initial search to load all records...")
        search_button = wait.until(
            EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_btnSearch"))
        )
        search_button.click()

        # Wait for the results table to appear after the search
        wait.until(
            EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_gvInspections"))
        )
        print("Search results loaded.")

    def _parse_page_data(self) -> pd.DataFrame:
        """Parses the inspection data from the current page into a pandas DataFrame."""
        print("Parsing data from the current page...")
        page_source = self.driver.page_source
        # pandas.read_html is highly effective for parsing HTML tables
        try:
            tables = pd.read_html(page_source, attrs={'id': 'ctl00_ContentPlaceHolder1_gvInspections'})
            if tables:
                print(f"Successfully extracted a table with {len(tables[0])} rows.")
                return tables[0]
        except ValueError:
            # This can happen if read_html doesn't find a matching table
            print("No table found on the page with the specified ID.")
        return pd.DataFrame()

    def _save_data(self, df: pd.DataFrame):
        """Appends a DataFrame to the CSV file."""
        if df.empty:
            return
        # Append to the CSV, writing the header only if the file doesn't exist yet
        file_exists = os.path.exists(self.output_file)
        df.to_csv(self.output_file, mode='a', header=not file_exists, index=False)
        print(f"Saved {len(df)} records to {self.output_file}")

    def run(self):
        """Main execution method to run the entire scraping process."""
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

        try:
            self._perform_initial_search()

            page_count = 1
            while True:
                print(f"\n--- Scraping Page {page_count} ---")

                # Wait for the table to be present on the current page before parsing
                wait = WebDriverWait(self.driver, 10)
                table = wait.until(
                    EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_gvInspections"))
                )

                # 1. Parse data from the current page
                df = self._parse_page_data()
                self._save_data(df)

                # 2. Find and click the 'Next' button.
                try:
                    print("Looking for the 'Next' page button...")
                    # The 'Next' button is a link with the text "Next"
                    next_button = self.driver.find_element(By.LINK_TEXT, "Next")
                    next_button.click()
                    print("Navigating to the next page...")
                    page_count += 1
                    # Wait for the old table to go stale, confirming the page has reloaded
                    wait.until(EC.staleness_of(table))
                except NoSuchElementException:
                    print("No 'Next' page button found. This is the last page.")
                    break
            print("\nScraping complete. Data saved.")

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if self.driver:
                print("Closing WebDriver...")
                self.driver.quit()


if __name__ == "__main__":
    # Ensure you have a 'drivers' directory with 'geckodriver' at the specified path
    # relative to this script's location.
    scraper = HealthScraper(
        url=BASE_URL,
        driver_path=GECKODRIVER_PATH,
        output_file=OUTPUT_FILE
    )
    scraper.run()

