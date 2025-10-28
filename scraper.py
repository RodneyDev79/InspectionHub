import os
import pandas as pd
import requests

# --- Configuration ---
BASE_URL = "https://inspections.myhealthdepartment.com/"
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "inspections.csv")


class HealthScraper:
    """
    A scraper for Tennessee health inspection data from myhealthdepartment.com.
    It navigates through paginated search results for a specific county.
    """

    def __init__(self, url: str, output_file: str):
        self.url = url
        self.output_file = output_file
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://inspections.myhealthdepartment.com/tennessee"
        })

    def _process_api_response(self, response_json: list) -> pd.DataFrame:
        """Extracts inspection records from the API JSON response."""
        # On success, the API returns a list of records.
        # On failure or end of data, it may return a dict or other format.
        if not isinstance(response_json, list):
            # This is likely the end of the results, so we return an empty frame.
            return pd.DataFrame()

        if not response_json:
            return pd.DataFrame()

        df = pd.DataFrame(response_json)

        # Rename columns from API format to our desired CSV format
        df = df.rename(columns={
            'establishmentName': 'name',
            'addressLine1': 'address',
            'inspectionDate': 'date',
            'inspectionType': 'type'
        })

        # --- Data Cleaning ---
        # Strip leading/trailing whitespace from names
        if 'name' in df.columns:
            df['name'] = df['name'].str.strip()

        # Format the date to be more readable (YYYY-MM-DD)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        columns_to_keep = ['name', 'address', 'city', 'state', 'zip', 'date', 'score', 'purpose', 'type']
        df = df.reindex(columns=columns_to_keep)
        return df

    def _save_data(self, df: pd.DataFrame):
        """Appends a DataFrame to the CSV file."""
        if df.empty:
            return
        file_exists = os.path.exists(self.output_file)
        df.to_csv(self.output_file, mode='a', header=not file_exists, index=False)
        print(f"Saved {len(df)} records to {self.output_file}")

    def run(self):
        """Main execution method to run the entire scraping process."""
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

        # Remove the old output file to ensure a clean slate for each run
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
            print(f"Removed old output file: {self.output_file}")

        search_city = "Knoxville"
        results_per_page = 25 # Adjusted to match the API's actual max return per page
        print(f"Starting scrape for establishments in '{search_city}'...")

        page_number = 1
        while True:
            start_index = (page_number - 1) * results_per_page
            print(f"\n--- Scraping Page {page_number} (starting at record {start_index}) ---")

            payload = {
                "data": {
                    "path": "tennessee",
                    "programName": "",
                    "filters": {"date": "2023-01-01 to 2024-12-31", "purpose": "", "county": ""}, # Added a default date range
                    "start": start_index,
                    "count": results_per_page,
                    "searchStr": search_city,
                    "lat": 0, "lng": 0, "sort": {}
                },
                "task": "searchInspections"
            }

            try:
                response = self.session.post(self.url, json=payload, timeout=20)
                response.raise_for_status()
                response_data = response.json()
            except requests.exceptions.RequestException as e:
                print(f"Failed to retrieve page {page_number}. Error: {e}")
                break
            except ValueError: # Catches JSON decoding errors
                print(f"Failed to decode JSON from page {page_number}.")
                break

            df = self._process_api_response(response_data)

            if df.empty:
                print("No more results found. Scraping complete.")
                break

            self._save_data(df)

            # If the number of returned records is less than what we asked for,
            # it must be the last page.
            if len(df) < results_per_page:
                print(f"Scraped all available records. Scraping complete.")
                break

            page_number += 1


if __name__ == "__main__":
    scraper = HealthScraper(
        url=BASE_URL,
        output_file=OUTPUT_FILE
    )
    scraper.run()