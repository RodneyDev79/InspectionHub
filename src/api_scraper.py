import os
from datetime import datetime, timedelta
import re
import json
from bs4 import BeautifulSoup
import time
import random
from curl_cffi import requests as curl_requests # Use curl_cffi to impersonate a browser
import logging
from database_setup import get_db_connection

BASE_URL = "https://inspections.myhealthdepartment.com/"

class ApiScraper:
    """
    Scrapes health inspection data from the myhealthdepartment.com API,
    finds new inspections, and saves them to a SQLite database.
    """
    def __init__(self):
        pass # No longer need to store db_path

    def _discover_paths(self):
        """
        Visits the main portal page to discover all available paths (states/regions).
        """
        logging.info("Discovering all available health department paths...")
        try:
            # Use a temporary session just for discovery
            with curl_requests.Session(impersonate="chrome110") as session:
                response = session.get(BASE_URL, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Find all the "View Site" or "View Inspections" anchor tags
            location_links = soup.select('div.jurisdiction-section a.search-button')
            if not location_links:
                logging.warning("Could not find any location links. Using fallback list.")
                return ["tennessee", "alabama", "arizona", "florida"]

            discovered_paths = set()
            for link in location_links:
                href = link.get('href')
                if href and href.startswith('/'):
                    path = href.strip('/')
                    # Filter out test/staging sites
                    if 'test' not in path and 'staging' not in path:
                        discovered_paths.add(path)
            
            logging.info(f"Discovered {len(discovered_paths)} paths to scrape.")
            return list(discovered_paths)
        except (curl_requests.errors.RequestsError, AttributeError, json.JSONDecodeError) as e:
            logging.error(f"Failed to discover paths, using fallback list. Error: {e}")
            return ["tennessee", "alabama", "arizona", "florida"]

    def _get_recent_inspections_for_path(self, session, path: str):
        """Fetches recent inspections from the API for a given path."""
        # Search the last 30 days to ensure we don't miss anything.
        # The database logic will handle duplicates.
        today = datetime.now()
        thirty_days_ago = today - timedelta(days=30)
        date_filter = f"{thirty_days_ago.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}"
        session.headers.update({
            "Referer": f"{BASE_URL}{path}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        })

        payload = {
            "data": {
                "path": path,
                "programName": "",
                "filters": {"date": date_filter, "purpose": "", "county": ""},
                "start": 0,
                "count": 500, # Fetch a large number to get all recent results
                "searchStr": "",
                "lat": 0, "lng": 0, "sort": {}
            },
            "task": "searchInspections"
        }
        try:
            # First attempt with the standard payload
            response = session.post(BASE_URL, json=payload, timeout=30)
            response.raise_for_status()
            results = response.json()

            # If the first attempt returns nothing, try a common variation
            if not results:
                logging.info(f"  Initial attempt for '{path}' returned no data. Retrying with 'Food' program filter.")
                payload["data"]["programName"] = "Food"
                response = session.post(BASE_URL, json=payload, timeout=30)
                response.raise_for_status()
                results = response.json()

            return results
        except (curl_requests.errors.RequestsError, ValueError) as e:
            logging.error(f"Failed to retrieve data from API for path '{path}'. Error: {e}")
            return []

    def run(self):
        """
        Main execution method. Fetches recent inspections and adds only
        new records to the database.
        Returns a list of newly added inspections for the bot to tweet.
        """
        paths_to_scrape = self._discover_paths()
        all_newly_added_inspections = []

        for path in paths_to_scrape:
            logging.info(f"--- Fetching recent inspections for '{path}' ---")
            
            # Create a new session for each path to avoid being blocked
            with curl_requests.Session(impersonate="chrome110") as session:
                api_results = self._get_recent_inspections_for_path(session, path)

            if not isinstance(api_results, list) or not api_results:
                logging.warning(f"No data returned from API for '{path}' or API format has changed.")
                # Add a small, random delay even on failure to be safe
                time.sleep(random.uniform(2, 4))
                continue
            
            # Add a small, random delay to avoid being rate-limited
            time.sleep(random.uniform(2, 4))

            with get_db_connection() as conn:
                cursor = conn.cursor()

                for record in api_results:
                    establishment_id = record.get('permitID')
                    if not establishment_id:
                        continue

                    # Clean and format data
                    name = record.get('establishmentName', 'N/A').strip()
                    address = record.get('addressLine1', '').strip()
                    
                    permit_type_value = record.get('permitType', '')
                    if isinstance(permit_type_value, list):
                        category = ', '.join(permit_type_value).strip()
                    else:
                        category = str(permit_type_value).strip()

                    inspection_date_str = record.get('inspectionDate', '').split('T')[0]
                    score = record.get('score')
                    
                    # Filter out records with no score or a score of 0
                    if score is None or score == 0:
                        continue

                    purpose = record.get('purpose', '').strip()

                    # --- 1. Insert or update restaurant ---
                    cursor.execute(
                        "INSERT INTO restaurants (establishment_id, path, name, address, category) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name=VALUES(name), address=VALUES(address), category=VALUES(category)",
                        (establishment_id, path, name, address, category)
                    )

                    # --- 2. Check if inspection is new ---
                    cursor.execute(
                        "SELECT id FROM inspections WHERE establishment_id = %s AND establishment_path = %s AND inspection_date = %s AND score = %s",
                        (establishment_id, path, inspection_date_str, score)
                    )
                    if cursor.fetchone():
                        continue # Skip if it already exists

                    logging.info(f"  New inspection found: {name} ({path}) on {inspection_date_str} - Score: {score}")

                    # --- 3. Insert new inspection ---
                    cursor.execute(
                        "INSERT INTO inspections (establishment_id, establishment_path, inspection_date, score, purpose) VALUES (%s, %s, %s, %s, %s)",
                        (establishment_id, path, inspection_date_str, score, purpose)
                    )

                    all_newly_added_inspections.append({
                        "name": name,
                        "score": score,
                        "date": inspection_date_str
                    })

                conn.commit()

        logging.info(f"Scraping complete. Found and added {len(all_newly_added_inspections)} new inspections across all paths.")
        return all_newly_added_inspections

if __name__ == "__main__":
    # To run this script directly for testing:
    # 1. Make sure the database is set up: python src/database_setup.py
    # 2. Run this script: python src/api_scraper.py
    from logger_setup import setup_logging
    setup_logging()
    logging.info("Running API scraper for testing...")
    scraper = ApiScraper()
    scraper.run()