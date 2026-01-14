import os
import re
import time
import json
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from flask import Flask, jsonify
from sqlalchemy import create_engine
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from rapidfuzz import fuzz

# =====================================================
# CONFIGURATION
# =====================================================
DB_URI = "postgresql+psycopg2://postgres:dost@localhost:5432/kruiz-dev"
EXCEL_PATH = "USE THIS - All CSL Properties with Global Ids and GDS Ids (Active)_Jul2025_2 2 - excel.xlsx"
TABLE_NAME = "hotel_mapped_url"
HEADLESS = False
RETRY_LIMIT = 3
FUZZY_THRESHOLD = 85

# =====================================================
# STATE MAP
# =====================================================
STATE_CODE_MAP = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL",
    "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY"
}

# =====================================================
# HELPERS
# =====================================================
def normalize_text(val):
    if not val or str(val).lower() == "nan":
        return ""
    val = str(val).lower()
    val = re.sub(r"[^a-z0-9\s]", "", val)
    return val.strip()

def state_to_code(state):
    if not state:
        return None
    state = str(state).strip()
    if len(state) == 2:
        return state.upper()
    return STATE_CODE_MAP.get(state.lower())

def country_to_code(country):
    if not country:
        return None
    c = str(country).upper().strip()
    return "US" if c in ("USA", "UNITED STATES") else c

def parse_address_components(address):
    parts = [p.strip() for p in address.split(",")]
    city = parts[-4] if len(parts) >= 4 else ""
    state = parts[-3] if len(parts) >= 3 else ""
    country = parts[-1] if parts else ""
    return city, state, country

def load_locations():
    with open("hilton_locations.json", "r", encoding="utf-8") as f:
        return json.load(f)

def make_uc_options():
    opts = uc.ChromeOptions()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--start-maximized")
    opts.add_argument("--window-size=1920,1080")
    return opts

def retry_action(action, retries=RETRY_LIMIT, delay=2):
    for i in range(retries):
        try:
            return action()
        except Exception as e:
            print(f"⚠️ Retry {i+1}/{retries} failed: {e}")
            time.sleep(delay)
    return None

def get_db_engine():
    return create_engine(DB_URI)

# =====================================================
# DB INIT
# =====================================================
def create_table_if_not_exists():
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            hotel_code TEXT,
            scraped_hotel_name TEXT,
            global_property_name TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            url TEXT,
            address TEXT,
            latitude NUMERIC,
            longitude NUMERIC,
            match_confidence NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

# =====================================================
# LOAD EXCEL
# =====================================================
def load_csl_hotels():
    df = pd.read_excel(EXCEL_PATH)

    df["hotel_norm"] = df["Global Property Name"].apply(normalize_text)
    df["city_norm"] = df["Property City Name"].apply(normalize_text)
    df["state_code"] = df["Property State/Province"].astype(str).str.upper().str.strip()
    df["country_code"] = df["Property Country Code"].astype(str).str.upper().str.strip()

    return df

# =====================================================
# SCRAPER
# =====================================================
def fetch_property_details(driver, wait):
    try:
        el = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a[href*='google.com/maps'], div[data-testid='hotel-address']")
            )
        )
        return el.text.strip()
    except TimeoutException:
        return ""

def scrape_single_hotel(driver, wait, url):
    try:
        driver.get(url)
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(1)

        try:
            name = wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1"))).text.strip()
        except TimeoutException:
            name = ""

        address = fetch_property_details(driver, wait)
        city, state, country = parse_address_components(address)

        return {
            "scraped_name": name,
            "city": city,
            "state": state,
            "country": country,
            "address": address,
            "url": url
        }
    except Exception:
        return None

def scrape_hotels_from_location(driver, wait, location):
    driver.get(location["url"])
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    time.sleep(2)

    links = list({
        a.get_attribute("href")
        for a in driver.find_elements(By.CSS_SELECTOR, "a[href*='/en/hotels/']")
        if a.get_attribute("href")
    })

    results = []
    for link in links:
        data = retry_action(lambda: scrape_single_hotel(driver, wait, link))
        if data:
            results.append(data)
    return results

# =====================================================
# MATCHING (CORRECT LOGIC)
# =====================================================
def match_hotels(scraped_hotels, df_master):
    matched = []

    for item in scraped_hotels:
        item_city = normalize_text(item["city"])
        item_hotel = normalize_text(item["scraped_name"])
        item_state = state_to_code(item["state"])
        item_country = country_to_code(item["country"])

        if not item_city or not item_country:
            continue

        candidates = df_master[
            (df_master["city_norm"] == item_city) &
            (df_master["country_code"] == item_country)
        ]

        if item_state:
            candidates = candidates[candidates["state_code"] == item_state]

        best_score = 0
        best_match = None

        for _, row in candidates.iterrows():
            score = fuzz.token_set_ratio(item_hotel, row["hotel_norm"])
            if score > best_score:
                best_score = score
                best_match = row

        if best_match is not None and best_score >= FUZZY_THRESHOLD:
            matched.append({
                "hotel_code": best_match["Global Property ID"],
                "scraped_hotel_name": item["scraped_name"],
                "global_property_name": best_match["Global Property Name"],
                "city": item["city"],
                "state": item["state"],
                "country": item["country"],
                "url": item["url"],
                "address": item["address"],
                "latitude": best_match["Property Latitude"],
                "longitude": best_match["Property Longitude"],
                "match_confidence": best_score
            })
        else:
            matched.append({
                "hotel_code": None,
                "scraped_hotel_name": item["scraped_name"],
                "global_property_name": None,
                "city": item["city"],
                "state": item["state"],
                "country": item["country"],
                "url": item["url"],
                "address": item["address"],
                "latitude": None,
                "longitude": None,
                "match_confidence": 0
            })

    return matched

# =====================================================
# SAVE
# =====================================================
def save_to_db(records):
    if records:
        df = pd.DataFrame(records)
        df.to_sql(TABLE_NAME, get_db_engine(), if_exists="append", index=False)

# =====================================================
# MAIN
# =====================================================
def main_scrape_and_map(driver, wait):
    create_table_if_not_exists()
    df_master = load_csl_hotels()
    all_results = []

    for loc in load_locations():
        hotels = scrape_hotels_from_location(driver, wait, loc)
        matched = match_hotels(hotels, df_master)
        save_to_db(matched)
        all_results.extend(matched)

    with open("hilton_hotel_url_mapped.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    return all_results

# =====================================================
# FLASK API
# =====================================================
app = Flask(__name__)

@app.route("/run_scrape_and_map", methods=["GET"])
def run_scrape_and_map():
    driver = uc.Chrome(options=make_uc_options())
    wait = WebDriverWait(driver, 25)
    try:
        results = main_scrape_and_map(driver, wait)
        return jsonify({"status": "success", "count": len(results)})
    finally:
        driver.quit()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
