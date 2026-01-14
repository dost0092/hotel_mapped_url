import os
import re
import time
import json
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from flask import Flask, jsonify
from sqlalchemy import create_engine, text
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from difflib import SequenceMatcher
from rapidfuzz import fuzz

# =====================================================
# CONFIGURATION
# =====================================================
DB_URI = "postgresql+psycopg2://postgres:dost@localhost:5432/kruiz-dev"
EXCEL_PATH = "USE THIS - All CSL Properties with Global Ids and GDS Ids (Active)_Jul2025_2 2 - excel.xlsx"
TABLE_NAME = "hotel_mapped_url"
HEADLESS = False
RETRY_LIMIT = 3

# Fuzzy match configuration
FUZZY_THRESHOLD = 85

# US state name to code lookup
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
# LOAD LOCATIONS
# =====================================================
def load_locations():
    with open("hilton_locations.json", "r", encoding="utf-8") as f:
        return json.load(f)

# =====================================================
# HELPERS
# =====================================================
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
    print("❌ Max retries reached")
    return None

def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = str(name).lower()
    name = re.sub(r"[^a-z0-9 ]+", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()

def normalize_text(val):
    if val is None:
        return ""
    val = str(val)
    if val.lower() == "nan":
        return ""
    val = val.lower()
    val = re.sub(r"[^a-z0-9\s]", "", val)
    return val.strip()

def state_to_code(state):
    if not state:
        return None
    s = str(state).strip()
    if len(s) == 2 and s.isalpha():
        return s.upper()
    return STATE_CODE_MAP.get(s.lower())

def country_to_code(country):
    if not country:
        return None
    c = str(country).strip()
    if not c:
        return None
    c_up = c.upper()
    # normalize common variants
    if c_up in ("USA", "UNITED STATES", "US", "U.S.", "U.S.A."):
        return "US"
    return c_up

def parse_address_components(address: str):
    """
    Robust address parser:
    - Works for US & non-US
    - Extracts city, state, country safely
    """
    if not address:
        return "", "", ""

    parts = [p.strip() for p in address.split(",") if p.strip()]

    city = state = country = ""

    # Country → usually last
    if len(parts) >= 1:
        country = parts[-1]

    # US-style: City, State ZIP, Country
    if len(parts) >= 3:
        possible_state = parts[-2]
        possible_city = parts[-3]

        # Handle "CA 90001"
        state_match = re.match(r"([A-Za-z]{2})\b", possible_state)
        if state_match:
            state = state_match.group(1)
            city = possible_city
        else:
            city = possible_state

    # Fallbacks
    if not city and len(parts) >= 2:
        city = parts[-2]

    return city.strip(), state.strip(), country.strip()

def get_db_engine():
    return create_engine(DB_URI)

def create_table_if_not_exists():
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            hotel_code text,
            scraped_name TEXT,
            global_property_name TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            url TEXT,
            address TEXT,
            latitude NUMERIC,
            longitude NUMERIC,
            match_confidence NUMERIC DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        # Make sure country_code column exists (used by improved logic if you later add updates)
        conn.execute(text(f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMN IF NOT EXISTS country_code VARCHAR(5)
        """))

# =====================================================
# LOAD CSL DATA
# =====================================================
def load_csl_hotels():
    print("📘 Loading masterfile Excel...")
    df = pd.read_excel(EXCEL_PATH)

    # Original normalized_name for potential reuse
    df["normalized_name"] = df["Global Property Name"].apply(normalize_name)

    # Ensure string types
    df["Property City Name"] = df["Property City Name"].astype(str)
    df["Property State/Province"] = df["Property State/Province"].astype(str)
    df["Property Country Code"] = df["Property Country Code"].astype(str)

    # Additional normalized fields for RapidFuzz matching
    df["city_norm"] = df["Property City Name"].apply(normalize_text)
    df["hotel_norm"] = df["Global Property Name"].apply(normalize_text)
    df["state_code"] = df["Property State/Province"].str.upper().str.strip()
    df["country_code"] = df["Property Country Code"].str.upper().str.strip()

    return df

# =====================================================
# SCRAPER CORE
# =====================================================
def fetch_property_details(driver, wait):
    try:
        addr_el = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a[href*='google.com/maps'] span, div[data-testid='hotel-address']")
            )
        )
        address = addr_el.text.strip()
    except TimeoutException:
        address = ""
    return address

def scrape_hotels_from_location(driver, wait, location):
    results = []
    print(f"🌍 Scraping location: {location['url']}")

    try:
        driver.get(location["url"])
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(2)

        hotel_cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/en/hotels/']")
        hotel_links = list({card.get_attribute("href") for card in hotel_cards if card.get_attribute("href")})
        print(f"🔗 Found {len(hotel_links)} hotel links")

        for href in hotel_links:
            hotel_data = retry_action(lambda: scrape_single_hotel(driver, wait, href))
            if hotel_data:
                results.append(hotel_data)

    except Exception as e:
        print(f"❌ Failed scraping location {location['url']}: {e}")

    return results


def normalize_state(state):
    return state_to_code(state)

def normalize_country(country):
    return country_to_code(country)


def scrape_single_hotel(driver, wait, href):
    try:
        driver.get(href)
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        time.sleep(1)

        try:
            name = wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1"))).text.strip()
        except TimeoutException:
            name = ""

        address = fetch_property_details(driver, wait)
        city, state, country = parse_address_components(address)

        state_code = normalize_state(state)
        country_code = normalize_country(country)

        return {
            "scraped_name": name,
            "hotel_norm": normalize_text(name),
            "address": address,
            "city": city,
            "city_norm": normalize_text(city),
            "state": state,
            "state_code": state_code,
            "country": country,
            "country_code": country_code,
            "url": href
        }

    except Exception as e:
        print(f"⚠️ Failed hotel {href}: {e}")
        return None


# =====================================================
# MATCH HOTELS (Replaced with RapidFuzz-based logic)
# =====================================================
def match_hotels(scraped_hotels, df_master):
    matched = []

    for item in scraped_hotels:
        if not item:
            continue

        if not item["city_norm"] or not item["country_code"]:
            continue

        candidates = df_master[
            (df_master["city_norm"] == item["city_norm"]) &
            (df_master["country_code"] == item["country_code"])
        ]

        if item["state_code"]:
            candidates = candidates[candidates["state_code"] == item["state_code"]]

        best_score = 0
        best_row = None

        for _, row in candidates.iterrows():
            score = fuzz.token_set_ratio(item["hotel_norm"], row["hotel_norm"])
            if score > best_score:
                best_score = score
                best_row = row

        if best_row is not None and best_score >= FUZZY_THRESHOLD:
            matched.append({
                "hotel_code": best_row["Global Property ID"],
                "scraped_hotel_name": item["scraped_name"],
                "global_property_name": best_row["Global Property Name"],
                "city": item["city"],
                "state": item["state"],
                "state_code": item["state_code"],
                "country": item["country"],
                "country_code": item["country_code"],
                "url": item["url"],
                "address": item["address"],
                "latitude": best_row["Property Latitude"],
                "longitude": best_row["Property Longitude"],
                "match_confidence": float(best_score)
            })
        else:
            matched.append({
                "hotel_code": None,
                "scraped_hotel_name": item["scraped_name"],
                "global_property_name": None,
                "city": item["city"],
                "state": item["state"],
                "state_code": item["state_code"],
                "country": item["country"],
                "country_code": item["country_code"],
                "url": item["url"],
                "address": item["address"],
                "latitude": None,
                "longitude": None,
                "match_confidence": 0.0
            })

    return matched


# =====================================================
# DATABASE SAVE
# =====================================================
def save_to_db(records):
    if not records:
        return

    engine = get_db_engine()
    df = pd.DataFrame(records)

    insert_sql = text(f"""
        INSERT INTO {TABLE_NAME} (
            hotel_code,
            scraped_hotel_name,
            global_property_name,
            city,
            state,
            state_code,
            country,
            country_code,
            url,
            address,
            latitude,
            longitude,
            match_confidence
        ) VALUES (
            :hotel_code,
            :scraped_hotel_name,
            :global_property_name,
            :city,
            :state,
            :state_code,
            :country,
            :country_code,
            :url,
            :address,
            :latitude,
            :longitude,
            :match_confidence
        )
        ON CONFLICT (hotel_code, url) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, df.to_dict(orient="records"))

    print(f"✅ Inserted {len(df)} records")

# =====================================================
# MAIN PROCESS
# =====================================================
def main_scrape_and_map(driver, wait):
    create_table_if_not_exists()
    df_master = load_csl_hotels()
    all_results = []

    try:
        locations = load_locations()
        for loc in locations:
            hotels = scrape_hotels_from_location(driver, wait, loc)
            matched = match_hotels(hotels, df_master)
            save_to_db(matched)
            all_results.extend(matched)

    except Exception as e:
        print("❌ Fatal Error:", e)

    with open("hilton_hotel_url_mapped.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    return all_results

# =====================================================
# FLASK API
# =====================================================
app = Flask(__name__)

@app.route("/run_scrape_and_map", methods=["GET"])
def run_scrape_and_map():
    options = make_uc_options()
    driver = uc.Chrome(options=options)
    wait = WebDriverWait(driver, 25)

    try:
        results = main_scrape_and_map(driver, wait)
        return jsonify({
            "status": "success",
            "count": len(results) if results else 0,
            "message": "Scraping and mapping completed successfully"
        }), 200
    except Exception as e:
        print("❌ Fatal Error:", e)
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    finally:
        driver.quit()

if __name__ == "__main__":
    print("🚀 Starting Hilton Scraper & Mapper API on port 8000...")
    app.run(host="0.0.0.0", port=8000, debug=True)