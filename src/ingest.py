"""
ingest.py — Step 1: Extract & Load
====================================
Responsibility: Read raw CSV files and load them into SQLite as-is.
This step does NOT clean or transform data — that is transform.py's job.
Keeping extraction and transformation separate makes each step easier to debug.

Data source: Brazilian E-Commerce Public Dataset by Olist (Kaggle)
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
"""

import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read paths from .env (with sensible defaults if not set)
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/raw")
DB_PATH = os.getenv("DB_PATH", "db/ecommerce.db")


# ── Helper ────────────────────────────────────────────────────────────────────

def get_connection():
    """Create and return a SQLite connection. Creates the db file if missing."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)


# ── Download from Kaggle (optional) ──────────────────────────────────

def download_dataset():
    """
    Uses the Kaggle API to download the dataset automatically.
    Requires KAGGLE_USERNAME and KAGGLE_KEY in your .env file.
    Skip this if you already placed CSVs manually in data/raw/.
    """
    import kaggle  # only imported if you call this function

    print("Downloading dataset from Kaggle...")
    os.makedirs(RAW_DATA_PATH, exist_ok=True)

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        "olistbr/brazilian-ecommerce",
        path=RAW_DATA_PATH,
        unzip=True,
    )
    print("✅  Download complete.\n")


# ── Define which files to ingest and their table names ───────────────

# Each entry: (csv_filename, sqlite_table_name)
# We map every Olist CSV to a clear, short table name.
CSV_TABLE_MAP = [
    ("olist_orders_dataset.csv",               "orders"),
    ("olist_order_items_dataset.csv",           "order_items"),
    ("olist_customers_dataset.csv",             "customers"),
    ("olist_products_dataset.csv",              "products"),
    ("olist_sellers_dataset.csv",               "sellers"),
    ("olist_order_payments_dataset.csv",        "payments"),
    ("olist_order_reviews_dataset.csv",         "reviews"),
    ("product_category_name_translation.csv",   "category_translation"),
]


# ── Load each CSV into SQLite ────────────────────────────────────────

def load_csv_to_db(conn: sqlite3.Connection, csv_file: str, table_name: str):
    """
    Read one CSV file with pandas and write it into SQLite.

    """
    filepath = os.path.join(RAW_DATA_PATH, csv_file)

    if not os.path.exists(filepath):
        print(f"  ⚠️  File not found, skipping: {filepath}")
        return

    df = pd.read_csv(filepath, encoding="utf-8")
    df.to_sql(table_name, conn, if_exists="replace", index=False)

    print(f"  ✅  {table_name:<28} {df.shape[0]:>8,} rows  {df.shape[1]:>3} cols")


def run_ingestion(auto_download: bool = False):
    """
    Main ingestion entry point.
    - auto_download=True  → downloads from Kaggle first (needs .env credentials)
    - auto_download=False → expects CSVs already in data/raw/
    """
    print("=" * 55)
    print("  STEP 1 — INGESTION")
    print("=" * 55)

    if auto_download:
        download_dataset()

    # Verify raw data folder exists and has files
    if not os.path.exists(RAW_DATA_PATH) or not os.listdir(RAW_DATA_PATH):
        print(
            f"\n❌  No files found in '{RAW_DATA_PATH}'.\n"
            "    Either:\n"
            "    1. Place Kaggle CSVs manually in data/raw/\n"
            "    2. Set KAGGLE credentials in .env and run with auto_download=True\n"
        )
        return False

    conn = get_connection()
    print(f"\nLoading CSVs into: {DB_PATH}\n")

    for csv_file, table_name in CSV_TABLE_MAP:
        load_csv_to_db(conn, csv_file, table_name)

    conn.close()

    print(f"\n✅  Ingestion complete. Database: {DB_PATH}\n")
    return True


# ── Optional: Scrape live BRL -> USD exchange rate ─────────────────────────────

def scrape_exchange_rate() -> float:
    """
    Lightweight web scrape to fetch the current BRL/USD exchange rate.
    Used to add a USD-converted revenue column in transform.py.
    """
    import requests
    from bs4 import BeautifulSoup

    url = "https://www.x-rates.com/calculator/?from=BRL&to=USD&amount=1"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        print("🌐  Scraping BRL -> USD exchange rate...")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        rate_tag = soup.find("span", class_="ccOutputRslt")

        if rate_tag:
            # Extract the numeric part before the currency code
            rate_text = rate_tag.get_text(strip=True).split()[0]
            rate = float(rate_text)
            print(f"  ✅  1 BRL = {rate:.4f} USD\n")
            return rate
        else:
            print("  ⚠️  Could not parse rate, using fallback 0.20\n")
            return 0.20

    except Exception as e:
        print(f"  ⚠️  Scrape failed ({e}), using fallback 0.20\n")
        return 0.20


def save_exchange_rate(rate: float):
    """Persist the scraped rate into SQLite so the dashboard can read it."""
    conn = get_connection()
    df = pd.DataFrame([{"currency_pair": "BRL_USD", "rate": rate}])
    df.to_sql("exchange_rates", conn, if_exists="replace", index=False)
    conn.close()


# ─ Run directly ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    success = run_ingestion(auto_download=False)
    if success:
        rate = scrape_exchange_rate()
        save_exchange_rate(rate)
