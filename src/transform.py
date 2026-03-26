import os
import sqlite3
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "db/ecommerce.db")
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH", "data/processed")


def get_connection():
    return sqlite3.connect(DB_PATH)


# ─ Load raw tables ──────────────────────────────────────────────────

def load_raw_tables(conn: sqlite3.Connection) -> dict:
    """
    Load all raw tables from SQLite into a dict of DataFrames.
    load everything at once.
    """
    print("  Loading raw tables from SQLite...")
    tables = {}
    table_names = [
        "orders", "order_items", "customers", "products",
        "sellers", "payments", "reviews", "category_translation",
    ]
    for name in table_names:
        try:
            tables[name] = pd.read_sql(f"SELECT * FROM {name}", conn)
            print(f"    ✅  {name:<28} {len(tables[name]):>8,} rows")
        except Exception as e:
            print(f"    ⚠️  Could not load '{name}': {e}")

    return tables


# ─ Clean each table ─────────────────────────────────────────────────

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    WHY these cleaning steps:
    - Date columns come in as plain strings from CSV. We need datetime objects
      for any time-based analysis (monthly aggregations, delivery time calc).
    - 'order_delivered_customer_date' has nulls for undelivered orders — that's
      expected and valid, so we keep NaT (not drop the row).
    - We filter to 'delivered' status only for revenue analysis since cancelled/
      unavailable orders don't represent actual business performance.
    """
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Drop rows where purchase date is missing — unusable for trend analysis
    df = df.dropna(subset=["order_purchase_timestamp"])

    # Extract date parts for grouping in dashboard
    df["order_year"]       = df["order_purchase_timestamp"].dt.year
    df["order_month"]      = df["order_purchase_timestamp"].dt.month
    df["order_month_name"] = df["order_purchase_timestamp"].dt.strftime("%b %Y")
    df["order_yearmonth"]  = df["order_purchase_timestamp"].dt.to_period("M").astype(str)

    return df


def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    price and freight_value are our revenue columns.
    Negative or zero prices are data errors — we remove them.
    shipping_revenue is a useful derived metric for margin analysis.
    """
    df = df.dropna(subset=["price", "freight_value"])
    df = df[df["price"] > 0]
    df["item_revenue"]    = df["price"] + df["freight_value"]
    df["shipping_pct"]    = (df["freight_value"] / df["item_revenue"] * 100).round(2)
    return df


def clean_products(df: pd.DataFrame, category_translation: pd.DataFrame) -> pd.DataFrame:
    """
    Product names are in Portuguese. We join the translation table to get
    English category names — much more readable on a dashboard shown to others.
    product_name_length and description_length are raw char counts that we won't
    use, so we drop them to keep the table lean.
    """
    df = df.merge(
        category_translation,
        on="product_category_name",
        how="left",
    )
    # Fall back to the Portuguese name if no translation exists
    df["category_en"] = df["product_category_name_english"].fillna(
        df["product_category_name"].fillna("unknown")
    )
    # Normalize: replace underscores with spaces, title case
    df["category_en"] = (
        df["category_en"]
        .str.replace("_", " ", regex=False)
        .str.title()
        .str.strip()
    )

    # Drop high-null columns that add no analytical value
    drop_cols = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    return df


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    review_score is our customer satisfaction KPI (1–5 stars).
    We keep only rows with a valid score (1–5). review_comment_message
    is mostly null and we don't do NLP here, so we drop it.
    """
    df = df[df["review_score"].between(1, 5)]
    if "review_comment_message" in df.columns:
        df = df.drop(columns=["review_comment_message"])
    return df


# ─ Build the master analytics table ─────────────────────────────────

def build_master_table(tables: dict, brl_usd_rate: float = 0.20) -> pd.DataFrame:
    """
    Join all cleaned tables into one wide, analytics-ready DataFrame.

    Join strategy:
    - Start with orders (the central entity — every sale has an order)
    - Left-join order_items (one order can have many items)
    - Left-join products and categories
    - Left-join customers (for geographic analysis)
    - Left-join reviews (for satisfaction analysis)
    - Left-join payments (for payment method analysis)

    We use left joins throughout so orders without items/reviews still appear —
    they give us visibility into incomplete data.
    """
    print("\n  Building master analytics table...")

    orders    = clean_orders(tables["orders"])
    items     = clean_order_items(tables["order_items"])
    products  = clean_products(tables["products"], tables.get("category_translation", pd.DataFrame()))
    customers = tables.get("customers", pd.DataFrame())
    reviews   = clean_reviews(tables.get("reviews", pd.DataFrame()))
    payments  = tables.get("payments", pd.DataFrame())

    # -- Join orders + items
    df = orders.merge(items, on="order_id", how="left")

    # -- Join products (to get category)
    if not products.empty:
        df = df.merge(
            products[["product_id", "category_en", "product_weight_g"]],
            on="product_id",
            how="left",
        )

    # -- Join customers (to get state for geographic analysis)
    if not customers.empty:
        df = df.merge(
            customers[["customer_id", "customer_city", "customer_state"]],
            on="customer_id",
            how="left",
        )

    # -- Aggregate payments to order level (one row per order)
    if not payments.empty:
        pay_agg = (
            payments.groupby("order_id")
            .agg(
                payment_type=("payment_type", "first"),
                payment_installments=("payment_installments", "max"),
                total_payment=("payment_value", "sum"),
            )
            .reset_index()
        )
        df = df.merge(pay_agg, on="order_id", how="left")

    # -- Join review score (one score per order)
    if not reviews.empty:
        rev_agg = (
            reviews.groupby("order_id")["review_score"].mean().round(1).reset_index()
        )
        df = df.merge(rev_agg, on="order_id", how="left")

    # -- Final feature engineering
    # Delivery time in days (for performance analysis)
    if "order_delivered_customer_date" in df.columns:
        df["delivery_days"] = (
            (df["order_delivered_customer_date"] - df["order_purchase_timestamp"])
            .dt.days
        )
        df["estimated_days"] = (
            (df["order_estimated_delivery_date"] - df["order_purchase_timestamp"])
            .dt.days
        )
        # On-time flag: delivered before or on estimated date
        df["on_time"] = df["order_delivered_customer_date"] <= df["order_estimated_delivery_date"]

    # Revenue in USD for international readability
    if "item_revenue" in df.columns:
        df["item_revenue_usd"] = (df["item_revenue"] * brl_usd_rate).round(2)
        df["price_usd"]        = (df["price"] * brl_usd_rate).round(2)

    # Fill nulls in category with 'Unknown' so groupby doesn't drop rows
    if "category_en" in df.columns:
        df["category_en"] = df["category_en"].fillna("Unknown")

    print(f"    ✅  Master table: {len(df):,} rows × {len(df.columns)} columns")
    return df


# ─ Save processed data ─────────────────────────────────────────────

def save_processed(df: pd.DataFrame, conn: sqlite3.Connection):
    
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

    # SQLite
    df.to_sql("analytics_master", conn, if_exists="replace", index=False)
    print("    ✅  Saved to SQLite as 'analytics_master'")

    # CSV backup
    out_path = os.path.join(PROCESSED_DATA_PATH, "analytics_master.csv")
    df.to_csv(out_path, index=False)
    print(f"    ✅  Saved CSV to {out_path}")


# ─ Main entry point ──────────────────────────────────────────────────────────

def run_transform():
    print("=" * 55)
    print("  STEP 2 — TRANSFORM & CLEAN")
    print("=" * 55)

    conn = get_connection()

    # Load the exchange rate we scraped during ingestion
    try:
        rate_df = pd.read_sql("SELECT rate FROM exchange_rates WHERE currency_pair='BRL_USD'", conn)
        brl_usd_rate = float(rate_df["rate"].iloc[0])
        print(f"\n  Exchange rate: 1 BRL = {brl_usd_rate:.4f} USD")
    except Exception:
        brl_usd_rate = 0.20
        print("\n  Exchange rate: using fallback 1 BRL = 0.20 USD")

    tables = load_raw_tables(conn)
    master = build_master_table(tables, brl_usd_rate)

    print("\n  Saving processed data...")
    save_processed(master, conn)
    conn.close()

    print(f"\n✅  Transform complete.\n")
    return True


if __name__ == "__main__":
    run_transform()
