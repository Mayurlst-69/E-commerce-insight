"""
queries.py — SQL Layer
========================
Responsibility: All SQL queries live here — not scattered in the dashboard file.
Each function returns a clean pandas DataFrame ready for Plotly.

WHY centralise queries?
- Dashboard code stays readable (chart logic, not SQL strings)
- Easy to swap SQLite for PostgreSQL later — only this file changes
- Each query is independently testable
"""

import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "db/ecommerce.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


# ─ KPI Summary ───────────────────────────────────────────────────────────────

def get_kpi_summary() -> dict:
    """
    Returns top-level business KPIs for the dashboard header cards.
    These are the first numbers a business owner would ask for.
    """
    conn = get_connection()
    sql = """
        SELECT
            COUNT(DISTINCT order_id)                        AS total_orders,
            ROUND(SUM(item_revenue_usd), 2)                 AS total_revenue_usd,
            ROUND(AVG(item_revenue_usd), 2)                 AS avg_order_value_usd,
            ROUND(AVG(review_score), 2)                     AS avg_review_score,
            ROUND(
                100.0 * SUM(CASE WHEN on_time = 1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(CASE WHEN on_time IS NOT NULL THEN 1 END), 0)
            , 1)                                            AS on_time_pct
        FROM analytics_master
        WHERE order_status = 'delivered'
    """
    row = pd.read_sql(sql, conn).iloc[0]
    conn.close()
    return row.to_dict()


# ─ Revenue Trend ─────────────────────────────────────────────────────────────

def get_monthly_revenue() -> pd.DataFrame:
    """
    Monthly revenue trend (USD).
    Insight: Spot seasonality peaks and troughs for inventory planning.
    """
    conn = get_connection()
    sql = """
        SELECT
            order_yearmonth,
            order_year,
            order_month,
            ROUND(SUM(item_revenue_usd), 2)  AS revenue_usd,
            COUNT(DISTINCT order_id)          AS order_count
        FROM analytics_master
        WHERE order_status = 'delivered'
            AND order_yearmonth IS NOT NULL
        GROUP BY order_yearmonth, order_year, order_month
        ORDER BY order_year, order_month
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


# ── Top Product Categories ────────────────────────────────────────────────────

def get_top_categories(top_n: int = 15) -> pd.DataFrame:
    """
    Top N product categories by total revenue (USD).
    Insight: Pareto analysis — which categories drive 80% of revenue?
    """
    conn = get_connection()
    sql = f"""
        SELECT
            category_en                         AS category,
            ROUND(SUM(item_revenue_usd), 2)     AS revenue_usd,
            COUNT(DISTINCT order_id)            AS order_count,
            ROUND(AVG(price_usd), 2)            AS avg_price_usd,
            ROUND(AVG(review_score), 2)         AS avg_review
        FROM analytics_master
        WHERE order_status = 'delivered'
            AND category_en IS NOT NULL
            AND category_en != 'Unknown'
        GROUP BY category_en
        ORDER BY revenue_usd DESC
        LIMIT {top_n}
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    # Add cumulative revenue % for Pareto line
    df["cumulative_pct"] = (df["revenue_usd"].cumsum() / df["revenue_usd"].sum() * 100).round(1)
    return df


# ─ Customer Geography ────────────────────────────────────────────────────────

def get_revenue_by_state() -> pd.DataFrame:
    """
    Revenue and order count by Brazilian state.
    Insight: Where is the customer base concentrated? Where to expand?
    """
    conn = get_connection()
    sql = """
        SELECT
            customer_state                       AS state,
            COUNT(DISTINCT order_id)             AS order_count,
            ROUND(SUM(item_revenue_usd), 2)      AS revenue_usd,
            ROUND(AVG(review_score), 2)          AS avg_review
        FROM analytics_master
        WHERE order_status = 'delivered'
            AND customer_state IS NOT NULL
        GROUP BY customer_state
        ORDER BY revenue_usd DESC
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


# ─ Payment Methods ───────────────────────────────────────────────────────────

def get_payment_breakdown() -> pd.DataFrame:
    """
    Order volume and average value by payment method.
    Insight: Credit card vs boleto — does payment method correlate with higher spend?
    """
    conn = get_connection()
    sql = """
        SELECT
            payment_type,
            COUNT(DISTINCT order_id)         AS order_count,
            ROUND(SUM(item_revenue_usd), 2)  AS revenue_usd,
            ROUND(AVG(total_payment), 2)     AS avg_payment_brl
        FROM analytics_master
        WHERE order_status = 'delivered'
            AND payment_type IS NOT NULL
            AND payment_type != 'not_defined'
        GROUP BY payment_type
        ORDER BY order_count DESC
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    # Clean up label names for the chart
    df["payment_type"] = df["payment_type"].str.replace("_", " ").str.title()
    return df


# ─ Delivery Performance ──────────────────────────────────────────────────────

def get_delivery_performance() -> pd.DataFrame:
    """
    On-time delivery rate by month.
    Insight: Is logistics improving over time? How does late delivery affect reviews?
    """
    conn = get_connection()
    sql = """
        SELECT
            order_yearmonth,
            order_year,
            order_month,
            COUNT(*)                                         AS total_orders,
            SUM(CASE WHEN on_time = 1 THEN 1 ELSE 0 END)   AS on_time_count,
            ROUND(
                100.0 * SUM(CASE WHEN on_time = 1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0)
            , 1)                                            AS on_time_pct,
            ROUND(AVG(delivery_days), 1)                    AS avg_delivery_days
        FROM analytics_master
        WHERE order_status = 'delivered'
            AND on_time IS NOT NULL
            AND order_yearmonth IS NOT NULL
        GROUP BY order_yearmonth, order_year, order_month
        ORDER BY order_year, order_month
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


# ─ Review Score Distribution ─────────────────────────────────────────────────

def get_review_distribution() -> pd.DataFrame:
    """
    Distribution of 1–5 star reviews.
    Insight: Understand overall customer satisfaction shape.
    """
    conn = get_connection()
    sql = """
        SELECT
            CAST(review_score AS INT)       AS score,
            COUNT(*)                        AS review_count
        FROM analytics_master
        WHERE review_score IS NOT NULL
        GROUP BY CAST(review_score AS INT)
        ORDER BY score
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    df["score_label"] = df["score"].astype(str) + " ★"
    return df


# ─ Late Delivery vs Review Score ─────────────────────────────────────────────

def get_late_vs_review() -> pd.DataFrame:
    """
    Average review score for on-time vs late deliveries.
    Insight: Quantifies how much logistics quality impacts customer satisfaction.
    """
    conn = get_connection()
    sql = """
        SELECT
            CASE WHEN on_time = 1 THEN 'On time' ELSE 'Late' END AS delivery_status,
            ROUND(AVG(review_score), 2)  AS avg_review_score,
            COUNT(*)                     AS order_count
        FROM analytics_master
        WHERE review_score IS NOT NULL
            AND on_time IS NOT NULL
        GROUP BY on_time
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


if __name__ == "__main__":
    # Quick smoke test — run this to verify queries work after transform
    print("Running query smoke tests...\n")
    print("KPIs:", get_kpi_summary())
    print("\nMonthly revenue (first 3 rows):")
    print(get_monthly_revenue().head(3))
    print("\nTop 5 categories:")
    print(get_top_categories(5))
    print("\nPayment breakdown:")
    print(get_payment_breakdown())
    print("\nAll queries OK ✅")
