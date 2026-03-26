"""
dashboard.py: Interactive Dashboard
=============================================
Streamlit + Plotly web app that turns the processed data into
business insights. Every chart answers a specific business question.

Run with:  streamlit run src/dashboard.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

import queries as q

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Retail Sales Insight",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🛒 Retail Sales")
    st.caption("Brazilian E-Commerce · Olist Dataset")
    st.divider()

    st.markdown("**Filters**")

    # Year filter
    year_options = ["All Years", "2016", "2017", "2018"]
    selected_year = st.selectbox("Year", year_options)

    # Top N for category chart
    top_n = st.slider("Top N categories", min_value=5, max_value=20, value=10)

    st.divider()
    st.caption(
        "Data: [Olist / Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)"
    )
    st.caption("Pipeline: pandas → SQLite → Streamlit")

# ── Load data (cached so Streamlit doesn't re-query on every interaction) ─────

@st.cache_data(ttl=300)
def load_all():
    return {
        "kpis":         q.get_kpi_summary(),
        "monthly":      q.get_monthly_revenue(),
        "categories":   q.get_top_categories(20),
        "states":       q.get_revenue_by_state(),
        "payments":     q.get_payment_breakdown(),
        "delivery":     q.get_delivery_performance(),
        "reviews":      q.get_review_distribution(),
        "late_review":  q.get_late_vs_review(),
    }

try:
    data = load_all()
except Exception as e:
    st.error(
        f"**Could not connect to database.**\n\n"
        f"Make sure you've run the pipeline first:\n\n"
        f"```\npython run_pipeline.py\n```\n\n"
        f"Error: {e}"
    )
    st.stop()

# ── Apply year filter to monthly dataframes ───────────────────────────────────

def filter_by_year(df, year_col="order_year"):
    if selected_year == "All Years":
        return df
    return df[df[year_col] == int(selected_year)]

monthly    = filter_by_year(data["monthly"])
delivery   = filter_by_year(data["delivery"])
categories = data["categories"].head(top_n)

# ── Header ────────────────────────────────────────────────────────────────────

st.title("🛒 Retail Sales Insight Dashboard")
st.caption(
    "End-to-end analytics pipeline: CSV ingestion -> SQLite -> pandas cleaning -> interactive insights"
)
st.divider()

# ── Row 1: KPI Cards ──────────────────────────────────────────────────────────

kpis = data["kpis"]
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        label="Total Orders",
        value=f"{int(kpis.get('total_orders', 0)):,}",
    )
with col2:
    st.metric(
        label="Total Revenue (USD)",
        value=f"${kpis.get('total_revenue_usd', 0):,.0f}",
    )
with col3:
    st.metric(
        label="Avg Order Value",
        value=f"${kpis.get('avg_order_value_usd', 0):.2f}",
    )
with col4:
    score = kpis.get("avg_review_score", 0)
    st.metric(
        label="Avg Review Score",
        value=f"{score:.1f} / 5.0" if score else "N/A",
    )
with col5:
    on_time = kpis.get("on_time_pct", 0)
    st.metric(
        label="On-Time Delivery",
        value=f"{on_time:.1f}%" if on_time else "N/A",
    )

st.divider()

# ── Row 2: Revenue Trend + Payment Breakdown ──────────────────────────────────

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📈 Monthly Revenue Trend")
    st.caption("Business question: Is revenue growing? Where are the peaks?")

    if not monthly.empty:
        fig_trend = px.line(
            monthly,
            x="order_yearmonth",
            y="revenue_usd",
            markers=True,
            labels={"order_yearmonth": "Month", "revenue_usd": "Revenue (USD)"},
            color_discrete_sequence=["#636EFA"],
        )
        fig_trend.update_traces(
            line_width=2.5,
            marker_size=6,
            hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>",
        )
        fig_trend.update_layout(
            xaxis_tickangle=-45,
            xaxis_title=None,
            yaxis_title="Revenue (USD)",
            hovermode="x unified",
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No data for the selected year.")

with col_right:
    st.subheader("💳 Payment Methods")
    st.caption("Business question: How do customers prefer to pay?")

    payments = data["payments"]
    if not payments.empty:
        fig_pay = px.pie(
            payments,
            names="payment_type",
            values="order_count",
            hole=0.45,
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig_pay.update_traces(
            textposition="outside",
            textinfo="percent+label",
            hovertemplate="<b>%{label}</b><br>Orders: %{value:,}<br>Share: %{percent}<extra></extra>",
        )
        fig_pay.update_layout(
            showlegend=False,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig_pay, use_container_width=True)

# ── Row 3: Top Categories ────────────────────────────────────────────

st.subheader("🏆 Top Product Categories by Revenue")
st.caption("Business question: Which categories drive most of the revenue? (Pareto principle)")

if not categories.empty:
    fig_cat = go.Figure()

    # Bar: revenue per category
    fig_cat.add_trace(go.Bar(
        x=categories["category"],
        y=categories["revenue_usd"],
        name="Revenue (USD)",
        marker_color="#636EFA",
        hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>",
    ))

    # Line: cumulative %  (Pareto line on secondary axis)
    fig_cat.add_trace(go.Scatter(
        x=categories["category"],
        y=categories["cumulative_pct"],
        name="Cumulative %",
        mode="lines+markers",
        marker_color="#EF553B",
        line_width=2,
        yaxis="y2",
        hovertemplate="Cumulative: %{y:.1f}%<extra></extra>",
    ))

    # 80% reference line
    fig_cat.add_hline(
        y=80,
        line_dash="dash",
        line_color="orange",
        annotation_text="80% mark",
        yref="y2",
    )

    fig_cat.update_layout(
        xaxis_tickangle=-35,
        xaxis_title=None,
        yaxis_title="Revenue (USD)",
        yaxis2=dict(title="Cumulative %", overlaying="y", side="right", range=[0, 105]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig_cat, use_container_width=True)

st.divider()

# ── Row 4: Delivery Performance + Review Distribution ─────────────────────────

col_del, col_rev = st.columns(2)

with col_del:
    st.subheader("🚚 Delivery Performance")
    st.caption("Business question: Is logistics improving over time?")

    if not delivery.empty:
        fig_del = px.bar(
            delivery,
            x="order_yearmonth",
            y="on_time_pct",
            labels={"order_yearmonth": "Month", "on_time_pct": "On-time %"},
            color="on_time_pct",
            color_continuous_scale="RdYlGn",
            range_color=[50, 100],
        )
        fig_del.add_hline(y=80, line_dash="dash", line_color="gray",
                            annotation_text="80% target")
        fig_del.update_layout(
            xaxis_tickangle=-45,
            xaxis_title=None,
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig_del, use_container_width=True)

with col_rev:
    st.subheader("⭐ Review Score Distribution")
    st.caption("Business question: What does customer satisfaction look like?")

    reviews = data["reviews"]
    if not reviews.empty:
        colors = ["#EF553B", "#FFA15A", "#FECB52", "#00CC96", "#636EFA"]
        fig_rev = px.bar(
            reviews,
            x="score_label",
            y="review_count",
            labels={"score_label": "Rating", "review_count": "Reviews"},
            color="score_label",
            color_discrete_sequence=colors,
        )
        fig_rev.update_layout(
            showlegend=False,
            xaxis_title=None,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig_rev, use_container_width=True)

# ── Row 5: Late Delivery Impact + State Revenue ───────────────────────────────

col_late, col_state = st.columns(2)

with col_late:
    st.subheader("📦 Late Delivery Impact on Reviews")
    st.caption("Business question: Does late delivery hurt customer satisfaction?")

    late_review = data["late_review"]
    if not late_review.empty:
        fig_late = px.bar(
            late_review,
            x="delivery_status",
            y="avg_review_score",
            color="delivery_status",
            text="avg_review_score",
            labels={"delivery_status": "", "avg_review_score": "Avg Review Score"},
            color_discrete_map={"On time": "#00CC96", "Late": "#EF553B"},
        )
        fig_late.update_traces(
            texttemplate="%{text:.2f} ★",
            textposition="outside",
        )
        fig_late.update_layout(
            showlegend=False,
            yaxis_range=[0, 5.5],
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig_late, use_container_width=True)

with col_state:
    st.subheader("🗺️ Revenue by State")
    st.caption("Business question: Where is our customer base concentrated?")

    states = data["states"]
    if not states.empty:
        fig_state = px.bar(
            states.head(15),
            x="state",
            y="revenue_usd",
            color="revenue_usd",
            color_continuous_scale="Blues",
            labels={"state": "State", "revenue_usd": "Revenue (USD)"},
        )
        fig_state.update_layout(
            coloraxis_showscale=False,
            xaxis_title=None,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig_state, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "Built with Python · pandas · SQLite · Streamlit · Plotly  |  "
    "Data: Olist Brazilian E-Commerce (Kaggle, public domain)"
)
