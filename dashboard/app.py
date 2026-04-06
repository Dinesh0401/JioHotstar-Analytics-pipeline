"""JioHotstar Streaming Analytics Dashboard — reads Gold Delta tables via PySpark."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession

from spark.conf.spark_settings import get_gold_path, get_silver_path, get_spark_session

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="JioHotstar Analytics",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Dark theme CSS ───────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0f172a; }
    .stApp { background-color: #0f172a; color: #e2e8f0; }
    h1, h2, h3 { color: #f8fafc !important; }
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border-radius: 12px; padding: 20px; text-align: center;
        border: 1px solid #475569;
    }
    .metric-value { font-size: 2.2rem; font-weight: 700; color: #38bdf8; }
    .metric-label { font-size: 0.85rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; }
    .live-badge {
        background: #ef4444; color: white; padding: 4px 12px; border-radius: 20px;
        font-weight: 700; font-size: 0.75rem; display: inline-block; animation: pulse 2s infinite;
    }
    @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.6; } }
    .section-header {
        background: linear-gradient(90deg, #1e293b, transparent);
        padding: 8px 16px; border-left: 4px solid #38bdf8; margin: 20px 0 10px 0;
        font-size: 1.1rem; font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def load_spark():
    """Create SparkSession once and cache it."""
    return get_spark_session("Dashboard")


@st.cache_data(ttl=300)
def load_gold_table(_spark, table_name):
    """Load a Gold Delta table as a pandas DataFrame."""
    return _spark.read.format("delta").load(get_gold_path(table_name)).toPandas()


@st.cache_data(ttl=300)
def load_silver_table(_spark, table_name):
    """Load a Silver Delta table as a pandas DataFrame."""
    return _spark.read.format("delta").load(get_silver_path(table_name)).toPandas()


def render_metric(label, value, prefix="", suffix=""):
    """Render a styled metric card."""
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{prefix}{value}{suffix}</div>
        <div class="metric-label">{label}</div>
    </div>
    """, unsafe_allow_html=True)


def main():
    spark = load_spark()

    # ── Header ───────────────────────────────────────────────────────────────
    col_title, col_badge = st.columns([6, 1])
    with col_title:
        st.markdown("# JioHotstar Streaming Analytics")
        st.markdown("Real-time IPL & OTT platform intelligence")
    with col_badge:
        st.markdown('<br><span class="live-badge">LIVE</span>', unsafe_allow_html=True)

    st.markdown("---")

    # ── Load data ────────────────────────────────────────────────────────────
    dau = load_gold_table(spark, "daily_active_users")
    cwm = load_gold_table(spark, "content_watch_metrics")
    genre = load_gold_table(spark, "genre_popularity")
    engage = load_gold_table(spark, "user_engagement")
    subs = load_gold_table(spark, "subscription_metrics")
    ratings = load_gold_table(spark, "content_ratings_summary")
    users = load_silver_table(spark, "users")

    # ── Top KPI Cards ────────────────────────────────────────────────────────
    total_views = int(dau["total_views"].sum())
    total_dau_avg = int(dau["daily_active_users"].mean())
    total_watch_hrs = int(dau["total_watch_time_sec"].sum() / 3600)
    active_subs = int(subs["active_subscriptions"].sum())

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        render_metric("Avg Daily Active Users", f"{total_dau_avg:,}")
    with k2:
        render_metric("Total Views", f"{total_views:,}")
    with k3:
        render_metric("Total Watch Time", f"{total_watch_hrs:,}", suffix=" hrs")
    with k4:
        render_metric("Active Subscriptions", f"{active_subs:,}")

    st.markdown("")

    # ── Row 1: DAU Timeline + Genre Popularity ───────────────────────────────
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown('<div class="section-header">Daily Active Users</div>', unsafe_allow_html=True)
        dau_sorted = dau.sort_values("event_date")
        fig_dau = px.area(
            dau_sorted, x="event_date", y="daily_active_users",
            color_discrete_sequence=["#38bdf8"],
        )
        fig_dau.update_layout(
            plot_bgcolor="#1e293b", paper_bgcolor="#0f172a",
            font_color="#e2e8f0", xaxis_title="", yaxis_title="Users",
            margin=dict(l=40, r=20, t=20, b=40),
            xaxis=dict(gridcolor="#334155"), yaxis=dict(gridcolor="#334155"),
        )
        st.plotly_chart(fig_dau, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">Genre Popularity</div>', unsafe_allow_html=True)
        genre_sorted = genre.sort_values("total_views", ascending=False).head(10)
        fig_genre = px.pie(
            genre_sorted, values="total_views", names="genre",
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.4,
        )
        fig_genre.update_layout(
            plot_bgcolor="#1e293b", paper_bgcolor="#0f172a",
            font_color="#e2e8f0", margin=dict(l=20, r=20, t=20, b=20),
            legend=dict(font_size=11),
        )
        st.plotly_chart(fig_genre, use_container_width=True)

    # ── Row 2: Top Content + Ratings ─────────────────────────────────────────
    col3, col4 = st.columns(2)

    with col3:
        st.markdown('<div class="section-header">Top 15 Content by Views</div>', unsafe_allow_html=True)
        top_content = cwm.nlargest(15, "total_views")
        # Truncate long titles
        top_content = top_content.copy()
        top_content["short_title"] = top_content["title"].str[:30]
        fig_content = px.bar(
            top_content.sort_values("total_views"),
            x="total_views", y="short_title", orientation="h",
            color="total_views", color_continuous_scale="blues",
        )
        fig_content.update_layout(
            plot_bgcolor="#1e293b", paper_bgcolor="#0f172a",
            font_color="#e2e8f0", xaxis_title="Views", yaxis_title="",
            margin=dict(l=20, r=20, t=20, b=40),
            coloraxis_showscale=False, showlegend=False,
            xaxis=dict(gridcolor="#334155"), yaxis=dict(gridcolor="#334155"),
        )
        st.plotly_chart(fig_content, use_container_width=True)

    with col4:
        st.markdown('<div class="section-header">Top 15 Rated Content</div>', unsafe_allow_html=True)
        top_rated = ratings[ratings["rating_count"] >= 5].nlargest(15, "avg_rating")
        top_rated = top_rated.copy()
        top_rated["short_title"] = top_rated["title"].str[:30]
        fig_rated = px.bar(
            top_rated.sort_values("avg_rating"),
            x="avg_rating", y="short_title", orientation="h",
            color="avg_rating", color_continuous_scale="oranges",
        )
        fig_rated.update_layout(
            plot_bgcolor="#1e293b", paper_bgcolor="#0f172a",
            font_color="#e2e8f0", xaxis_title="Avg Rating", yaxis_title="",
            margin=dict(l=20, r=20, t=20, b=40),
            coloraxis_showscale=False, showlegend=False,
            xaxis=dict(gridcolor="#334155", range=[0, 5]),
            yaxis=dict(gridcolor="#334155"),
        )
        st.plotly_chart(fig_rated, use_container_width=True)

    # ── Row 3: Subscriptions + Device + Country ──────────────────────────────
    col5, col6, col7 = st.columns(3)

    with col5:
        st.markdown('<div class="section-header">Subscription Plans</div>', unsafe_allow_html=True)
        plan_order = ["FREE", "BASIC", "PREMIUM", "VIP"]
        subs_sorted = subs.copy()
        subs_sorted["plan_id"] = subs_sorted["plan_id"].astype("category")
        fig_subs = go.Figure()
        fig_subs.add_trace(go.Bar(
            x=subs_sorted["plan_id"], y=subs_sorted["active_subscriptions"],
            name="Active", marker_color="#22c55e",
        ))
        fig_subs.add_trace(go.Bar(
            x=subs_sorted["plan_id"], y=subs_sorted["cancelled_subscriptions"],
            name="Cancelled", marker_color="#ef4444",
        ))
        fig_subs.update_layout(
            barmode="stack", plot_bgcolor="#1e293b", paper_bgcolor="#0f172a",
            font_color="#e2e8f0", margin=dict(l=40, r=20, t=20, b=40),
            legend=dict(orientation="h", y=1.1), xaxis_title="", yaxis_title="Count",
            xaxis=dict(gridcolor="#334155"), yaxis=dict(gridcolor="#334155"),
        )
        st.plotly_chart(fig_subs, use_container_width=True)

    with col6:
        st.markdown('<div class="section-header">Device Usage</div>', unsafe_allow_html=True)
        device_df = users.groupby("device_category").size().reset_index(name="count")
        fig_device = px.pie(
            device_df, values="count", names="device_category",
            color_discrete_sequence=["#38bdf8", "#a78bfa", "#fb923c"],
            hole=0.45,
        )
        fig_device.update_layout(
            plot_bgcolor="#1e293b", paper_bgcolor="#0f172a",
            font_color="#e2e8f0", margin=dict(l=20, r=20, t=20, b=20),
        )
        st.plotly_chart(fig_device, use_container_width=True)

    with col7:
        st.markdown('<div class="section-header">Viewers by Country</div>', unsafe_allow_html=True)
        country_df = users.groupby("country").size().reset_index(name="viewers")
        country_sorted = country_df.sort_values("viewers", ascending=False)
        fig_country = px.bar(
            country_sorted, x="country", y="viewers",
            color="viewers", color_continuous_scale="teal",
        )
        fig_country.update_layout(
            plot_bgcolor="#1e293b", paper_bgcolor="#0f172a",
            font_color="#e2e8f0", xaxis_title="", yaxis_title="Users",
            margin=dict(l=40, r=20, t=20, b=40),
            coloraxis_showscale=False, showlegend=False,
            xaxis=dict(gridcolor="#334155", tickangle=45),
            yaxis=dict(gridcolor="#334155"),
        )
        st.plotly_chart(fig_country, use_container_width=True)

    # ── Row 4: Watch Time Distribution ───────────────────────────────────────
    st.markdown('<div class="section-header">User Watch Time Distribution</div>', unsafe_allow_html=True)
    engage_hrs = engage.copy()
    engage_hrs["watch_hours"] = engage_hrs["total_watch_time_sec"] / 3600
    fig_hist = px.histogram(
        engage_hrs, x="watch_hours", nbins=50,
        color_discrete_sequence=["#a78bfa"],
    )
    fig_hist.update_layout(
        plot_bgcolor="#1e293b", paper_bgcolor="#0f172a",
        font_color="#e2e8f0", xaxis_title="Total Watch Time (hours)",
        yaxis_title="Number of Users",
        margin=dict(l=40, r=20, t=20, b=40),
        xaxis=dict(gridcolor="#334155"), yaxis=dict(gridcolor="#334155"),
    )
    st.plotly_chart(fig_hist, use_container_width=True)

    # ── Footer ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(
        '<div style="text-align:center; color:#64748b; font-size:0.8rem;">'
        'JioHotstar Streaming Analytics | Medallion Lakehouse Architecture | '
        'Bronze → Silver → Gold | PySpark + Delta Lake + AWS S3'
        '</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
