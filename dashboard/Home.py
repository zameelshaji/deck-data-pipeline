"""DECK Analytics Dashboard - Executive Overview (Home Page)"""

import streamlit as st
import pandas as pd
from datetime import datetime
from utils.data_loader import (
    load_homepage_totals,
    load_latest_mau,
    load_latest_wau,
    load_referral_metrics,
)
from utils.styling import apply_deck_branding, add_deck_footer

# Page configuration
st.set_page_config(
    page_title="DECK Analytics - Executive Overview",
    page_icon="🎴",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("🎴 DECK Analytics - Executive Overview")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

try:
    # Load data — gold_homepage_totals is the single source of truth
    homepage = load_homepage_totals()
    mau_data = load_latest_mau()
    wau_data = load_latest_wau()
    referral_data = load_referral_metrics()

    if homepage.empty:
        st.warning("⚠️ No data available. Run `dbt run` to populate gold_homepage_totals.")
        st.stop()

    # Extract values from the single-row homepage totals table
    h = homepage.iloc[0]
    mau = mau_data.iloc[0] if not mau_data.empty else None
    wau = wau_data.iloc[0] if not wau_data.empty else None
    referral_metrics = referral_data.iloc[0] if not referral_data.empty else None

    # Display last updated
    st.caption(f"📅 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    st.divider()

    # ============================================
    # KEY METRICS - 3 rows x 3 columns
    # ============================================
    st.subheader("📈 Key Metrics")

    # Add custom CSS for enhanced metrics
    st.markdown("""
        <style>
        .home-metric [data-testid="stMetricValue"] {
            font-size: 2.5rem !important;
            font-weight: 600 !important;
        }
        .home-metric [data-testid="stMetricLabel"] {
            font-size: 1rem !important;
            font-weight: 500 !important;
        }
        .home-metric [data-testid="metric-container"] {
            padding: 1.5rem !important;
        }
        </style>
    """, unsafe_allow_html=True)

    # Wrap metrics in div with home-metric class
    st.markdown('<div class="home-metric">', unsafe_allow_html=True)

    # Row 1
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="👥 Total Users",
            value=f"{int(h['total_signups']):,}",
            delta=None,
            help="Total number of registered users on the platform"
        )

    with col2:
        st.metric(
            label="✅ Activated Users",
            value=f"{int(h['total_activated_users']):,}",
            delta=None,
            help="Total number of users who have completed an activation action (save, share, or prompt). Excludes test users."
        )

    with col3:
        st.metric(
            label="📊 Monthly Active Users (MAU)",
            value=f"{int(h['mau']):,}",
            delta=f"{mau['mom_growth_percent']:.1f}%" if mau is not None and pd.notna(mau.get('mom_growth_percent')) else None,
            help="Unique users who performed at least one action in the last 30 days. Excludes test users."
        )

    # Row 2
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="💬 Total Prompts",
            value=f"{int(h['total_prompts']):,}",
            delta=None,
            help="Cumulative number of AI queries submitted by all users"
        )

    with col2:
        st.metric(
            label="👆 Total Swipes",
            value=f"{int(h['total_swipes']):,}",
            delta=None,
            help="Cumulative number of card swipes (left or right) by all users"
        )

    with col3:
        total_saves_shares = int(h['total_saves']) + int(h['total_shares'])
        st.metric(
            label="💾 Total Saves & Shares",
            value=f"{total_saves_shares:,}",
            delta=None,
            help="Total number of cards saved and shared by all users"
        )

    # Row 3
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="🎨 Decks Created",
            value=f"{int(h['total_boards']):,}",
            delta=None,
            help="Total number of custom boards/wishlists created by users"
        )

    with col2:
        st.metric(
            label="🎮 Multiplayer Sessions",
            value=f"{int(h['total_multiplayer_sessions']):,}",
            delta=None,
            help="Total number of collaborative planning sessions created"
        )

    with col3:
        st.metric(
            label="🎁 Referrals Made",
            value=f"{int(referral_metrics['total_referrals_made']):,}" if referral_metrics is not None else "0",
            delta=None,
            help="Number of users who provided their referral codes to new users"
        )

    st.markdown('</div>', unsafe_allow_html=True)

except Exception as e:
    st.error(f"❌ Error loading data: {str(e)}")

    with st.expander("🔧 How to fix this issue"):
        st.markdown("""
        **Common Solutions:**

        1. **Check Database Connection**
           - Verify your database credentials in `.streamlit/secrets.toml`
           - Ensure your database server is running and accessible
           - Check network connectivity and firewall settings

        2. **Verify Configuration File**
           ```toml
           [database]
           host = "your-host"
           port = 5432
           database = "your-database"
           user = "your-username"
           password = "your-password"
           ```

        3. **Test Connection**
           - Try connecting to your database using a SQL client
           - Verify table names and schemas match the expected format

        4. **Check Data Models**
           - Ensure all required analytics tables exist
           - Verify data is being populated correctly
        """)

    st.exception(e)

# Footer
add_deck_footer()
