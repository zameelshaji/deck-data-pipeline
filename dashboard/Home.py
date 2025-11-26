"""DECK Analytics Dashboard - Executive Overview (Home Page)"""

import streamlit as st
import pandas as pd
from datetime import datetime
from utils.data_loader import (
    load_executive_summary,
    load_headline_metrics,
    load_latest_mau,
    load_latest_wau,
    load_total_multiplayer_sessions,
    load_total_decks_created,
    load_referral_metrics, 
    load_giveaway_metrics
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
    # Load data
    exec_summary = load_executive_summary()
    headline_metrics = load_headline_metrics(days=1)  # Get latest
    mau_data = load_latest_mau()
    wau_data = load_latest_wau()
    multiplayer_sessions_data = load_total_multiplayer_sessions()
    decks_created_data = load_total_decks_created()
    referral_data = load_referral_metrics()
    giveaway_data = load_giveaway_metrics()

    if exec_summary.empty or headline_metrics.empty:
        st.warning("⚠️ No data available")
        st.stop()

    # Extract values
    summary = exec_summary.iloc[0]
    latest = headline_metrics.iloc[0]
    mau = mau_data.iloc[0] if not mau_data.empty else None
    wau = wau_data.iloc[0] if not wau_data.empty else None
    multiplayer_total = multiplayer_sessions_data.iloc[0]['total_multiplayer_sessions'] if not multiplayer_sessions_data.empty else 0
    decks_total = decks_created_data.iloc[0]['total_decks_created'] if not decks_created_data.empty else 0
    referral_metrics = referral_data.iloc[0] if not referral_data.empty else None
    giveaway_metrics = giveaway_data.iloc[0] if not giveaway_data.empty else None


    # Calculate MAU % of Total Users
    mau_percentage = 0
    if mau is not None and latest['total_users'] > 0:
        mau_percentage = (mau['monthly_active_users'] / latest['total_users']) * 100

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
            value=f"{int(latest['total_users']):,}",
            delta=None,
            help="Total number of registered users on the platform"
        )

    with col2:
        st.metric(
            label="📊 Monthly Active Users (MAU)",
            value=f"{int(mau['monthly_active_users']):,}" if mau is not None else "N/A",
            delta=f"{mau['mom_growth_percent']:.1f}%" if mau is not None and pd.notna(mau['mom_growth_percent']) else None,
            help="Unique users who performed at least one action in the last 30 days"
        )

    with col3:
        st.metric(
            label="📈 MAU % of Total Users",
            value=f"{mau_percentage:.1f}%",
            delta=None,
            help="Percentage of total registered users who were active in the last 30 days"
        )

    # Row 2
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="💬 Total Prompts",
            value=f"{int(latest['total_prompts']):,}",
            delta=None,
            help="Cumulative number of AI queries submitted by all users"
        )

    with col2:
        st.metric(
            label="👆 Total Swipes",
            value=f"{int(latest['total_swipes']):,}",
            delta=None,
            help="Cumulative number of card swipes (left or right) by all users"
        )

    with col3:
        st.metric(
            label="🎮 Multiplayer Sessions",
            value=f"{int(multiplayer_total):,}",
            delta=None,
            help="Total number of collaborative planning sessions created"
        )

    # Row 3
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="🎨 Decks Created",
            value=f"{int(decks_total):,}",
            delta=None,
            help="Total number of custom boards/wishlists created by users"
        )

    with col2:
        st.metric(
            label="🎁 Referrals Made",
            value=f"{int(referral_metrics['total_referrals_made']):,}" if referral_metrics is not None else "0",
            delta=None,
            help="Number of users who provided their referral codes to new users"
        )

    with col3:
        st.metric(
            label="✅ Giveaways Claimed",
            value=f"{int(giveaway_metrics['giveaway_claims']):,}" if giveaway_metrics is not None else "0",
            delta=None,
            help="Number of giveaway claims made"
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
