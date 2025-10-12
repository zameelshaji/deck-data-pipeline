"""DECK Analytics Dashboard - Executive Overview (Home Page)"""

import streamlit as st
import pandas as pd
from datetime import datetime
from utils.data_loader import load_executive_summary, load_headline_metrics

# Page configuration
st.set_page_config(
    page_title="DECK Analytics - Executive Overview",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("📊 DECK Analytics - Executive Overview")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

try:
    # Load data
    exec_summary = load_executive_summary()
    headline_metrics = load_headline_metrics(days=1)  # Get latest

    if exec_summary.empty or headline_metrics.empty:
        st.warning("⚠️ No data available")
        st.stop()

    # Extract values
    summary = exec_summary.iloc[0]
    latest = headline_metrics.iloc[0]

    # Display last updated
    st.caption(f"📅 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    st.divider()

    # ============================================
    # SECTION A: HERO METRICS
    # ============================================
    st.subheader("📈 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="👥 Total Users",
            value=f"{int(latest['total_users']):,}",
            delta=None
        )

    with col2:
        st.metric(
            label="📊 Daily Active Users (30d avg)",
            value=f"{int(summary['daily_active_users']):,}",
            delta=f"{summary['dau_growth_percent']:.1f}%" if pd.notna(summary['dau_growth_percent']) else None
        )

    with col3:
        st.metric(
            label="💬 Total Prompts",
            value=f"{int(latest['total_prompts']):,}",
            delta=None
        )

    with col4:
        st.metric(
            label="👆 Total Swipes",
            value=f"{int(latest['total_swipes']):,}",
            delta=None
        )

    st.divider()

    # ============================================
    # SECTION B: KEY METRICS GRID
    # ============================================
    st.subheader("🎯 Performance Metrics (Last 30 Days)")

    col1, col2, col3 = st.columns(3)

    # Column 1: Growth Metrics
    with col1:
        st.markdown("### 📈 Growth")
        st.metric(
            label="New Signups",
            value=f"{int(summary['new_signups_30d']):,}",
            delta=None
        )
        st.metric(
            label="DAU Growth",
            value=f"{summary['dau_growth_percent']:.1f}%" if pd.notna(summary['dau_growth_percent']) else "N/A",
            delta=None
        )
        st.metric(
            label="Activation Rate",
            value=f"{summary['activation_rate']:.1f}%" if pd.notna(summary['activation_rate']) else "N/A",
            delta=None
        )
        st.metric(
            label="Avg Events/User",
            value=f"{summary['avg_events_per_user']:.1f}" if pd.notna(summary['avg_events_per_user']) else "N/A",
            delta=None
        )

    # Column 2: AI Performance
    with col2:
        st.markdown("### 🤖 AI Performance")
        st.metric(
            label="Avg Daily AI Queries",
            value=f"{int(summary['avg_daily_ai_queries']):,}" if pd.notna(summary['avg_daily_ai_queries']) else "N/A",
            delta=None
        )
        st.metric(
            label="AI Satisfaction Rate",
            value=f"{summary['ai_satisfaction_rate']:.1f}%" if pd.notna(summary['ai_satisfaction_rate']) else "N/A",
            delta=None
        )
        st.metric(
            label="AI Adoption Rate",
            value=f"{summary['ai_adoption_rate_percent']:.1f}%" if pd.notna(summary['ai_adoption_rate_percent']) else "N/A",
            delta=None
        )
        st.metric(
            label="AI Power Users",
            value=f"{summary['ai_power_user_percentage']:.1f}%" if pd.notna(summary['ai_power_user_percentage']) else "N/A",
            delta=None
        )

    # Column 3: Content & Features
    with col3:
        st.markdown("### 🎨 Content & Features")
        st.metric(
            label="Active Experience Cards",
            value=f"{int(summary['active_cards_count']):,}" if pd.notna(summary['active_cards_count']) else "N/A",
            delta=None
        )
        st.metric(
            label="Content Like Rate",
            value=f"{summary['content_like_rate']:.1f}%" if pd.notna(summary['content_like_rate']) else "N/A",
            delta=None
        )
        st.metric(
            label="High Performing Cards",
            value=f"{int(summary['high_performing_cards']):,}" if pd.notna(summary['high_performing_cards']) else "N/A",
            delta=None
        )
        st.metric(
            label="Multiplayer Sessions",
            value=f"{int(summary['total_multiplayer_sessions_last_30']):,}" if pd.notna(summary['total_multiplayer_sessions_last_30']) else "N/A",
            delta=None
        )

    st.divider()

    # ============================================
    # SECTION C: ADDITIONAL INSIGHTS
    # ============================================
    st.subheader("💡 Additional Insights")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🎯 Engagement Quality")
        st.metric(
            label="Conversion User Rate",
            value=f"{summary['conversion_user_rate_percent']:.1f}%" if pd.notna(summary['conversion_user_rate_percent']) else "N/A",
            delta=None,
            help="Percentage of users performing conversion actions"
        )
        st.metric(
            label="Content Conversion Rate",
            value=f"{summary['content_conversion_rate']:.1f}%" if pd.notna(summary['content_conversion_rate']) else "N/A",
            delta=None,
            help="Percentage of impressions leading to conversions"
        )

    with col2:
        st.markdown("### 🤝 Multiplayer")
        st.metric(
            label="Avg Participants/Session",
            value=f"{summary['avg_multiplayer_participants']:.1f}" if pd.notna(summary['avg_multiplayer_participants']) else "N/A",
            delta=None
        )
        st.metric(
            label="Consensus Rate",
            value=f"{summary['multiplayer_consensus_rate']:.1f}%" if pd.notna(summary['multiplayer_consensus_rate']) else "N/A",
            delta=None,
            help="Average agreement rate in multiplayer sessions"
        )

    st.divider()

    # ============================================
    # SECTION D: GROWTH COMPARISON
    # ============================================
    st.subheader("📊 Growth vs Previous Period")

    col1, col2 = st.columns(2)

    with col1:
        dau_growth = summary['dau_growth_vs_previous_30d']
        if pd.notna(dau_growth):
            st.metric(
                label="DAU Growth (vs Previous 30d)",
                value=f"{dau_growth:+.1f}%",
                delta=f"{dau_growth:+.1f}%"
            )
        else:
            st.metric(label="DAU Growth (vs Previous 30d)", value="N/A")

    with col2:
        ai_growth = summary['ai_adoption_growth_vs_previous_30d']
        if pd.notna(ai_growth):
            st.metric(
                label="AI Adoption Growth (vs Previous 30d)",
                value=f"{ai_growth:+.1f}%",
                delta=f"{ai_growth:+.1f}%"
            )
        else:
            st.metric(label="AI Adoption Growth (vs Previous 30d)", value="N/A")

except Exception as e:
    st.error(f"❌ Error loading data: {str(e)}")
    st.info("💡 Make sure your database connection is configured correctly in .streamlit/secrets.toml")
    st.exception(e)

# Footer
st.divider()
st.caption("📊 DECK Analytics Dashboard | Use the sidebar to navigate to other sections")
