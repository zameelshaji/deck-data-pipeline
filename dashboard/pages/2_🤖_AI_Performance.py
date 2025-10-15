"""DECK Analytics Dashboard - AI Performance Page"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.data_loader import load_dextr_performance
from utils.visualizations import create_line_chart, create_multi_line_chart, create_gauge_chart
from utils.styling import apply_deck_branding, add_deck_footer

# Page configuration
st.set_page_config(
    page_title="DECK Analytics - AI Performance",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("🤖 AI Performance (Dextr)")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

try:
    # Load AI performance data
    ai_data = load_dextr_performance(days=90)

    if ai_data.empty:
        st.warning("⚠️ No AI performance data available")
        st.stop()

    latest = ai_data.iloc[0]

    # ============================================
    # SECTION A: AI USAGE METRICS
    # ============================================
    st.subheader("📊 AI Usage Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Total Queries (Today)",
            value=f"{int(latest['total_queries']):,}" if pd.notna(latest['total_queries']) else "N/A",
            help="Total number of AI queries submitted today across all users"
        )

    with col2:
        st.metric(
            label="Unique AI Users (Today)",
            value=f"{int(latest['unique_users']):,}" if pd.notna(latest['unique_users']) else "N/A",
            help="Number of unique users who used AI features today"
        )

    with col3:
        st.metric(
            label="Queries per User",
            value=f"{latest['queries_per_user']:.1f}" if pd.notna(latest['queries_per_user']) else "N/A",
            help="Average number of AI queries per active user today"
        )

    with col4:
        st.metric(
            label="Unique Packs Generated",
            value=f"{int(latest['unique_packs_generated']):,}" if pd.notna(latest['unique_packs_generated']) else "N/A",
            help="Number of unique AI-generated experience packs created today"
        )

    st.divider()

    # ============================================
    # SECTION B: AI QUALITY METRICS
    # ============================================
    st.subheader("⭐ AI Quality Metrics")

    col1, col2 = st.columns(2)

    with col1:
        # AI Satisfaction Gauge
        satisfaction_rate = latest['avg_like_rate'] if pd.notna(latest['avg_like_rate']) else 0
        satisfaction_gauge = create_gauge_chart(
            value=satisfaction_rate,
            title="AI Satisfaction Rate (%)",
            max_value=100
        )
        st.plotly_chart(satisfaction_gauge, use_container_width=True)

    with col2:
        # Pack Success & Engagement Rates
        st.markdown("#### Success Metrics")

        col2a, col2b = st.columns(2)
        with col2a:
            st.metric(
                label="Pack Success Rate",
                value=f"{latest['pack_success_rate']:.1f}%" if pd.notna(latest['pack_success_rate']) else "N/A",
                help="Percentage of packs with 50%+ completion rate"
            )
        with col2b:
            st.metric(
                label="Engagement Success",
                value=f"{latest['engagement_success_rate']:.1f}%" if pd.notna(latest['engagement_success_rate']) else "N/A",
                help="Percentage of sessions with 3+ cards acted upon"
            )

        col2c, col2d = st.columns(2)
        with col2c:
            st.metric(
                label="Satisfaction Rate",
                value=f"{latest['satisfaction_rate']:.1f}%" if pd.notna(latest['satisfaction_rate']) else "N/A",
                help="Percentage of AI-generated packs that received positive user feedback (likes, high engagement)"
            )
        with col2d:
            st.metric(
                label="Avg Completion Rate",
                value=f"{latest['avg_completion_rate']:.1f}%" if pd.notna(latest['avg_completion_rate']) else "N/A"
            )

    st.divider()

    # ============================================
    # SECTION C: PERFORMANCE TRENDS
    # ============================================
    st.subheader("📈 Performance Trends (Last 90 Days)")

    # Queries and Satisfaction Trend
    trend_data = ai_data.sort_values('query_date')

    col1, col2 = st.columns(2)

    with col1:
        queries_chart = create_line_chart(
            trend_data,
            x='query_date',
            y='total_queries',
            title="Daily AI Queries",
            y_label="Queries"
        )
        st.plotly_chart(queries_chart, use_container_width=True)

    with col2:
        satisfaction_chart = create_line_chart(
            trend_data,
            x='query_date',
            y='avg_like_rate',
            title="AI Satisfaction Rate (%)",
            y_label="Like Rate %"
        )
        st.plotly_chart(satisfaction_chart, use_container_width=True)

    # 7-day rolling averages
    st.markdown("#### 7-Day Rolling Averages")
    rolling_chart = create_multi_line_chart(
        trend_data,
        x='query_date',
        y_columns=['queries_7day_avg', 'satisfaction_7day_avg'],
        title="",
        y_label="Value"
    )
    st.plotly_chart(rolling_chart, use_container_width=True)

    st.divider()

    # ============================================
    # SECTION D: AI PERFORMANCE DEEP DIVE
    # ============================================
    st.subheader("🔍 AI Performance Deep Dive")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("#### ⚡ Processing Performance")
        st.metric(
            label="Avg Response Time",
            value=f"{latest['avg_processing_time']:.1f}s" if pd.notna(latest['avg_processing_time']) else "N/A"
        )
        st.metric(
            label="P95 Response Time",
            value=f"{latest['p95_processing_time']:.1f}s" if pd.notna(latest['p95_processing_time']) else "N/A"
        )
        performance_category = latest['performance_category'] if pd.notna(latest['performance_category']) else "N/A"

        # Color code performance category
        if performance_category == "Fast":
            st.success(f"Performance: {performance_category}")
        elif performance_category == "Acceptable":
            st.warning(f"Performance: {performance_category}")
        else:
            st.error(f"Performance: {performance_category}")

    with col2:
        st.markdown("#### 📦 Pack Quality")
        st.metric(
            label="Avg Cards per Pack",
            value=f"{latest['avg_cards_per_pack']:.1f}" if pd.notna(latest['avg_cards_per_pack']) else "N/A"
        )
        st.metric(
            label="Avg Cards Shown",
            value=f"{latest['avg_cards_shown']:.1f}" if pd.notna(latest['avg_cards_shown']) else "N/A"
        )
        st.metric(
            label="Avg Engagement Rate",
            value=f"{latest['avg_engagement_rate']:.1f}%" if pd.notna(latest['avg_engagement_rate']) else "N/A"
        )

    with col3:
        st.markdown("#### 👥 User Behavior")
        st.metric(
            label="One-Time Users",
            value=f"{int(latest['one_time_users']):,}" if pd.notna(latest['one_time_users']) else "N/A"
        )
        st.metric(
            label="Casual Users (2-5)",
            value=f"{int(latest['casual_users']):,}" if pd.notna(latest['casual_users']) else "N/A"
        )
        st.metric(
            label="Power Users (6+)",
            value=f"{int(latest['power_users']):,}" if pd.notna(latest['power_users']) else "N/A"
        )
        st.metric(
            label="Power User %",
            value=f"{latest['power_user_percentage']:.1f}%" if pd.notna(latest['power_user_percentage']) else "N/A"
        )

    st.divider()

    # ============================================
    # SECTION E: SUCCESS METRICS
    # ============================================
    st.subheader("✅ Success Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Successful Packs",
            value=f"{int(latest['successful_packs']):,}" if pd.notna(latest['successful_packs']) else "N/A",
            help="Packs with 50%+ completion rate"
        )

    with col2:
        st.metric(
            label="Well-Liked Packs",
            value=f"{int(latest['well_liked_packs']):,}" if pd.notna(latest['well_liked_packs']) else "N/A",
            help="Packs with 30%+ like rate"
        )

    with col3:
        st.metric(
            label="Engaged Sessions",
            value=f"{int(latest['engaged_sessions']):,}" if pd.notna(latest['engaged_sessions']) else "N/A",
            help="Sessions with 3+ cards acted upon"
        )

    with col4:
        st.metric(
            label="Multi-Like Sessions",
            value=f"{int(latest['sessions_with_multiple_likes']):,}" if pd.notna(latest['sessions_with_multiple_likes']) else "N/A",
            help="Sessions with 2+ cards liked"
        )

except Exception as e:
    st.error(f"❌ Error loading AI performance data: {str(e)}")

    with st.expander("🔧 Troubleshooting Guide"):
        st.markdown("""
        **Common Issues & Solutions:**

        1. **Database Connection**
           - Verify database credentials in `.streamlit/secrets.toml`
           - Check if AI analytics tables exist (e.g., `dextr_performance`)
           - Ensure database server is accessible

        2. **Missing AI Data**
           - Verify AI queries are being logged to the database
           - Check if Dextr (AI service) is running and tracking events
           - Ensure analytics pipeline is processing AI interactions

        3. **Chart Display Issues**
           - If you see "got multiple values for keyword argument 'title'", this is a known bug
           - Refresh the page to retry loading charts
        """)

    st.exception(e)

# Footer
add_deck_footer()
