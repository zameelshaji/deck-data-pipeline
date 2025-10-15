"""DECK Analytics Dashboard - User Analytics Page"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.data_loader import (
    load_daily_active_users,
    load_weekly_active_users,
    load_monthly_active_users,
    load_user_acquisition_funnel
)
from utils.visualizations import (
    create_line_chart,
    create_multi_line_chart,
    create_funnel_chart
)
from utils.styling import apply_deck_branding, add_deck_footer

# Page configuration
st.set_page_config(
    page_title="DECK Analytics - User Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("📈 User Analytics")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

try:
    # ============================================
    # SECTION A: ACTIVE USERS OVERVIEW
    # ============================================
    st.subheader("👥 Active Users Overview")

    # Tabs for DAU/WAU/MAU
    tab_daily, tab_weekly, tab_monthly = st.tabs(["📅 Daily", "📆 Weekly", "📊 Monthly"])

    # TAB: Daily Active Users
    with tab_daily:
        dau_data = load_daily_active_users(days=90)

        if not dau_data.empty:
            # Metrics row
            latest_dau = dau_data.iloc[0]
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    label="Current DAU",
                    value=f"{int(latest_dau['daily_active_users']):,}",
                    delta=f"{latest_dau['dau_wow_growth_percent']:.1f}%" if pd.notna(latest_dau['dau_wow_growth_percent']) else None
                )

            with col2:
                st.metric(
                    label="7-Day Avg",
                    value=f"{int(latest_dau['dau_7day_avg']):,}" if pd.notna(latest_dau['dau_7day_avg']) else "N/A"
                )

            with col3:
                st.metric(
                    label="AI Adoption",
                    value=f"{latest_dau['ai_adoption_rate']:.1f}%" if pd.notna(latest_dau['ai_adoption_rate']) else "N/A"
                )

            with col4:
                st.metric(
                    label="Avg Events/User",
                    value=f"{latest_dau['avg_events_per_user']:.1f}" if pd.notna(latest_dau['avg_events_per_user']) else "N/A"
                )

            # DAU Trend Chart
            st.markdown("#### Daily Active Users Trend (Last 90 Days)")
            dau_chart = create_line_chart(
                dau_data.sort_values('activity_date'),
                x='activity_date',
                y='daily_active_users',
                title="Daily Active Users",
                y_label="Active Users"
            )
            st.plotly_chart(dau_chart, use_container_width=True, config={'displayModeBar': False})

            # Feature Breakdown Chart
            st.markdown("#### Feature Adoption Breakdown")
            feature_chart = create_multi_line_chart(
                dau_data.sort_values('activity_date'),
                x='activity_date',
                y_columns=['ai_active_users', 'curation_active_users', 'conversion_active_users', 'multiplayer_active_users'],
                title="",
                y_label="Active Users"
            )
            st.plotly_chart(feature_chart, use_container_width=True)

        else:
            st.warning("No daily active users data available")

    # TAB: Weekly Active Users
    with tab_weekly:
        wau_data = load_weekly_active_users(weeks=12)

        if not wau_data.empty:
            # Metrics row
            latest_wau = wau_data.iloc[0]
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    label="Current WAU",
                    value=f"{int(latest_wau['weekly_active_users']):,}",
                    delta=f"{latest_wau['wow_growth_percent']:.1f}%" if pd.notna(latest_wau['wow_growth_percent']) else None
                )

            with col2:
                st.metric(
                    label="4-Week Avg",
                    value=f"{int(latest_wau['rolling_4week_avg']):,}" if pd.notna(latest_wau['rolling_4week_avg']) else "N/A"
                )

            with col3:
                st.metric(
                    label="AI Adoption",
                    value=f"{latest_wau['ai_adoption_rate']:.1f}%" if pd.notna(latest_wau['ai_adoption_rate']) else "N/A"
                )

            with col4:
                st.metric(
                    label="Multiplayer Adoption",
                    value=f"{latest_wau['multiplayer_adoption_rate']:.1f}%" if pd.notna(latest_wau['multiplayer_adoption_rate']) else "N/A"
                )

            # WAU Trend Chart
            st.markdown("#### Weekly Active Users Trend (Last 12 Weeks)")
            wau_chart = create_line_chart(
                wau_data.sort_values('activity_week'),
                x='activity_week',
                y='weekly_active_users',
                title=None,
                y_label="Active Users"
            )
            st.plotly_chart(wau_chart, use_container_width=True)

            # Feature Adoption Rates
            st.markdown("#### Feature Adoption Rates")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("AI", f"{latest_wau['ai_adoption_rate']:.1f}%")
            with col2:
                st.metric("Conversion", f"{latest_wau['conversion_user_rate']:.1f}%")
            with col3:
                st.metric("Multiplayer", f"{latest_wau['multiplayer_adoption_rate']:.1f}%")
            with col4:
                st.metric("Featured", f"{latest_wau['featured_adoption_rate']:.1f}%")

        else:
            st.warning("No weekly active users data available")

    # TAB: Monthly Active Users
    with tab_monthly:
        mau_data = load_monthly_active_users(months=12)

        if not mau_data.empty:
            # Metrics row
            latest_mau = mau_data.iloc[0]
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    label="Current MAU",
                    value=f"{int(latest_mau['monthly_active_users']):,}",
                    delta=f"{latest_mau['mom_growth_percent']:.1f}%" if pd.notna(latest_mau['mom_growth_percent']) else None
                )

            with col2:
                st.metric(
                    label="3-Month Avg",
                    value=f"{int(latest_mau['rolling_3month_avg']):,}" if pd.notna(latest_mau['rolling_3month_avg']) else "N/A"
                )

            with col3:
                st.metric(
                    label="AI Adoption",
                    value=f"{latest_mau['ai_adoption_rate']:.1f}%" if pd.notna(latest_mau['ai_adoption_rate']) else "N/A"
                )

            with col4:
                st.metric(
                    label="Avg Events/User",
                    value=f"{latest_mau['avg_events_per_user']:.1f}" if pd.notna(latest_mau['avg_events_per_user']) else "N/A"
                )

            # MAU Trend Chart
            st.markdown("#### Monthly Active Users Trend (Last 12 Months)")
            mau_chart = create_line_chart(
                mau_data.sort_values('activity_month'),
                x='activity_month',
                y='monthly_active_users',
                title=None,
                y_label="Active Users"
            )
            st.plotly_chart(mau_chart, use_container_width=True)

        else:
            st.warning("No monthly active users data available")

    st.divider()

    # ============================================
    # SECTION B: USER ACQUISITION FUNNEL
    # ============================================
    st.subheader("🎯 User Acquisition Funnel (Last 30 Days)")

    funnel_data = load_user_acquisition_funnel(days=30)

    if not funnel_data.empty:
        # Calculate totals
        total_signups = funnel_data['signups'].sum()
        total_onboarded = funnel_data['completed_onboarding'].sum()
        total_ai_users = funnel_data['used_ai'].sum()
        total_activated = funnel_data['active_first_week'].sum()

        # Funnel visualization
        funnel_stages = [
            'Signups',
            'Completed Onboarding',
            'Used AI',
            'Week 1 Activated'
        ]
        funnel_values = [
            total_signups,
            total_onboarded,
            total_ai_users,
            total_activated
        ]

        funnel_chart = create_funnel_chart(
            stages=funnel_stages,
            values=funnel_values,
            title=""
        )
        st.plotly_chart(funnel_chart, use_container_width=True)

        # Conversion rate metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="Total Signups",
                value=f"{total_signups:,}"
            )

        with col2:
            onboarding_rate = (total_onboarded / total_signups * 100) if total_signups > 0 else 0
            st.metric(
                label="Onboarding Rate",
                value=f"{onboarding_rate:.1f}%"
            )

        with col3:
            ai_adoption = (total_ai_users / total_signups * 100) if total_signups > 0 else 0
            st.metric(
                label="AI Adoption",
                value=f"{ai_adoption:.1f}%"
            )

        with col4:
            activation_rate = (total_activated / total_signups * 100) if total_signups > 0 else 0
            st.metric(
                label="Week 1 Activation",
                value=f"{activation_rate:.1f}%"
            )

        # Show average metrics
        st.markdown("#### Average Funnel Performance")
        col1, col2, col3 = st.columns(3)

        avg_onboarding = funnel_data['onboarding_completion_rate'].mean()
        avg_ai_adoption = funnel_data['ai_adoption_rate'].mean()
        avg_activation = funnel_data['week_1_activation_rate'].mean()

        with col1:
            st.metric(
                label="Avg Onboarding Completion",
                value=f"{avg_onboarding:.1f}%" if pd.notna(avg_onboarding) else "N/A"
            )

        with col2:
            st.metric(
                label="Avg AI Adoption",
                value=f"{avg_ai_adoption:.1f}%" if pd.notna(avg_ai_adoption) else "N/A"
            )

        with col3:
            st.metric(
                label="Avg Week 1 Activation",
                value=f"{avg_activation:.1f}%" if pd.notna(avg_activation) else "N/A"
            )

    else:
        st.warning("No user acquisition funnel data available")

    st.divider()

    # ============================================
    # SECTION C: ENGAGEMENT BREAKDOWN
    # ============================================
    st.subheader("💡 Engagement Breakdown")

    if not dau_data.empty:
        latest = dau_data.iloc[0]

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("#### Engagement Intensity")
            st.metric("Avg Events/User", f"{latest['avg_events_per_user']:.1f}" if pd.notna(latest['avg_events_per_user']) else "N/A")
            st.metric("Median Events/User", f"{int(latest['median_events_per_user'])}" if pd.notna(latest['median_events_per_user']) else "N/A")
            st.metric("Max Events/User", f"{int(latest['max_events_per_user'])}" if pd.notna(latest['max_events_per_user']) else "N/A")

        with col2:
            st.markdown("#### Feature Adoption %")
            st.metric("AI Users", f"{latest['ai_adoption_rate']:.1f}%" if pd.notna(latest['ai_adoption_rate']) else "N/A")
            st.metric("Conversion Users", f"{latest['conversion_user_rate']:.1f}%" if pd.notna(latest['conversion_user_rate']) else "N/A")

        with col3:
            st.markdown("#### Active User Counts")
            st.metric("AI Active", f"{int(latest['ai_active_users']):,}" if pd.notna(latest['ai_active_users']) else "N/A")
            st.metric("Multiplayer Active", f"{int(latest['multiplayer_active_users']):,}" if pd.notna(latest['multiplayer_active_users']) else "N/A")

except Exception as e:
    st.error(f"❌ Error loading user analytics data: {str(e)}")

    with st.expander("🔧 How to fix this"):
        st.markdown("""
        **Troubleshooting Steps:**

        1. **Database Connection**
           - Check `.streamlit/secrets.toml` for correct database credentials
           - Verify database server is running and accessible
           - Test connection using a database client

        2. **Analytics Tables**
           - Ensure these tables exist: `daily_active_users`, `weekly_active_users`, `monthly_active_users`, `user_acquisition_funnel`
           - Verify analytics models have been run and populated data
           - Check that date ranges have sufficient data

        3. **Data Issues**
           - Verify user events are being tracked and logged
           - Check if analytics aggregation jobs are running
           - Ensure date columns are properly formatted
        """)

    st.exception(e)

# Footer
add_deck_footer()
