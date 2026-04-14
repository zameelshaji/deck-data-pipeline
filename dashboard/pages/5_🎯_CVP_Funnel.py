"""DECK Analytics Dashboard - CVP Funnel Analysis Page"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.data_loader import (
    load_cvp_funnel_metrics,
    load_funnel_by_cohort,
    load_like_rate_by_position
)
from utils.visualizations import (
    create_funnel_chart,
    create_line_chart
)
from utils.styling import apply_deck_branding, add_deck_footer

# Page configuration
st.set_page_config(
    page_title="DECK Analytics - CVP Funnel",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("🎯 CVP Funnel Analysis")

st.markdown("""
**Core Value Proposition (CVP) Funnel** - Tracking how "Planners" move through our core user journey:
1. **Initiated** - User starts planning (prompts Dextr)
2. **Considered** - User swipes and saves recommendations to a deck
3. **Validated** - User shares deck or creates multiplayer session
4. **Decided** - Friends engage (join multiplayer or click shared link)
""")

# Refresh button and time filter
col_refresh, col_filter = st.columns([1, 4])
with col_refresh:
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

with col_filter:
    days_filter = st.selectbox(
        "Time Period",
        [7, 30, 90],
        index=1,
        help="Filter data by time period (based on user signup date)"
    )

st.divider()

try:
    # ============================================
    # SECTION 1: OVERALL FUNNEL VISUALIZATION
    # ============================================
    st.subheader("📊 Overall CVP Funnel")

    funnel_data = load_cvp_funnel_metrics(days=days_filter)

    if not funnel_data.empty and len(funnel_data) > 0:
        # Extract values
        total_users = int(funnel_data.iloc[0]['total_users'])
        initiated = int(funnel_data.iloc[0]['initiated'])
        considered = int(funnel_data.iloc[0]['considered'])
        validated = int(funnel_data.iloc[0]['validated'])
        decided = int(funnel_data.iloc[0]['decided'])

        # Calculate conversion rates
        init_rate = (initiated / total_users * 100) if total_users > 0 else 0
        consider_rate = (considered / initiated * 100) if initiated > 0 else 0
        validate_rate = (validated / considered * 100) if considered > 0 else 0
        decide_rate = (decided / validated * 100) if validated > 0 else 0

        # Display total users as reference metric
        st.metric(
            label="Total Users (Reference)",
            value=f"{total_users:,}",
            help=f"Total non-test users who signed up in the last {days_filter} days"
        )

        st.markdown("")

        # Display funnel chart (starting from Initiated)
        stages = ['Initiated', 'Considered', 'Validated', 'Decided']
        values = [initiated, considered, validated, decided]

        funnel_fig = create_funnel_chart(stages, values, "")
        st.plotly_chart(funnel_fig, use_container_width=True, config={'displayModeBar': False})

        # Display conversion rates in columns
        st.markdown("##### Conversion Rates by Stage")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                label="Initiated → Considered",
                value=f"{consider_rate:.1f}%",
                help=f"{considered:,} of {initiated:,} users who initiated saved cards"
            )

        with col2:
            st.metric(
                label="Considered → Validated",
                value=f"{validate_rate:.1f}%",
                help=f"{validated:,} of {considered:,} users who saved then shared"
            )

        with col3:
            st.metric(
                label="Validated → Decided",
                value=f"{decide_rate:.1f}%",
                help=f"{decided:,} of {validated:,} shares had friend engagement"
            )

        st.divider()

        # ============================================
        # SECTION 2: LIKE RATE BY CARD POSITION
        # ============================================
        st.subheader("📍 Like Rate by Card Position")

        position_data = load_like_rate_by_position(days=days_filter)

        if not position_data.empty and len(position_data) > 0:
            # Create line chart
            position_chart = create_line_chart(
                position_data,
                x='position',
                y='like_rate',
                title="",
                y_label="Like Rate (%)"
            )
            st.plotly_chart(position_chart, use_container_width=True, config={'displayModeBar': False})

            # Show detailed table
            with st.expander("📋 View Detailed Position Data"):
                st.dataframe(position_data, use_container_width=True, hide_index=True)
        else:
            st.warning("No like rate by position data available")

        st.divider()

        # ============================================
        # SECTION 3: COHORT COMPARISON (OPTIONAL)
        # ============================================
        with st.expander("📅 View Funnel by Signup Cohort (Weekly)"):
            st.markdown("""
            This shows how the funnel performs for different signup cohorts.
            Newer cohorts may behave differently than older ones.
            """)

            cohort_data = load_funnel_by_cohort(days=days_filter)

            if not cohort_data.empty and len(cohort_data) > 0:
                # Calculate conversion rates
                cohort_data['init_rate'] = (cohort_data['initiated'] / cohort_data['total_users'] * 100).round(1)
                cohort_data['consider_rate'] = (cohort_data['considered'] / cohort_data['initiated'] * 100).round(1)
                cohort_data['validate_rate'] = (cohort_data['validated'] / cohort_data['considered'] * 100).round(1)
                cohort_data['decide_rate'] = (cohort_data['decided'] / cohort_data['validated'] * 100).round(1)

                # Format dates
                cohort_data['cohort_week'] = pd.to_datetime(cohort_data['cohort_week']).dt.strftime('%Y-%m-%d')

                # Display table
                display_df = cohort_data[[
                    'cohort_week', 'total_users', 'initiated', 'init_rate',
                    'considered', 'consider_rate', 'validated', 'validate_rate',
                    'decided', 'decide_rate'
                ]].rename(columns={
                    'cohort_week': 'Cohort Week',
                    'total_users': 'Total Users',
                    'initiated': 'Initiated',
                    'init_rate': 'Init %',
                    'considered': 'Considered',
                    'consider_rate': 'Consider %',
                    'validated': 'Validated',
                    'validate_rate': 'Validate %',
                    'decided': 'Decided',
                    'decide_rate': 'Decide %'
                })

                st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.warning("No cohort data available")

    else:
        st.warning(f"No CVP funnel data available for the last {days_filter} days")

except Exception as e:
    st.error(f"Error loading CVP funnel data: {str(e)}")
    st.exception(e)

# Add footer
add_deck_footer()
