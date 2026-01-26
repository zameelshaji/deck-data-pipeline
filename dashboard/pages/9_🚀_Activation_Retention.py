"""Activation & Retention Dashboard"""

import streamlit as st
import pandas as pd
from datetime import datetime

from utils.styling import apply_deck_branding, add_deck_footer
from utils.data_loader import load_north_star_activation
from utils.visualizations import (
    create_kpi_trend_chart,
    create_line_chart,
    create_multi_line_chart,
    create_bar_chart
)

# Page config
st.set_page_config(
    page_title="Activation & Retention - DECK Analytics",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("🚀 Activation & Retention")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.divider()

# Activation definition
st.info("""
**Activation Definition:** A user is considered "activated" when they save at least 1 card within their first week.
**Strong Activation (SCR3):** A user who saves 3+ cards in their first week.
""")

try:
    # Load data
    activation_df = load_north_star_activation(weeks=12)

    if activation_df.empty:
        st.warning("No activation data available. Please run dbt models first.")
        st.stop()

    # Sort for charting
    activation_df = activation_df.sort_values('cohort_week')

    # Get latest cohort
    latest = activation_df.iloc[-1] if len(activation_df) > 0 else None
    previous = activation_df.iloc[-2] if len(activation_df) > 1 else None

    # ============================================
    # KPI CARDS
    # ============================================
    st.subheader("Latest Cohort Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        value = float(latest['activation_rate_7d']) if latest is not None else 0
        delta = round(value - previous['activation_rate_7d'], 1) if previous is not None else None
        st.metric(
            label="7-Day Activation Rate",
            value=f"{value:.1f}%",
            delta=f"{delta:+.1f}%" if delta else None,
            help="% of users who saved within 7 days of signup"
        )

    with col2:
        value = float(latest['strong_activation_rate']) if latest is not None else 0
        delta = round(value - previous['strong_activation_rate'], 1) if previous is not None else None
        st.metric(
            label="Strong Activation (SCR3)",
            value=f"{value:.1f}%",
            delta=f"{delta:+.1f}%" if delta else None,
            help="% of users who saved 3+ cards in first week"
        )

    with col3:
        value = float(latest['median_days_to_activation']) if latest is not None and latest['median_days_to_activation'] else 0
        delta = round(value - previous['median_days_to_activation'], 1) if previous is not None and previous['median_days_to_activation'] else None
        st.metric(
            label="Median Days to Activation",
            value=f"{value:.1f}",
            delta=f"{delta:+.1f}" if delta else None,
            delta_color="inverse",
            help="Median days from signup to first save (lower is better)"
        )

    with col4:
        value = float(latest['activated_retention_d7']) if latest is not None and latest['activated_retention_d7'] else 0
        delta = round(value - previous['activated_retention_d7'], 1) if previous is not None and previous['activated_retention_d7'] else None
        st.metric(
            label="D7 Retention (Activated)",
            value=f"{value:.1f}%",
            delta=f"{delta:+.1f}%" if delta else None,
            help="% of activated users who returned after week 1"
        )

    st.divider()

    # ============================================
    # ACTIVATION TRENDS
    # ============================================
    st.subheader("Activation Trends by Cohort")

    col1, col2 = st.columns(2)

    with col1:
        # Activation rates trend
        fig = create_kpi_trend_chart(
            activation_df,
            x_col='cohort_week',
            kpi_cols=['activation_rate_7d', 'activation_rate_30d'],
            title="Activation Rates (7-Day vs 30-Day)",
            y_label="Activation Rate %"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Strong activation trend
        fig = create_line_chart(
            activation_df,
            x='cohort_week',
            y='strong_activation_rate',
            title="Strong Activation Rate (SCR3 in Week 1)",
            y_label="Rate %"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ============================================
    # TIME TO ACTIVATION
    # ============================================
    st.subheader("Time to Activation")

    col1, col2 = st.columns(2)

    with col1:
        fig = create_multi_line_chart(
            activation_df,
            x='cohort_week',
            y_columns=['avg_days_to_activation_7d', 'median_days_to_activation'],
            title="Days to First Save",
            y_label="Days"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # First week engagement
        fig = create_multi_line_chart(
            activation_df,
            x='cohort_week',
            y_columns=['avg_sessions_week1', 'avg_saves_week1'],
            title="First Week Engagement",
            y_label="Count"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ============================================
    # RETENTION ANALYSIS
    # ============================================
    st.subheader("Retention Analysis")

    col1, col2 = st.columns(2)

    with col1:
        # Overall retention
        fig = create_kpi_trend_chart(
            activation_df,
            x_col='cohort_week',
            kpi_cols=['retention_d7', 'retention_d30'],
            title="Retention Rates (All Users)",
            y_label="Retention %"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Activated user retention
        fig = create_line_chart(
            activation_df,
            x='cohort_week',
            y='activated_retention_d7',
            title="D7 Retention (Activated Users Only)",
            y_label="Retention %"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ============================================
    # COHORT SIZE AND ACTIVATION VOLUME
    # ============================================
    st.subheader("Cohort Volume")

    col1, col2 = st.columns(2)

    with col1:
        fig = create_bar_chart(
            activation_df,
            x='cohort_week',
            y='cohort_size',
            title="Cohort Size by Week"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = create_multi_line_chart(
            activation_df,
            x='cohort_week',
            y_columns=['activated_7d', 'activated_with_scr3'],
            title="Activated Users by Week",
            y_label="Users"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ============================================
    # ACTIVATION vs RETENTION CORRELATION
    # ============================================
    st.subheader("Activation Quality Analysis")

    st.markdown("""
    **Key Question:** Do strongly activated users (SCR3) retain better than regular activated users?
    """)

    # Calculate correlation if we have enough data
    if len(activation_df) >= 4:
        corr_data = activation_df[['strong_activation_rate', 'activated_retention_d7']].dropna()
        if len(corr_data) >= 3:
            correlation = corr_data['strong_activation_rate'].corr(corr_data['activated_retention_d7'])
            st.metric(
                "Correlation: Strong Activation → D7 Retention",
                f"{correlation:.2f}",
                help="Positive correlation suggests SCR3 users retain better"
            )

    # Scatter plot would go here if we had more data points
    st.info("Track this correlation over time to validate that SCR3 is a meaningful activation threshold.")

    st.divider()

    # ============================================
    # DATA TABLE
    # ============================================
    with st.expander("Cohort Data Table", expanded=False):
        display_cols = [
            'cohort_week', 'cohort_size', 'activated_7d', 'activated_with_scr3',
            'activation_rate_7d', 'strong_activation_rate',
            'median_days_to_activation', 'retention_d7', 'activated_retention_d7'
        ]
        display_df = activation_df[display_cols].copy()
        display_df.columns = [
            'Cohort Week', 'Cohort Size', 'Activated (7d)', 'SCR3 Users',
            'Activation % (7d)', 'Strong Activation %',
            'Median Days to Activate', 'D7 Retention %', 'D7 Retention (Activated) %'
        ]
        st.dataframe(display_df.sort_values('Cohort Week', ascending=False), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading activation data: {str(e)}")
    with st.expander("🔧 Troubleshooting"):
        st.markdown("""
        **To fix this issue:**

        1. Run the dbt models:
           ```bash
           dbt run --select north_star_activation
           ```

        2. Verify the dependencies are built:
           ```bash
           dbt run --select stg_users north_star_session_metrics
           ```

        3. Note: Activation metrics require at least 7 days of data after user signup
           to calculate meaningful activation rates.
        """)

# Footer
add_deck_footer()
