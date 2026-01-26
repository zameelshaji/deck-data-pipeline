"""Session Funnel Analysis Dashboard"""

import streamlit as st
import pandas as pd
from datetime import datetime

from utils.styling import apply_deck_branding, add_deck_footer
from utils.data_loader import (
    load_north_star_funnel,
    load_north_star_funnel_latest,
    load_north_star_by_surface
)
from utils.visualizations import (
    create_north_star_funnel_chart,
    create_kpi_trend_chart,
    create_line_chart,
    create_multi_line_chart
)

# Page config
st.set_page_config(
    page_title="Session Funnel - DECK Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("📊 Session Funnel Analysis")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.divider()

try:
    # Load data
    funnel_df = load_north_star_funnel(weeks=12)
    latest_funnel = load_north_star_funnel_latest()

    if funnel_df.empty:
        st.warning("No funnel data available. Please run dbt models first.")
        st.stop()

    # Sort for charting
    funnel_df = funnel_df.sort_values('session_week')

    # ============================================
    # FUNNEL VISUALIZATION
    # ============================================
    st.subheader("Session Funnel (Latest Week)")

    if not latest_funnel.empty:
        latest = latest_funnel.iloc[0]
        week_label = latest['session_week']

        # Funnel stages and values
        stages = ['Initiated', 'Browsed', 'Engaged', 'Saved', 'Shortlisted', 'Social', 'Converted']
        values = [
            int(latest['initiated']),
            int(latest['browsed']),
            int(latest['engaged']),
            int(latest['saved']),
            int(latest['shortlisted']),
            int(latest['social']),
            int(latest['converted'])
        ]

        col1, col2 = st.columns([2, 1])

        with col1:
            fig = create_north_star_funnel_chart(
                stages,
                values,
                title=f"Planning Session Funnel (Week of {week_label})"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Funnel Stage Definitions**")
            st.markdown("""
            1. **Initiated** - Started a planning session
            2. **Browsed** - Engaged beyond initial event
            3. **Engaged** - Swiped or viewed cards
            4. **Saved** - Saved at least 1 card (SSR)
            5. **Shortlisted** - Saved 3+ cards (SCR3)
            6. **Social** - Shared or used multiplayer
            7. **Converted** - Booking/directions action
            """)

            st.divider()

            st.markdown("**Conversion Rates**")
            st.metric("Initiated → Saved", f"{latest['pct_saved']:.1f}%")
            st.metric("Saved → Shortlisted", f"{latest['cvr_saved_to_shortlisted']:.1f}%")
            st.metric("Shortlisted → Social", f"{latest['cvr_shortlisted_to_social']:.1f}%")

    st.divider()

    # ============================================
    # DROP-OFF ANALYSIS
    # ============================================
    st.subheader("Drop-off Analysis")

    col1, col2 = st.columns(2)

    with col1:
        # Drop-off rates trend
        fig = create_kpi_trend_chart(
            funnel_df,
            x_col='session_week',
            kpi_cols=['drop_initiated', 'drop_browsed', 'drop_engaged', 'drop_saved'],
            title="Drop-off Rates by Stage",
            y_label="Drop-off %"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Conversion rates from start
        fig = create_kpi_trend_chart(
            funnel_df,
            x_col='session_week',
            kpi_cols=['pct_saved', 'pct_shortlisted', 'pct_social', 'pct_converted'],
            title="Cumulative Conversion Rates (from Initiated)",
            y_label="Conversion %"
        )
        st.plotly_chart(fig, use_container_width=True)

    # Key drop-off metrics
    if not latest_funnel.empty:
        latest = latest_funnel.iloc[0]

        st.markdown("**Key Drop-off Points (Latest Week)**")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Initiated → Browsed Drop",
                f"{latest['drop_initiated']:.1f}%",
                help="Users who started but didn't browse"
            )

        with col2:
            st.metric(
                "Browsed → Engaged Drop",
                f"{latest['drop_browsed']:.1f}%",
                help="Users who browsed but didn't engage"
            )

        with col3:
            st.metric(
                "Engaged → Saved Drop",
                f"{latest['drop_engaged']:.1f}%",
                help="Users who engaged but didn't save"
            )

        with col4:
            st.metric(
                "Saved → Shortlisted Drop",
                f"{latest['drop_saved']:.1f}%",
                help="Users who saved but didn't reach 3+ saves"
            )

    st.divider()

    # ============================================
    # STAGE VOLUME TRENDS
    # ============================================
    st.subheader("Stage Volume Trends")

    col1, col2 = st.columns(2)

    with col1:
        # Early funnel stages
        fig = create_multi_line_chart(
            funnel_df,
            x='session_week',
            y_columns=['initiated', 'browsed', 'engaged'],
            title="Early Funnel Stages",
            y_label="Sessions"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Later funnel stages
        fig = create_multi_line_chart(
            funnel_df,
            x='session_week',
            y_columns=['saved', 'shortlisted', 'social', 'converted'],
            title="Later Funnel Stages",
            y_label="Sessions"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ============================================
    # WEEK-OVER-WEEK COMPARISON
    # ============================================
    st.subheader("Week-over-Week Changes")

    if len(funnel_df) >= 2:
        latest_week = funnel_df.iloc[-1]
        prev_week = funnel_df.iloc[-2]

        st.markdown(f"**Comparing {latest_week['session_week']} vs {prev_week['session_week']}**")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            change = int(latest_week['initiated'] - prev_week['initiated'])
            pct_change = 100 * change / prev_week['initiated'] if prev_week['initiated'] > 0 else 0
            st.metric(
                "Initiated",
                f"{int(latest_week['initiated']):,}",
                f"{change:+,} ({pct_change:+.1f}%)"
            )

        with col2:
            change = int(latest_week['saved'] - prev_week['saved'])
            pct_change = 100 * change / prev_week['saved'] if prev_week['saved'] > 0 else 0
            st.metric(
                "Saved",
                f"{int(latest_week['saved']):,}",
                f"{change:+,} ({pct_change:+.1f}%)"
            )

        with col3:
            change = int(latest_week['shortlisted'] - prev_week['shortlisted'])
            pct_change = 100 * change / prev_week['shortlisted'] if prev_week['shortlisted'] > 0 else 0
            st.metric(
                "Shortlisted",
                f"{int(latest_week['shortlisted']):,}",
                f"{change:+,} ({pct_change:+.1f}%)"
            )

        with col4:
            change = int(latest_week['converted'] - prev_week['converted'])
            pct_change = 100 * change / prev_week['converted'] if prev_week['converted'] > 0 else 0
            st.metric(
                "Converted",
                f"{int(latest_week['converted']):,}",
                f"{change:+,} ({pct_change:+.1f}%)"
            )

    st.divider()

    # ============================================
    # DATA TABLE
    # ============================================
    with st.expander("Weekly Funnel Data", expanded=False):
        display_cols = [
            'session_week', 'initiated', 'browsed', 'engaged',
            'saved', 'shortlisted', 'social', 'converted',
            'pct_saved', 'pct_shortlisted', 'pct_converted'
        ]
        display_df = funnel_df[display_cols].copy()
        display_df.columns = [
            'Week', 'Initiated', 'Browsed', 'Engaged',
            'Saved', 'Shortlisted', 'Social', 'Converted',
            '% Saved', '% Shortlisted', '% Converted'
        ]
        st.dataframe(display_df.sort_values('Week', ascending=False), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading funnel data: {str(e)}")
    with st.expander("🔧 Troubleshooting"):
        st.markdown("""
        **To fix this issue:**

        1. Run the dbt models:
           ```bash
           dbt run --select north_star_session_funnel
           ```

        2. Verify the dependencies are built:
           ```bash
           dbt run --select north_star_session_metrics
           ```
        """)

# Footer
add_deck_footer()
