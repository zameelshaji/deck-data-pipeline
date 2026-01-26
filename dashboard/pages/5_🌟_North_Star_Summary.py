"""North Star Metrics Summary Dashboard"""

import streamlit as st
import pandas as pd
from datetime import datetime

from utils.styling import apply_deck_branding, add_deck_footer
from utils.data_loader import (
    load_north_star_weekly,
    load_north_star_latest,
    load_north_star_by_surface
)
from utils.visualizations import (
    create_kpi_trend_chart,
    create_line_chart,
    create_surface_comparison_chart
)

# Page config
st.set_page_config(
    page_title="North Star Metrics - DECK Analytics",
    page_icon="🌟",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("🌟 North Star Metrics")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.divider()

# Data gaps callout
with st.expander("⚠️ Known Instrumentation Gaps", expanded=False):
    st.markdown("""
    **Current data limitations:**
    - **Deck-level shares not tracked** - `deck_shared` event is not instrumented
    - **Post-share interactions not tracked** - Cannot measure recipient engagement
    - **No share_link_id** - Cannot attribute conversions to specific shares
    - **Share metrics significantly undercounted** - Only card-level shares captured

    **Multiplayer metrics** are shown separately as a social engagement signal, not as a share proxy.

    The **save metrics (SSR, SCR3, TTFS)** are reliable and should be the primary focus.
    """)

try:
    # Load data
    weekly_df = load_north_star_weekly(weeks=12)
    latest_df = load_north_star_latest()
    surface_df = load_north_star_by_surface(weeks=8)

    if weekly_df.empty:
        st.warning("No North Star data available. Please run dbt models first.")
        st.stop()

    # Get current and previous week metrics
    current = latest_df.iloc[0] if len(latest_df) > 0 else None
    previous = latest_df.iloc[1] if len(latest_df) > 1 else None

    # ============================================
    # KPI CARDS
    # ============================================
    st.subheader("Key Metrics (Current Week)")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        ssr_value = current['ssr'] if current is not None else 0
        ssr_delta = round(ssr_value - previous['ssr'], 1) if previous is not None else None
        st.metric(
            label="SSR (Session Save Rate)",
            value=f"{ssr_value:.1f}%",
            delta=f"{ssr_delta:+.1f}%" if ssr_delta else None,
            help="Percentage of planning sessions with at least 1 save"
        )

    with col2:
        scr3_value = current['scr3'] if current is not None else 0
        scr3_delta = round(scr3_value - previous['scr3'], 1) if previous is not None else None
        st.metric(
            label="SCR3 (3+ Saves Rate)",
            value=f"{scr3_value:.1f}%",
            delta=f"{scr3_delta:+.1f}%" if scr3_delta else None,
            help="Percentage of planning sessions with 3+ unique saves"
        )

    with col3:
        wap_value = int(current['wap']) if current is not None else 0
        wap_delta = int(wap_value - previous['wap']) if previous is not None else None
        st.metric(
            label="WAP (Weekly Active Planners)",
            value=f"{wap_value:,}",
            delta=f"{wap_delta:+,}" if wap_delta else None,
            help="Unique users with save or multiplayer activity"
        )

    with col4:
        ttfs_value = current['median_ttfs_seconds'] if current is not None else 0
        ttfs_delta = round(ttfs_value - previous['median_ttfs_seconds'], 0) if previous is not None else None
        st.metric(
            label="Median TTFS",
            value=f"{ttfs_value:.0f}s",
            delta=f"{ttfs_delta:+.0f}s" if ttfs_delta else None,
            delta_color="inverse",
            help="Time to First Save (lower is better)"
        )

    st.divider()

    # ============================================
    # TRENDS
    # ============================================
    st.subheader("12-Week Trends")

    # Sort by week for proper charting
    weekly_df = weekly_df.sort_values('session_week')

    tab1, tab2 = st.tabs(["Save Metrics", "Activity Metrics"])

    with tab1:
        col1, col2 = st.columns(2)

        with col1:
            # SSR and SCR3 trend
            fig = create_kpi_trend_chart(
                weekly_df,
                x_col='session_week',
                kpi_cols=['ssr', 'scr3'],
                title="Save Rate Trends",
                y_label="Rate (%)"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # TTFS trend
            fig = create_line_chart(
                weekly_df,
                x='session_week',
                y='median_ttfs_seconds',
                title="Median Time to First Save",
                y_label="Seconds"
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        col1, col2 = st.columns(2)

        with col1:
            # WAP trend
            fig = create_line_chart(
                weekly_df,
                x='session_week',
                y='wap',
                title="Weekly Active Planners",
                y_label="Users"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Sessions trend
            fig = create_line_chart(
                weekly_df,
                x='session_week',
                y='planning_sessions',
                title="Planning Sessions",
                y_label="Sessions"
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ============================================
    # SURFACE BREAKDOWN
    # ============================================
    st.subheader("Metrics by Surface")

    if not surface_df.empty:
        # Get latest week's surface data
        latest_week = surface_df['session_week'].max()
        latest_surface = surface_df[surface_df['session_week'] == latest_week]

        col1, col2 = st.columns(2)

        with col1:
            fig = create_surface_comparison_chart(
                latest_surface,
                x_col='primary_surface',
                y_col='ssr_percent',
                color_col='primary_surface',
                title=f"SSR by Surface (Week of {latest_week})"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = create_surface_comparison_chart(
                latest_surface,
                x_col='primary_surface',
                y_col='total_sessions',
                color_col='primary_surface',
                title=f"Sessions by Surface (Week of {latest_week})"
            )
            st.plotly_chart(fig, use_container_width=True)

        # Surface breakdown table
        st.markdown("**Surface Performance Summary**")
        display_df = latest_surface[[
            'primary_surface', 'total_sessions', 'ssr_percent', 'scr3_percent',
            'avg_saves_per_saver', 'median_ttfs_seconds', 'conversion_rate'
        ]].copy()
        display_df.columns = [
            'Surface', 'Sessions', 'SSR %', 'SCR3 %',
            'Avg Saves/Saver', 'Median TTFS (s)', 'Conversion %'
        ]
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.divider()

    # ============================================
    # WEEKLY DATA TABLE
    # ============================================
    with st.expander("Weekly Data Table", expanded=False):
        display_cols = [
            'session_week', 'planning_sessions', 'ssr', 'scr3',
            'wap', 'median_ttfs_seconds', 'conversion_rate', 'sessions_wow_growth'
        ]
        display_df = weekly_df[display_cols].copy()
        display_df.columns = [
            'Week', 'Planning Sessions', 'SSR %', 'SCR3 %',
            'WAP', 'Median TTFS (s)', 'Conversion %', 'WoW Growth %'
        ]
        st.dataframe(display_df.sort_values('Week', ascending=False), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading North Star data: {str(e)}")
    with st.expander("🔧 Troubleshooting"):
        st.markdown("""
        **Common issues:**

        1. **Models not built**: Run the dbt models first:
           ```bash
           dbt run --select north_star_weekly north_star_by_surface
           ```

        2. **Database connection**: Verify your Supabase credentials in `.streamlit/secrets.toml`

        3. **Missing dependencies**: Ensure all silver models are built:
           ```bash
           dbt run --select int_derived_sessions int_session_events north_star_session_metrics
           ```
        """)

# Footer
add_deck_footer()
