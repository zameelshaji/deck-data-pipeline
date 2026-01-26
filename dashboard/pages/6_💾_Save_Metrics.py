"""Save Metrics Deep Dive Dashboard"""

import streamlit as st
import pandas as pd
from datetime import datetime

from utils.styling import apply_deck_branding, add_deck_footer
from utils.data_loader import (
    load_north_star_weekly,
    load_north_star_save_analysis,
    load_north_star_by_surface,
    load_north_star_session_metrics
)
from utils.visualizations import (
    create_kpi_trend_chart,
    create_line_chart,
    create_save_source_pie,
    create_ttfs_histogram,
    create_stacked_bar_chart
)

# Page config
st.set_page_config(
    page_title="Save Metrics - DECK Analytics",
    page_icon="💾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("💾 Save Metrics Deep Dive")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.divider()

try:
    # Load data
    weekly_df = load_north_star_weekly(weeks=12)
    save_analysis_df = load_north_star_save_analysis(weeks=12)
    surface_df = load_north_star_by_surface(weeks=8)

    if weekly_df.empty:
        st.warning("No save metrics data available. Please run dbt models first.")
        st.stop()

    # Sort for charting
    weekly_df = weekly_df.sort_values('session_week')

    # ============================================
    # SSR AND SCR3 TRENDS
    # ============================================
    st.subheader("Save Rate Trends")

    col1, col2 = st.columns(2)

    with col1:
        # SSR trend with target line
        fig = create_line_chart(
            weekly_df,
            x='session_week',
            y='ssr',
            title="Session Save Rate (SSR)",
            y_label="Rate (%)"
        )
        # Could add horizontal line for target here
        st.plotly_chart(fig, use_container_width=True)

        # SSR explanation
        st.caption("**SSR** = Sessions with ≥1 save / Total planning sessions")

    with col2:
        # SCR3 trend
        fig = create_line_chart(
            weekly_df,
            x='session_week',
            y='scr3',
            title="Session Conversion Rate 3+ (SCR3)",
            y_label="Rate (%)"
        )
        st.plotly_chart(fig, use_container_width=True)

        st.caption("**SCR3** = Sessions with ≥3 saves / Total planning sessions")

    st.divider()

    # ============================================
    # SAVE SOURCE BREAKDOWN
    # ============================================
    st.subheader("Save Source Analysis")

    if not save_analysis_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            # Aggregate by source for pie chart
            latest_week = save_analysis_df['save_week'].max()
            latest_saves = save_analysis_df[save_analysis_df['save_week'] == latest_week]

            source_totals = latest_saves.groupby('save_source').agg({
                'save_count': 'sum',
                'unique_savers': 'sum'
            }).reset_index()

            if not source_totals.empty:
                fig = create_save_source_pie(
                    source_totals,
                    title=f"Save Sources (Week of {latest_week})"
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Source trend over time
            source_weekly = save_analysis_df.groupby(['save_week', 'save_source']).agg({
                'save_count': 'sum'
            }).reset_index()

            source_pivot = source_weekly.pivot(
                index='save_week',
                columns='save_source',
                values='save_count'
            ).reset_index().fillna(0)

            if len(source_pivot.columns) > 1:
                save_cols = [c for c in source_pivot.columns if c != 'save_week']
                fig = create_stacked_bar_chart(
                    source_pivot.sort_values('save_week'),
                    x='save_week',
                    y_columns=save_cols,
                    title="Saves by Source Over Time"
                )
                st.plotly_chart(fig, use_container_width=True)

        # Deduplication metrics
        st.markdown("**Deduplication Analysis**")
        dedup_stats = save_analysis_df.groupby('save_week').agg({
            'week_total_saves': 'first',
            'users_using_both_sources': 'first'
        }).reset_index().sort_values('save_week', ascending=False)

        col1, col2, col3 = st.columns(3)
        if len(dedup_stats) > 0:
            latest = dedup_stats.iloc[0]
            col1.metric("Total Saves (Latest Week)", f"{int(latest['week_total_saves']):,}")
            col2.metric("Users Using Both Sources", f"{int(latest['users_using_both_sources']):,}")
    else:
        st.info("No save source analysis data available.")

    st.divider()

    # ============================================
    # TIME TO FIRST SAVE (TTFS)
    # ============================================
    st.subheader("Time to First Save (TTFS)")

    col1, col2 = st.columns(2)

    with col1:
        # TTFS trend
        fig = create_kpi_trend_chart(
            weekly_df,
            x_col='session_week',
            kpi_cols=['median_ttfs_seconds', 'avg_ttfs_seconds'],
            title="TTFS Trend (Median vs Average)",
            y_label="Seconds"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Try to load session-level data for histogram
        try:
            session_df = load_north_star_session_metrics(weeks=2)
            if not session_df.empty and 'ttfs_seconds' in session_df.columns:
                ttfs_data = session_df[session_df['ttfs_seconds'].notna() & (session_df['ttfs_seconds'] < 600)]
                if not ttfs_data.empty:
                    fig = create_ttfs_histogram(
                        ttfs_data,
                        col='ttfs_seconds',
                        title="TTFS Distribution (Last 2 Weeks)"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Not enough TTFS data for distribution chart.")
            else:
                st.info("Session-level TTFS data not available.")
        except Exception:
            st.info("Could not load session-level TTFS data.")

    st.divider()

    # ============================================
    # SAVE METRICS BY SURFACE
    # ============================================
    st.subheader("Save Metrics by Surface")

    if not surface_df.empty:
        surface_df = surface_df.sort_values('session_week')

        # Filter to last 8 weeks
        latest_weeks = surface_df['session_week'].unique()[-8:]
        recent_surface = surface_df[surface_df['session_week'].isin(latest_weeks)]

        col1, col2 = st.columns(2)

        with col1:
            # SSR by surface over time
            surface_pivot = recent_surface.pivot(
                index='session_week',
                columns='primary_surface',
                values='ssr_percent'
            ).reset_index()

            surface_cols = [c for c in surface_pivot.columns if c != 'session_week']
            if surface_cols:
                fig = create_kpi_trend_chart(
                    surface_pivot,
                    x_col='session_week',
                    kpi_cols=surface_cols,
                    title="SSR by Surface",
                    y_label="SSR %"
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Average saves per saver by surface
            saves_pivot = recent_surface.pivot(
                index='session_week',
                columns='primary_surface',
                values='avg_saves_per_saver'
            ).reset_index()

            saves_cols = [c for c in saves_pivot.columns if c != 'session_week']
            if saves_cols:
                fig = create_kpi_trend_chart(
                    saves_pivot,
                    x_col='session_week',
                    kpi_cols=saves_cols,
                    title="Avg Saves per Saver by Surface",
                    y_label="Saves"
                )
                st.plotly_chart(fig, use_container_width=True)

        # Surface comparison table
        latest_week = surface_df['session_week'].max()
        latest_surface = surface_df[surface_df['session_week'] == latest_week]

        st.markdown(f"**Surface Comparison (Week of {latest_week})**")
        display_df = latest_surface[[
            'primary_surface', 'total_sessions', 'ssr_percent', 'scr3_percent',
            'avg_saves_per_saver', 'median_ttfs_seconds'
        ]].copy()
        display_df.columns = ['Surface', 'Sessions', 'SSR %', 'SCR3 %', 'Avg Saves/Saver', 'Median TTFS (s)']
        st.dataframe(display_df.sort_values('Sessions', ascending=False), use_container_width=True, hide_index=True)

    st.divider()

    # ============================================
    # SAVE DENSITY ANALYSIS
    # ============================================
    st.subheader("Save Density")

    col1, col2 = st.columns(2)

    with col1:
        # Average save density trend
        fig = create_line_chart(
            weekly_df,
            x='session_week',
            y='avg_save_density',
            title="Avg Saves per Saving Session",
            y_label="Saves"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Total saves trend
        fig = create_line_chart(
            weekly_df,
            x='session_week',
            y='total_unique_saves',
            title="Total Unique Saves",
            y_label="Saves"
        )
        st.plotly_chart(fig, use_container_width=True)

    # Weekly data table
    with st.expander("Weekly Save Data", expanded=False):
        display_cols = [
            'session_week', 'sessions_with_save', 'sessions_with_scr3',
            'ssr', 'scr3', 'total_unique_saves', 'avg_save_density', 'median_ttfs_seconds'
        ]
        display_df = weekly_df[display_cols].copy()
        display_df.columns = [
            'Week', 'Sessions w/ Save', 'Sessions w/ 3+ Saves',
            'SSR %', 'SCR3 %', 'Total Saves', 'Avg Save Density', 'Median TTFS (s)'
        ]
        st.dataframe(display_df.sort_values('Week', ascending=False), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading save metrics: {str(e)}")
    with st.expander("🔧 Troubleshooting"):
        st.markdown("""
        **To fix this issue:**

        1. Run the dbt models:
           ```bash
           dbt run --select north_star_weekly north_star_save_analysis north_star_by_surface
           ```

        2. Verify the silver models are built:
           ```bash
           dbt run --select int_user_saves_unified
           ```
        """)

# Footer
add_deck_footer()
