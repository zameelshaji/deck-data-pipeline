"""Multiplayer Analysis Dashboard"""

import streamlit as st
import pandas as pd
from datetime import datetime

from utils.styling import apply_deck_branding, add_deck_footer
from utils.data_loader import load_north_star_multiplayer
from utils.visualizations import (
    create_kpi_trend_chart,
    create_line_chart,
    create_multi_line_chart,
    create_funnel_chart,
    create_bar_chart
)

# Page config
st.set_page_config(
    page_title="Multiplayer Analysis - DECK Analytics",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("🎮 Multiplayer Analysis")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.divider()

# Important context
st.info("""
**Note:** Multiplayer is analyzed as its own engagement feature, NOT as a share proxy.
Multiplayer sessions represent collaborative planning, while shares (deck_shared) are not currently tracked.
""")

try:
    # Load data
    mp_df = load_north_star_multiplayer(weeks=12)

    if mp_df.empty:
        st.warning("No multiplayer data available. Please run dbt models first.")
        st.stop()

    # Sort for charting
    mp_df = mp_df.sort_values('metric_week')

    # Get latest week
    latest = mp_df.iloc[-1] if len(mp_df) > 0 else None
    previous = mp_df.iloc[-2] if len(mp_df) > 1 else None

    # ============================================
    # KPI CARDS
    # ============================================
    st.subheader("Current Week Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        value = int(latest['true_multiplayer_sessions']) if latest is not None else 0
        delta = int(value - previous['true_multiplayer_sessions']) if previous is not None else None
        st.metric(
            label="True Multiplayer Sessions",
            value=f"{value:,}",
            delta=f"{delta:+,}" if delta else None,
            help="Sessions with 2+ participants"
        )

    with col2:
        value = float(latest['collaboration_rate']) if latest is not None else 0
        delta = round(value - previous['collaboration_rate'], 1) if previous is not None else None
        st.metric(
            label="Collaboration Rate",
            value=f"{value:.1f}%",
            delta=f"{delta:+.1f}%" if delta else None,
            help="% of created sessions that achieved 2+ participants"
        )

    with col3:
        value = float(latest['avg_participants']) if latest is not None else 0
        delta = round(value - previous['avg_participants'], 2) if previous is not None else None
        st.metric(
            label="Avg Participants",
            value=f"{value:.1f}",
            delta=f"{delta:+.2f}" if delta else None,
            help="Average participants per true multiplayer session"
        )

    with col4:
        value = float(latest['clear_consensus_rate']) if latest is not None else 0
        delta = round(value - previous['clear_consensus_rate'], 1) if previous is not None else None
        st.metric(
            label="Consensus Rate",
            value=f"{value:.1f}%",
            delta=f"{delta:+.1f}%" if delta else None,
            help="% of sessions reaching clear consensus"
        )

    st.divider()

    # ============================================
    # CREATION & PARTICIPATION TRENDS
    # ============================================
    st.subheader("Creation & Participation")

    col1, col2 = st.columns(2)

    with col1:
        # Sessions created trend
        fig = create_multi_line_chart(
            mp_df,
            x='metric_week',
            y_columns=['sessions_created', 'true_multiplayer_sessions'],
            title="Sessions Created vs True Multiplayer",
            y_label="Sessions"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Creators vs Joiners
        fig = create_multi_line_chart(
            mp_df,
            x='metric_week',
            y_columns=['unique_creators', 'unique_joiners'],
            title="Unique Creators vs Joiners",
            y_label="Users"
        )
        st.plotly_chart(fig, use_container_width=True)

    # Virality metrics
    col1, col2 = st.columns(2)

    with col1:
        fig = create_line_chart(
            mp_df,
            x='metric_week',
            y='joins_per_session',
            title="Joins per Session Created",
            y_label="Joins"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = create_line_chart(
            mp_df,
            x='metric_week',
            y='collaboration_rate',
            title="Collaboration Rate Trend",
            y_label="Rate (%)"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ============================================
    # COLLABORATION FUNNEL
    # ============================================
    st.subheader("Collaboration Funnel (Latest Week)")

    if latest is not None:
        stages = ['Created', '2+ Participants', 'Had Voting', 'Clear Consensus']
        values = [
            int(latest['sessions_created']),
            int(latest['true_multiplayer_sessions']),
            int(latest['sessions_with_voting']),
            int(latest['sessions_with_clear_consensus'])
        ]

        # Only show funnel if we have data
        if values[0] > 0:
            fig = create_funnel_chart(stages, values, title="Multiplayer Collaboration Funnel")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Not enough data for funnel visualization.")

    st.divider()

    # ============================================
    # VOTING ENGAGEMENT
    # ============================================
    st.subheader("Voting Engagement")

    col1, col2 = st.columns(2)

    with col1:
        fig = create_kpi_trend_chart(
            mp_df,
            x_col='metric_week',
            kpi_cols=['voting_rate', 'clear_consensus_rate'],
            title="Voting & Consensus Rates",
            y_label="Rate (%)"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = create_multi_line_chart(
            mp_df,
            x='metric_week',
            y_columns=['avg_votes_per_session', 'avg_like_rate'],
            title="Voting Metrics",
            y_label="Value"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ============================================
    # AI-INITIATED SESSIONS
    # ============================================
    st.subheader("AI-Initiated Multiplayer")

    col1, col2 = st.columns(2)

    with col1:
        fig = create_line_chart(
            mp_df,
            x='metric_week',
            y='ai_initiated_sessions',
            title="AI-Initiated Sessions",
            y_label="Sessions"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = create_line_chart(
            mp_df,
            x='metric_week',
            y='ai_source_rate',
            title="AI Source Rate",
            y_label="Rate (%)"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ============================================
    # WEEKLY DATA TABLE
    # ============================================
    with st.expander("Weekly Multiplayer Data", expanded=False):
        display_cols = [
            'metric_week', 'sessions_created', 'true_multiplayer_sessions',
            'collaboration_rate', 'avg_participants', 'sessions_with_voting',
            'voting_rate', 'clear_consensus_rate', 'ai_source_rate'
        ]
        display_df = mp_df[display_cols].copy()
        display_df.columns = [
            'Week', 'Created', 'True MP', 'Collab %', 'Avg Participants',
            'With Voting', 'Vote %', 'Consensus %', 'AI Source %'
        ]
        st.dataframe(display_df.sort_values('Week', ascending=False), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading multiplayer data: {str(e)}")
    with st.expander("🔧 Troubleshooting"):
        st.markdown("""
        **To fix this issue:**

        1. Run the dbt models:
           ```bash
           dbt run --select north_star_multiplayer_analysis
           ```

        2. Verify the dependencies are built:
           ```bash
           dbt run --select int_user_multiplayer_actions stg_multiplayer
           ```
        """)

# Footer
add_deck_footer()
