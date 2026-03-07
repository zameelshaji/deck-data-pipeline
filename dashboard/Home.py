"""DECK Analytics Dashboard - Executive Overview (Home Page)"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from utils.data_loader import (
    load_homepage_totals,
    load_latest_mau,
    load_latest_wau,
    load_referral_metrics,
    load_growth_snapshot,
    load_dau_sparkline,
    load_weekly_health_comparison,
    load_top_places_this_week,
)
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS

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
st.title("DECK Analytics — Executive Overview")

# Refresh button
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

try:
    # Load data — gold_homepage_totals is the single source of truth
    homepage = load_homepage_totals()
    mau_data = load_latest_mau()
    wau_data = load_latest_wau()
    referral_data = load_referral_metrics()

    if homepage.empty:
        st.warning("No data available. Run `dbt run` to populate gold_homepage_totals.")
        st.stop()

    # Extract values from the single-row homepage totals table
    h = homepage.iloc[0]
    mau = mau_data.iloc[0] if not mau_data.empty else None
    wau = wau_data.iloc[0] if not wau_data.empty else None
    referral_metrics = referral_data.iloc[0] if not referral_data.empty else None

    # Display last updated
    st.caption(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    st.divider()

    # ============================================
    # SECTION A: Lifetime Totals (EXISTING — kept as-is)
    # ============================================
    st.subheader("Lifetime Totals")

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

    st.markdown('<div class="home-metric">', unsafe_allow_html=True)

    # Row 1
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Total Users",
            value=f"{int(h['total_signups']):,}",
            help="Total number of registered users on the platform"
        )

    with col2:
        st.metric(
            label="Activated Users",
            value=f"{int(h['total_activated_users']):,}",
            help="Users who have prompted, saved, or shared at least once. Excludes test users."
        )

    with col3:
        st.metric(
            label="Monthly Active Users (MAU)",
            value=f"{int(h['mau']):,}",
            delta=f"{mau['mom_growth_percent']:.1f}%" if mau is not None and pd.notna(mau.get('mom_growth_percent')) else None,
            help="Unique users who performed at least one action in the last 30 days."
        )

    # Row 2
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Total Prompts",
            value=f"{int(h['total_prompts']):,}",
            help="Cumulative AI queries submitted by all users"
        )

    with col2:
        st.metric(
            label="Total Swipes",
            value=f"{int(h['total_swipes']):,}",
            help="Cumulative card swipes (left or right) by all users"
        )

    with col3:
        total_saves_shares = int(h['total_saves']) + int(h['total_shares'])
        st.metric(
            label="Total Saves & Shares",
            value=f"{total_saves_shares:,}",
            help="Total cards saved and shared by all users"
        )

    # Row 3
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Decks Created",
            value=f"{int(h['total_boards']):,}",
            help="Total custom boards/wishlists created by users"
        )

    with col2:
        st.metric(
            label="Multiplayer Sessions",
            value=f"{int(h['total_multiplayer_sessions']):,}",
            help="Total collaborative planning sessions created"
        )

    with col3:
        st.metric(
            label="Referrals Made",
            value=f"{int(referral_metrics['total_referrals_made']):,}" if referral_metrics is not None else "0",
            help="Users who provided their referral codes to new users"
        )

    st.markdown('</div>', unsafe_allow_html=True)

    st.divider()

    # ============================================
    # SECTION B: Growth Snapshot (NEW)
    # ============================================
    st.subheader("Growth Snapshot")

    growth_df = load_growth_snapshot()

    if not growth_df.empty:
        col1, col2, col3 = st.columns(3)

        for idx, metric_name in enumerate(['dau', 'wau', 'mau']):
            row = growth_df[growth_df['metric'] == metric_name]
            if not row.empty:
                r = row.iloc[0]
                val = int(r['value']) if pd.notna(r['value']) else 0
                delta = r['delta']
                delta_str = f"{delta:.1f}%" if pd.notna(delta) else None
                label = {'dau': 'DAU', 'wau': 'WAU', 'mau': 'MAU'}[metric_name]
                delta_label = {'dau': 'WoW', 'wau': 'WoW', 'mau': 'MoM'}[metric_name]
                help_text = {
                    'dau': 'Daily Active Users — unique users active today',
                    'wau': 'Weekly Active Users — unique users active this week',
                    'mau': 'Monthly Active Users — unique users active this month',
                }[metric_name]

                with [col1, col2, col3][idx]:
                    st.metric(
                        label=label,
                        value=f"{val:,}",
                        delta=f"{delta_str} {delta_label}" if delta_str else None,
                        help=help_text,
                    )

    # DAU Sparkline
    sparkline_df = load_dau_sparkline(days=30)
    if not sparkline_df.empty:
        fig = go.Figure(go.Scatter(
            x=sparkline_df['activity_date'],
            y=sparkline_df['total_active_users'],
            mode='lines',
            line=dict(color='#E91E8C', width=2),
            fill='tozeroy',
            fillcolor='rgba(233, 30, 140, 0.1)',
        ))
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=80,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

    st.divider()

    # ============================================
    # SECTION C: This Week's Health (NEW)
    # ============================================
    st.subheader("This Week's Health")

    health_df = load_weekly_health_comparison()

    if not health_df.empty:
        hw = health_df.iloc[0]

        def safe_pct(val):
            return float(val) * 100 if val is not None and pd.notna(val) else 0.0

        def safe_delta(this_val, last_val):
            t = safe_pct(this_val)
            l = safe_pct(last_val)
            if l == 0 and t == 0:
                return None
            return round(t - l, 1)

        ssr_tw = safe_pct(hw.get('ssr_this_week'))
        ssr_delta = safe_delta(hw.get('ssr_this_week'), hw.get('ssr_last_week'))
        shr_tw = safe_pct(hw.get('shr_this_week'))
        shr_delta = safe_delta(hw.get('shr_this_week'), hw.get('shr_last_week'))
        psr_tw = safe_pct(hw.get('psr_this_week'))
        psr_delta = safe_delta(hw.get('psr_this_week'), hw.get('psr_last_week'))
        nvr_tw = safe_pct(hw.get('nvr_this_week'))
        nvr_delta = safe_delta(hw.get('nvr_this_week'), hw.get('nvr_last_week'))

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "SSR (Save Rate)", f"{ssr_tw:.1f}%",
                delta=f"{ssr_delta:+.1f}pp" if ssr_delta is not None else None,
                help="Session Save Rate: % of sessions with at least one save this week"
            )
        with col2:
            st.metric(
                "SHR (Share Rate)", f"{shr_tw:.1f}%",
                delta=f"{shr_delta:+.1f}pp" if shr_delta is not None else None,
                help="Session Share Rate: % of sessions with at least one share this week"
            )
        with col3:
            st.metric(
                "PSR (Broad)", f"{psr_tw:.1f}%",
                delta=f"{psr_delta:+.1f}pp" if psr_delta is not None else None,
                help="Plan Survival Rate: % of sessions with both save AND share this week"
            )
        with col4:
            st.metric(
                "NVR (No-Value)", f"{nvr_tw:.1f}%",
                delta=f"{nvr_delta:+.1f}pp" if nvr_delta is not None else None,
                delta_color="inverse",
                help="No-Value Rate: % of sessions with zero saves and zero shares (lower is better)"
            )

    st.divider()

    # ============================================
    # SECTION D: Quick Wins (NEW)
    # ============================================
    st.subheader("Top 5 Places Saved This Week")

    top_places_df = load_top_places_this_week()

    if not top_places_df.empty:
        st.dataframe(
            top_places_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "place_name": st.column_config.TextColumn("Place"),
                "category": st.column_config.TextColumn("Category"),
                "saves_last_7d": st.column_config.NumberColumn("Saves (7d)", format="%d"),
                "save_rate_pct": st.column_config.NumberColumn("Save Rate %", format="%.1f%%"),
            }
        )
    else:
        st.info("No place saves recorded this week.")

except Exception as e:
    st.error(f"Error loading data: {str(e)}")

    with st.expander("How to fix this issue"):
        st.markdown("""
        **Common Solutions:**

        1. **Check Database Connection**
           - Verify your database credentials in `.streamlit/secrets.toml`
           - Ensure your database server is running and accessible

        2. **Check Data Models**
           - Ensure all required analytics tables exist
           - Run `dbt run` to populate tables
        """)

    st.exception(e)

# Footer
add_deck_footer()
