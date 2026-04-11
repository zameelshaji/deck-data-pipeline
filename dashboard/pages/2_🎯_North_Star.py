"""DECK North Star Metrics Dashboard — PSR ladder + core health metrics"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.filters import render_sidebar_filters
from utils.data_loader import (
    load_north_star_daily,
    load_north_star_weekly,
    load_north_star_headline,
    load_psr_ladder_current,
    load_active_planners_trend,
    load_session_diagnostics,
    load_surface_performance,
)

st.set_page_config(
    page_title="North Star Metrics | DECK Analytics",
    page_icon="🎯",
    layout="wide"
)

apply_deck_branding()

st.title("North Star Metrics")
st.caption("PSR ladder and core health metrics — are we getting better?")

# --- Sidebar Filters ---
filters = render_sidebar_filters(
    show_date_range=True,
    show_app_version=True,
    show_data_source=True,
    show_session_type=True,
    default_days=90,
)

start_date = filters['start_date']
end_date = filters['end_date']
app_version = filters['app_version']
data_source = filters['data_source']
session_type = filters['session_type']

# --- Load Data ---
try:
    daily_df = load_north_star_daily(
        data_source=data_source,
        session_type=session_type,
        start_date=start_date,
        end_date=end_date,
        app_version=app_version,
    )
    weekly_df = load_north_star_weekly(data_source=data_source, session_type=session_type, app_version=app_version)
    ladder_df = load_psr_ladder_current(
        data_source=data_source, session_type=session_type,
        app_version=app_version, start_date=start_date, end_date=end_date
    )
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Run `dbt run --select +fct_north_star_daily` first.")
    st.stop()

if daily_df.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

# ============================================================================
# Section A: Headline KPIs
# ============================================================================
st.subheader("Headline KPIs")

headline = load_north_star_headline(
    data_source=data_source,
    session_type=session_type,
    app_version=app_version,
    start_date=start_date,
    end_date=end_date,
)

if headline:
    total = headline.get('total_sessions', 0)
    ssr_val = float(headline.get('ssr', 0))
    shr_val = float(headline.get('shr', 0))
    psr_val = float(headline.get('psr_broad', 0))
    nvr_val = float(headline.get('nvr', 0))

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "PSR (Broad)", f"{psr_val * 100:.1f}%",
            help="Plan Survival Rate: % of sessions with both a save AND a share. DECK's North Star metric."
        )
    with col2:
        st.metric(
            "SSR (Save Rate)", f"{ssr_val * 100:.1f}%",
            help="Session Save Rate: % of sessions where user saved at least one card."
        )
    with col3:
        st.metric(
            "SHR (Share Rate)", f"{shr_val * 100:.1f}%",
            help="Session Share Rate: % of sessions where user shared."
        )
    with col4:
        st.metric(
            "NVR (No-Value)", f"{nvr_val * 100:.1f}%",
            help="No-Value Rate: % of sessions with zero saves and zero shares."
        )

    st.metric(
        "Unique Active Planners",
        f"{headline.get('unique_active_planners', 0):,}",
        help="Distinct users who prompted, saved, or shared in the selected period"
    )

st.divider()

# ============================================================================
# Section B: PSR Ladder Funnel
# ============================================================================
st.subheader("PSR Ladder Funnel")

if not ladder_df.empty:
    row = ladder_df.iloc[0]
    total_s = int(row.get('total_sessions', 0))
    prompt_s = int(row.get('sessions_with_prompt', 0))
    swipe3_s = int(row.get('sessions_with_3plus_swipes', 0))
    saves_s = int(row.get('sessions_with_save', 0))
    shares_s = int(row.get('sessions_with_share', 0))
    psr_b = int(row.get('sessions_with_psr_broad', 0))

    if total_s > 0:
        fig = go.Figure(go.Funnel(
            y=['Total Sessions', 'Sessions with Prompt', 'Sessions with 3+ Swipes', 'Sessions with Save (SSR)', 'Sessions with Share (SHR)', 'PSR Broad (Save+Share)'],
            x=[total_s, prompt_s, swipe3_s, saves_s, shares_s, psr_b],
            textinfo="value+percent initial",
            marker=dict(color=[
                BRAND_COLORS['info'],
                BRAND_COLORS['primary'],
                '#FF6B35',
                BRAND_COLORS['accent'],
                '#E91E8C',
                BRAND_COLORS['success'],
            ])
        ))
        fig.update_layout(
            margin=dict(l=20, r=20, t=20, b=20),
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor='white', paper_bgcolor='white',
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data available for the selected filters.")

st.divider()

# ============================================================================
# Section C: Metrics Ladder Over Time
# ============================================================================
st.subheader("Metrics Ladder Over Time")

use_rolling = st.toggle("Show 7-day rolling average", value=True)

if not daily_df.empty:
    trend_df = daily_df.sort_values('metric_date').copy()

    if use_rolling:
        y_ssr = trend_df['ssr_7d_avg'].astype(float) * 100
        y_shr = trend_df['shr_7d_avg'].astype(float) * 100
        y_psr = trend_df['psr_broad_7d_avg'].astype(float) * 100
        y_nvr = trend_df['nvr_7d_avg'].astype(float) * 100
        suffix = " (7d avg)"
    else:
        trend_df['ssr_raw'] = trend_df.apply(
            lambda r: float(r['sessions_with_save']) / float(r['total_sessions']) * 100
            if float(r['total_sessions']) > 0 else 0, axis=1)
        trend_df['shr_raw'] = trend_df.apply(
            lambda r: float(r['sessions_with_share']) / float(r['total_sessions']) * 100
            if float(r['total_sessions']) > 0 else 0, axis=1)
        trend_df['psr_raw'] = trend_df.apply(
            lambda r: float(r['sessions_with_psr_broad']) / float(r['total_sessions']) * 100
            if float(r['total_sessions']) > 0 else 0, axis=1)
        trend_df['nvr_raw'] = trend_df.apply(
            lambda r: float(r['no_value_sessions']) / float(r['total_sessions']) * 100
            if float(r['total_sessions']) > 0 else 0, axis=1)
        y_ssr = trend_df['ssr_raw']
        y_shr = trend_df['shr_raw']
        y_psr = trend_df['psr_raw']
        y_nvr = trend_df['nvr_raw']
        suffix = ""

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=trend_df['metric_date'], y=y_ssr, name=f'SSR{suffix}',
                             line=dict(color=BRAND_COLORS['info'], width=2)))
    fig.add_trace(go.Scatter(x=trend_df['metric_date'], y=y_shr, name=f'SHR{suffix}',
                             line=dict(color=BRAND_COLORS['accent'], width=2)))
    fig.add_trace(go.Scatter(x=trend_df['metric_date'], y=y_psr, name=f'PSR Broad{suffix}',
                             line=dict(color='#E91E8C', width=2.5)))
    fig.add_trace(go.Scatter(x=trend_df['metric_date'], y=y_nvr, name=f'NVR{suffix}',
                             line=dict(color=BRAND_COLORS['text_tertiary'], width=1.5, dash='dot')))

    fig.update_layout(
        yaxis_title="Rate (%)",
        font=dict(family="Inter, system-ui, sans-serif", size=13, color=BRAND_COLORS['text_primary']),
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=40, r=20, t=40, b=40),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        xaxis=dict(showgrid=False, linecolor=BRAND_COLORS['border']),
        yaxis=dict(showgrid=True, gridcolor=BRAND_COLORS['bg_secondary'], linecolor=BRAND_COLORS['border']),
        hoverlabel=dict(bgcolor='white', font_size=12),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ============================================================================
# Section D: Weekly PSR + WAP Trend
# ============================================================================
st.subheader("Weekly PSR + Active Planners")

if not weekly_df.empty:
    w_df = weekly_df.sort_values('metric_week').copy()

    try:
        ap_df = load_active_planners_trend()
        weekly_ap = ap_df[ap_df['period_type'] == 'week'].copy() if not ap_df.empty else pd.DataFrame()
    except Exception:
        weekly_ap = pd.DataFrame()

    from plotly.subplots import make_subplots
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(x=w_df['metric_week'], y=w_df['psr_broad'].astype(float) * 100,
                   name='PSR Broad %', line=dict(color='#E91E8C', width=2.5)),
        secondary_y=False
    )

    if not weekly_ap.empty:
        fig.add_trace(
            go.Bar(x=weekly_ap['period_start'], y=weekly_ap['active_planners'],
                   name='Weekly Active Planners', marker_color='rgba(99, 102, 241, 0.3)'),
            secondary_y=True
        )

    fig.update_layout(
        font=dict(family="Inter, system-ui, sans-serif", size=13),
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        hoverlabel=dict(bgcolor='white', font_size=12),
    )
    fig.update_yaxes(title_text="PSR Broad (%)", secondary_y=False,
                     showgrid=True, gridcolor=BRAND_COLORS['bg_secondary'])
    fig.update_yaxes(title_text="Active Planners", secondary_y=True, showgrid=False)
    fig.update_xaxes(showgrid=False)

    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ============================================================================
# Section E: Active Planners
# ============================================================================
st.subheader("Active Planners Trend")

try:
    ap_df = load_active_planners_trend()
    if not ap_df.empty:
        weekly_ap = ap_df[ap_df['period_type'] == 'week'].sort_values('period_start')
        monthly_ap = ap_df[ap_df['period_type'] == 'month'].sort_values('period_start')

        fig = go.Figure()
        if not weekly_ap.empty:
            fig.add_trace(go.Scatter(
                x=weekly_ap['period_start'], y=weekly_ap['active_planners'],
                name='WAP', line=dict(color='#E91E8C', width=2),
            ))
        if not monthly_ap.empty:
            fig.add_trace(go.Scatter(
                x=monthly_ap['period_start'], y=monthly_ap['active_planners'],
                name='MAP', line=dict(color=BRAND_COLORS['accent'], width=2),
            ))
        fig.update_layout(
            yaxis_title="Planners",
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor='white', paper_bgcolor='white',
            margin=dict(l=40, r=20, t=20, b=40),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
            xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor=BRAND_COLORS['bg_secondary']),
        )
        st.plotly_chart(fig, use_container_width=True)

        if not monthly_ap.empty:
            latest_stickiness = monthly_ap.iloc[-1].get('planner_stickiness', None)
            if latest_stickiness is not None and pd.notna(latest_stickiness):
                st.metric("Planner Stickiness (WAP/MAP)", f"{float(latest_stickiness) * 100:.1f}%",
                          help="Weekly Active Planners / Monthly Active Planners")
except Exception:
    st.info("No active planners data available.")

st.divider()

# ============================================================================
# Section F: Surface Attribution (NEW — Phase D, PR #50)
# ============================================================================
st.subheader("Surface Attribution")
st.caption(
    "Which parts of the app drive the North Star? "
    "Sessions are attributed to their initiation surface. Telemetry era only (2026-01-30+)."
)

try:
    surface_df = load_surface_performance(start_date=start_date, end_date=end_date)
except Exception as e:
    st.warning(f"Could not load surface performance: {str(e)}")
    surface_df = pd.DataFrame()

if surface_df.empty:
    st.info("No surface attribution data for the selected period.")
else:
    # PSR by surface — horizontal bar, descending
    sdf = surface_df[surface_df['initiated_sessions'] > 0].copy()
    sdf = sdf.sort_values('psr_broad_initiated', ascending=True, na_position='first')

    fig = go.Figure(go.Bar(
        x=(sdf['psr_broad_initiated'].astype(float) * 100),
        y=sdf['origin_surface'],
        orientation='h',
        text=[f"{float(v) * 100:.1f}%" if pd.notna(v) else "—"
              for v in sdf['psr_broad_initiated']],
        textposition='outside',
        marker_color=BRAND_COLORS['info'],
        hovertemplate="<b>%{y}</b><br>PSR Broad: %{x:.1f}%<br>"
                      "Initiated sessions: %{customdata:,}<extra></extra>",
        customdata=sdf['initiated_sessions'],
    ))
    fig.update_layout(
        xaxis_title="PSR Broad (%) — initiated sessions",
        font=dict(family="Inter, system-ui, sans-serif", size=13),
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=40, r=60, t=20, b=40),
        xaxis=dict(showgrid=True, gridcolor=BRAND_COLORS['border']),
        yaxis=dict(showgrid=False),
        showlegend=False,
        height=max(300, 40 * len(sdf) + 100),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Save-rate × volume bubble — which surfaces convert well AND have volume?
    st.markdown("**Save Rate vs Volume** — bubble size = total saves")
    bdf = surface_df[surface_df['views'] > 0].copy()
    if not bdf.empty:
        fig2 = go.Figure(go.Scatter(
            x=bdf['views'].astype(float),
            y=(bdf['save_per_view_rate'].astype(float) * 100),
            mode='markers+text',
            marker=dict(
                size=(bdf['saves'].astype(float).clip(lower=1) ** 0.5) * 3 + 8,
                color=BRAND_COLORS['accent'],
                opacity=0.6,
                line=dict(color=BRAND_COLORS['primary'], width=1),
            ),
            text=bdf['origin_surface'],
            textposition='top center',
            hovertemplate="<b>%{text}</b><br>"
                          "Views: %{x:,}<br>"
                          "Save-per-view: %{y:.1f}%<br>"
                          "Total saves: %{marker.size}<extra></extra>",
        ))
        fig2.update_layout(
            xaxis_title="Views (log)",
            yaxis_title="Save per view (%)",
            xaxis=dict(type='log', showgrid=True, gridcolor=BRAND_COLORS['border']),
            yaxis=dict(showgrid=True, gridcolor=BRAND_COLORS['border']),
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor='white', paper_bgcolor='white',
            margin=dict(l=40, r=20, t=20, b=40),
            showlegend=False,
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Full table
    with st.expander("Surface attribution — full table"):
        table_df = surface_df.copy()
        table_df['SSR'] = (table_df['ssr_initiated'].astype(float) * 100).round(1).astype(str) + '%'
        table_df['SHR'] = (table_df['shr_initiated'].astype(float) * 100).round(1).astype(str) + '%'
        table_df['PSR Broad'] = (table_df['psr_broad_initiated'].astype(float) * 100).round(1).astype(str) + '%'
        table_df['Save/View'] = (table_df['save_per_view_rate'].astype(float) * 100).round(1).astype(str) + '%'
        show_cols = ['origin_surface', 'total_events', 'initiated_sessions',
                     'saves', 'shares', 'SSR', 'SHR', 'PSR Broad', 'Save/View']
        display = table_df[show_cols].rename(columns={
            'origin_surface': 'Surface',
            'total_events': 'Events',
            'initiated_sessions': 'Init Sessions',
            'saves': 'Saves',
            'shares': 'Shares',
        })
        st.dataframe(display, use_container_width=True, hide_index=True)

st.divider()

# ============================================================================
# Section G: Session Diagnostics
# ============================================================================
st.subheader("Session Diagnostics")

diag_df = load_session_diagnostics(start_date=start_date, end_date=end_date, app_version=app_version)

if not diag_df.empty:
    latest = diag_df.iloc[-1]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Zero-Action Sessions",
                   f"{latest.get('pct_zero_action', 0):.1f}%",
                   help="% of sessions with no saves and no shares")
    with col2:
        st.metric("Swipe-but-No-Save",
                   f"{latest.get('pct_swipe_no_save', 0):.1f}%",
                   help="% of sessions with swipes but zero saves")
    with col3:
        st.metric("Genuine Planning",
                   f"{latest.get('pct_genuine_planning', 0):.1f}%",
                   help="% of sessions with at least one prompt, save, or share")
    with col4:
        ttfs = latest.get('median_ttfs')
        ttfs_str = f"{float(ttfs):.0f}s" if ttfs is not None and pd.notna(ttfs) else "N/A"
        st.metric("Median TTFS", ttfs_str,
                   help="Median Time to First Save (seconds)")

    # Trend chart
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=diag_df['session_week'], y=diag_df['pct_zero_action'],
                             name='Zero-Action %', line=dict(color=BRAND_COLORS['error'], width=2)))
    fig.add_trace(go.Scatter(x=diag_df['session_week'], y=diag_df['pct_swipe_no_save'],
                             name='Swipe-No-Save %', line=dict(color=BRAND_COLORS['warning'], width=2)))
    fig.add_trace(go.Scatter(x=diag_df['session_week'], y=diag_df['pct_genuine_planning'],
                             name='Genuine Planning %', line=dict(color=BRAND_COLORS['success'], width=2)))
    fig.update_layout(
        yaxis_title="Percentage (%)",
        font=dict(family="Inter, system-ui, sans-serif", size=13),
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=40, r=20, t=20, b=40),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor=BRAND_COLORS['bg_secondary']),
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No session diagnostics data available for the selected filters.")

add_deck_footer()
