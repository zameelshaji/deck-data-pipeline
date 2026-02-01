"""DECK North Star Metrics Dashboard - PSR and the metrics ladder"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, timedelta
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.data_loader import (
    load_north_star_daily,
    load_north_star_weekly,
    load_psr_ladder_current,
    load_activation_funnel_data,
    load_retention_activated_summary,
    load_active_planners_trend,
    load_available_app_versions,
)

st.set_page_config(
    page_title="North Star Metrics | DECK Analytics",
    page_icon="🎯",
    layout="wide"
)

apply_deck_branding()

st.title("🎯 North Star Metrics")
st.markdown("*Plan Survival Rate (PSR) and the metrics ladder*")

# --- Sidebar Filters ---
with st.sidebar:
    st.header("Filters")

    data_source_label = st.radio(
        "Data Source",
        options=["All Data", "Native Sessions Only", "Inferred Sessions Only"],
        index=0,
        help="Native = from planning_sessions table. Inferred = session inferred from event timestamps."
    )
    data_source_map = {
        "All Data": "all",
        "Native Sessions Only": "native",
        "Inferred Sessions Only": "inferred",
    }
    data_source = data_source_map[data_source_label]

    session_type_label = st.radio(
        "Session Type",
        options=["All Sessions", "Prompt Sessions Only", "Non-Prompt Sessions"],
        index=0,
        help="Prompt sessions = initiated via Dextr AI"
    )
    session_type_map = {
        "All Sessions": "all",
        "Prompt Sessions Only": "prompt",
        "Non-Prompt Sessions": "non_prompt",
    }
    session_type = session_type_map[session_type_label]

    app_version_options = load_available_app_versions()
    app_version_label = st.selectbox(
        "App Version",
        options=["All Versions"] + app_version_options,
        index=0,
        help="Filter by app version"
    )
    app_version = None if app_version_label == "All Versions" else app_version_label

    date_range = st.date_input(
        "Date Range",
        value=(date.today() - timedelta(days=90), date.today()),
        help="Filter metrics to this date range"
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = date.today() - timedelta(days=90), date.today()

# --- Load Data ---
try:
    daily_df = load_north_star_daily(
        data_source=data_source,
        session_type=session_type,
        start_date=str(start_date),
        end_date=str(end_date),
        app_version=app_version,
    )
    weekly_df = load_north_star_weekly(data_source=data_source, session_type=session_type, app_version=app_version)
    ladder_df = load_psr_ladder_current(data_source=data_source, session_type=session_type, app_version=app_version, start_date=str(start_date), end_date=str(end_date))
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("The North Star tables may not be populated yet. Run `dbt run --select +fct_north_star_daily` first.")
    st.stop()

if daily_df.empty:
    st.warning("No data available for the selected filters. The North Star tables may not be populated yet.")
    st.info("Run `dbt run --select +fct_north_star_daily` to populate the data.")
    st.stop()

# --- Section A: Executive Summary ---
st.subheader("📊 Current Performance")

latest = daily_df.iloc[0] if not daily_df.empty else {}

col1, col2, col3, col4 = st.columns(4)

with col1:
    psr_val = latest.get('psr_broad', 0) or 0
    psr_wow = latest.get('psr_broad_wow_change', None)
    delta_str = f"{float(psr_wow)*100:+.1f}pp WoW" if psr_wow is not None and pd.notna(psr_wow) else None
    st.metric(
        label="PSR (Broad)",
        value=f"{float(psr_val)*100:.1f}%",
        delta=delta_str,
        help="% sessions with ≥1 save AND ≥1 share"
    )

with col2:
    ssr_val = latest.get('ssr', 0) or 0
    ssr_wow = latest.get('ssr_wow_change', None)
    delta_str = f"{float(ssr_wow)*100:+.1f}pp WoW" if ssr_wow is not None and pd.notna(ssr_wow) else None
    st.metric(
        label="Session Save Rate",
        value=f"{float(ssr_val)*100:.1f}%",
        delta=delta_str,
        help="% sessions with ≥1 save"
    )

with col3:
    shr_val = latest.get('shr', 0) or 0
    st.metric(
        label="Session Share Rate",
        value=f"{float(shr_val)*100:.1f}%",
        help="% sessions with ≥1 share"
    )

with col4:
    # WAP from weekly data
    wap_val = 0
    wap_delta = None
    if not weekly_df.empty:
        wap_val = int(weekly_df.iloc[0].get('weekly_active_planners', 0) or 0)
        wap_change = weekly_df.iloc[0].get('wap_wow_change', None)
        if wap_change is not None and pd.notna(wap_change):
            wap_delta = f"{int(wap_change):+d} WoW"
    st.metric(
        label="Weekly Active Planners",
        value=f"{wap_val:,}",
        delta=wap_delta,
        help="Unique users with ≥1 save or share this week"
    )

st.divider()

# --- Section B: PSR Ladder ---
st.subheader("🪜 The Value Ladder")
st.caption("Each step is a strict subset of the one above — tracking how sessions progress through the value loop")

if not ladder_df.empty:
    row = ladder_df.iloc[0]
    total = int(row.get('total_sessions', 0))
    genuine = int(row.get('genuine_planning_sessions', 0))
    saves = int(row.get('sessions_with_save', 0))
    psr_b = int(row.get('sessions_with_psr_broad', 0))
    psr_s = int(row.get('sessions_with_psr_strict', 0))
    nvr_count = int(row.get('no_value_sessions', 0))

    if total > 0:
        fig = go.Figure(go.Funnel(
            y=['All Sessions', 'Genuine Planning Attempts', 'Had Save (SSR)', 'Had Save + Share (PSR)', 'Save + Share + Validation'],
            x=[total, genuine, saves, psr_b, psr_s],
            textinfo="value+percent initial+percent previous",
            marker=dict(color=[
                BRAND_COLORS['text_tertiary'],
                '#6B7280',
                BRAND_COLORS['info'],
                BRAND_COLORS['success'],
                '#0D5F56',
            ])
        ))
        fig.update_layout(
            margin=dict(l=20, r=20, t=20, b=20),
            font=dict(family="Inter", size=13),
            plot_bgcolor='white',
            paper_bgcolor='white',
        )
        st.plotly_chart(fig, use_container_width=True)

        genuine_pct = genuine / total * 100 if total > 0 else 0
        nvr_pct = nvr_count / total * 100 if total > 0 else 0
        st.info(f"📊 Genuine Planning Attempts: {genuine_pct:.1f}% of sessions had at least one prompt, save, or share")
        st.warning(f"⚠️ No Value Rate: {nvr_pct:.1f}% of sessions had neither saves nor shares")
    else:
        st.info("No sessions in the selected period.")
else:
    st.info("No ladder data available.")

st.divider()

# --- Section C: Trends Over Time ---
st.subheader("📈 Trends Over Time")

granularity = st.radio("View by", ["Daily (7-day avg)", "Weekly"], horizontal=True)

if granularity == "Daily (7-day avg)" and not daily_df.empty:
    trend_df = daily_df.sort_values('metric_date')
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_df['metric_date'], y=trend_df['ssr_7d_avg'].astype(float) * 100,
        name='SSR (7d avg)', line=dict(color='#E91E8C', width=2)
    ))
    fig.add_trace(go.Scatter(
        x=trend_df['metric_date'], y=trend_df['shr_7d_avg'].astype(float) * 100,
        name='SHR (7d avg)', line=dict(color='#2563EB', width=2)
    ))
    fig.add_trace(go.Scatter(
        x=trend_df['metric_date'], y=trend_df['psr_broad_7d_avg'].astype(float) * 100,
        name='PSR Broad (7d avg)', line=dict(color='#16A34A', width=2.5)
    ))
    fig.update_layout(
        yaxis_title="Rate (%)",
        xaxis_title="Date",
        font=dict(family="Inter", size=12),
        plot_bgcolor='white',
        paper_bgcolor='white',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        margin=dict(l=40, r=20, t=40, b=40),
        yaxis=dict(gridcolor=BRAND_COLORS['border']),
        xaxis=dict(gridcolor=BRAND_COLORS['border']),
    )
    st.plotly_chart(fig, use_container_width=True)

elif granularity == "Weekly" and not weekly_df.empty:
    trend_df = weekly_df.sort_values('metric_week')
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_df['metric_week'], y=trend_df['ssr'].astype(float) * 100,
        name='SSR', line=dict(color='#E91E8C', width=2)
    ))
    fig.add_trace(go.Scatter(
        x=trend_df['metric_week'], y=trend_df['shr'].astype(float) * 100,
        name='SHR', line=dict(color='#2563EB', width=2)
    ))
    fig.add_trace(go.Scatter(
        x=trend_df['metric_week'], y=trend_df['psr_broad'].astype(float) * 100,
        name='PSR Broad', line=dict(color='#16A34A', width=2.5)
    ))
    fig.update_layout(
        yaxis_title="Rate (%)",
        xaxis_title="Week",
        font=dict(family="Inter", size=12),
        plot_bgcolor='white',
        paper_bgcolor='white',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        margin=dict(l=40, r=20, t=40, b=40),
        yaxis=dict(gridcolor=BRAND_COLORS['border']),
        xaxis=dict(gridcolor=BRAND_COLORS['border']),
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No trend data available for the selected view.")

st.divider()

# --- Section D: Activation Funnel ---
st.subheader("🚀 Activation Funnel (7-day window)")

try:
    funnel_df = load_activation_funnel_data(data_source=data_source, session_type=session_type)

    if not funnel_df.empty:
        row = funnel_df.iloc[0]
        total_users = int(row.get('total_users', 0))
        f1 = int(row.get('f1_planning_initiation', 0))
        f2 = int(row.get('f2_activated', 0))
        f2b = int(row.get('f2b_prompted', 0))
        f3 = int(row.get('f3_first_share', 0))
        f4 = int(row.get('f4_first_validation', 0))

        if total_users > 0:
            fig = go.Figure(go.Funnel(
                y=['F1: Planning Initiation', 'F2: Activated (Prompt/Save/Share)',
                   'F2b: Used Dextr Prompt', 'F3: First Share', 'F4: First Validation'],
                x=[f1, f2, f2b, f3, f4],
                textinfo="value+percent initial",
                marker=dict(color=[
                    BRAND_COLORS['info'],
                    BRAND_COLORS['text_tertiary'],
                    BRAND_COLORS['accent'],
                    BRAND_COLORS['success'],
                    '#0D5F56',
                ])
            ))
            fig.update_layout(
                margin=dict(l=20, r=20, t=20, b=20),
                font=dict(family="Inter", size=13),
                plot_bgcolor='white',
                paper_bgcolor='white',
            )
            st.plotly_chart(fig, use_container_width=True)

            # Key metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                arate_7d = f2 / total_users * 100 if total_users > 0 else 0
                st.metric("7-Day Activation Rate", f"{arate_7d:.1f}%")
            with col2:
                activated_30d = int(row.get('activated_30d', 0))
                arate_30d = activated_30d / total_users * 100 if total_users > 0 else 0
                st.metric("30-Day Activation Rate", f"{arate_30d:.1f}%")
            with col3:
                tta = row.get('median_tta', None)
                tta_str = f"{float(tta):.1f} days" if tta is not None and pd.notna(tta) else "N/A"
                st.metric("Median Time to Activation", tta_str)
        else:
            st.info("No activation data available.")
    else:
        st.info("No activation funnel data available.")
except Exception as e:
    st.error(f"Error loading activation funnel: {str(e)}")

st.divider()

# --- Section E: Retention (Activated Users) ---
st.subheader("🔁 Retention (Activated Users)")

try:
    ret_df = load_retention_activated_summary()

    if not ret_df.empty:
        row = ret_df.iloc[0]
        mature_d7 = int(row.get('mature_d7_count', 0))
        retained_d7 = int(row.get('retained_d7', 0))
        mature_d30 = int(row.get('mature_d30_count', 0))
        retained_d30 = int(row.get('retained_d30', 0))
        mature_d60 = int(row.get('mature_d60_count', 0))
        retained_d60 = int(row.get('retained_d60', 0))

        col1, col2, col3 = st.columns(3)

        with col1:
            r7 = retained_d7 / mature_d7 * 100 if mature_d7 > 0 else 0
            st.metric("D7 Retention", f"{r7:.1f}%",
                       help=f"% of {mature_d7} activated users active within 7 days")
        with col2:
            r30 = retained_d30 / mature_d30 * 100 if mature_d30 > 0 else 0
            st.metric("D30 Retention", f"{r30:.1f}%",
                       help=f"% of {mature_d30} activated users active within 30 days")
        with col3:
            r60 = retained_d60 / mature_d60 * 100 if mature_d60 > 0 else 0
            st.metric("D60 Retention", f"{r60:.1f}%",
                       help=f"% of {mature_d60} activated users active within 60 days")

        # Stickiness from active planners
        try:
            ap_df = load_active_planners_trend()
            monthly_ap = ap_df[ap_df['period_type'] == 'month'] if not ap_df.empty else pd.DataFrame()
            if not monthly_ap.empty:
                latest_stickiness = monthly_ap.iloc[0].get('planner_stickiness', None)
                if latest_stickiness is not None and pd.notna(latest_stickiness):
                    st.metric("Planner Stickiness (WAP/MAP)", f"{float(latest_stickiness)*100:.1f}%",
                              help="Average Weekly Active Planners / Monthly Active Planners")
        except Exception:
            pass
    else:
        st.info("No retention data available yet.")
except Exception as e:
    st.error(f"Error loading retention data: {str(e)}")

add_deck_footer()
