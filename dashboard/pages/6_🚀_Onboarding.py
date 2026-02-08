"""DECK Onboarding Analytics Dashboard - Tracking the user onboarding funnel"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date, timedelta
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.data_loader import (
    load_onboarding_funnel_summary,
    load_onboarding_funnel_current,
    load_onboarding_user_journeys,
    load_onboarding_feature_distribution,
    load_onboarding_time_distribution,
    load_onboarding_completion_rate_prior_7d,
    load_app_versions_with_dates,
)

st.set_page_config(
    page_title="Onboarding Analytics | DECK Analytics",
    page_icon="🚀",
    layout="wide"
)

apply_deck_branding()

st.title("🚀 Onboarding Analytics")
st.markdown("*Tracking the user onboarding funnel and permission grants*")

# --- Sidebar Filters ---
with st.sidebar:
    st.header("Filters")

    app_version_map = load_app_versions_with_dates()
    app_version_options = list(app_version_map.keys())
    app_version_label = st.selectbox(
        "App Version",
        options=["All Versions"] + app_version_options,
        index=0,
        help="Filter by app version (release date shown in brackets)"
    )
    app_version = None if app_version_label == "All Versions" else app_version_map.get(app_version_label)

    date_range = st.date_input(
        "Date Range",
        value=(date.today() - timedelta(days=90), date.today()),
        help="Filter metrics by onboarding date range"
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = date.today() - timedelta(days=90), date.today()

# --- Load Data ---
try:
    current_df = load_onboarding_funnel_current(
        start_date=str(start_date),
        end_date=str(end_date),
        app_version=app_version
    )
    summary_df = load_onboarding_funnel_summary()
    prior_rates_df = load_onboarding_completion_rate_prior_7d(app_version=app_version)
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    with st.expander("Troubleshooting"):
        st.markdown("""
        The onboarding tables may not be populated yet. Run the following dbt commands:
        ```bash
        dbt run --select +onboarding_daily_summary
        ```
        This will build:
        - `stg_onboarding_events` (Silver)
        - `fct_onboarding_funnel` (Gold)
        - `onboarding_daily_summary` (Gold)
        """)
    st.stop()

if current_df.empty:
    st.warning("No onboarding data available for the selected filters.")
    with st.expander("Troubleshooting"):
        st.markdown("""
        Run `dbt run --select +onboarding_daily_summary` to populate the data.
        """)
    st.stop()

# --- Section A: Headline KPIs ---
st.subheader("📊 Headline KPIs")
st.caption(f"Aggregated for {start_date.strftime('%b %d, %Y')} – {end_date.strftime('%b %d, %Y')}"
           + (f" | App Version: {app_version}" if app_version else ""))

row = current_df.iloc[0]
total_started = int(row.get('total_users_started', 0))
total_completed = int(row.get('total_completed', 0))
completion_rate = float(row.get('completion_rate', 0))
median_time_seconds = row.get('median_time_to_complete_seconds', None)
location_grant_rate = float(row.get('location_grant_rate', 0))
notification_grant_rate = float(row.get('notification_grant_rate', 0))
contacts_grant_rate = float(row.get('contacts_grant_rate', 0))

# Calculate delta for completion rate
completion_delta = None
if not prior_rates_df.empty:
    prior_row = prior_rates_df.iloc[0]
    recent_rate = float(prior_row.get('recent_completion_rate', 0))
    prior_rate = float(prior_row.get('prior_completion_rate', 0))
    if prior_rate > 0:
        completion_delta = round(recent_rate - prior_rate, 1)

# Format median time
if median_time_seconds is not None and pd.notna(median_time_seconds):
    minutes = int(median_time_seconds // 60)
    seconds = int(median_time_seconds % 60)
    time_str = f"{minutes}m {seconds}s"
else:
    time_str = "N/A"

# Combined permission grant rate
avg_permission_rate = round((location_grant_rate + notification_grant_rate + contacts_grant_rate) / 3, 1)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Users Started Onboarding",
        value=f"{total_started:,}",
        help="Total number of users who viewed the welcome screen"
    )

with col2:
    st.metric(
        label="Completion Rate",
        value=f"{completion_rate:.1f}%",
        delta=f"{completion_delta:+.1f}% vs prior 7d" if completion_delta is not None else None,
        help="Percentage of users who completed the full onboarding flow"
    )

with col3:
    st.metric(
        label="Median Time to Complete",
        value=time_str,
        help="Median time from welcome screen to onboarding completion"
    )

with col4:
    st.metric(
        label="Avg Permission Grant Rate",
        value=f"{avg_permission_rate:.1f}%",
        help="Average grant rate across location, notifications, and contacts permissions"
    )

st.divider()

# --- Section B: Onboarding Funnel ---
st.subheader("📈 Onboarding Funnel")

reached_welcome = int(row.get('reached_welcome', 0))
reached_referral = int(row.get('reached_referral', 0))
reached_location = int(row.get('reached_location', 0))
reached_notification = int(row.get('reached_notification', 0))
reached_contacts = int(row.get('reached_contacts', 0))
reached_feature_router = int(row.get('reached_feature_router', 0))
reached_completion = int(row.get('reached_completion', 0))

funnel_steps = ['Welcome', 'Referral', 'Location', 'Notifications', 'Contacts', 'Feature Router', 'Completed']
funnel_values = [reached_welcome, reached_referral, reached_location, reached_notification,
                 reached_contacts, reached_feature_router, total_completed]

if reached_welcome > 0:
    fig = go.Figure(go.Funnel(
        y=funnel_steps,
        x=funnel_values,
        textinfo="value+percent initial+percent previous",
        marker=dict(color=[
            BRAND_COLORS['text_tertiary'],
            '#6B7280',
            BRAND_COLORS['info'],
            '#8B5CF6',
            '#F59E0B',
            BRAND_COLORS['warning'],
            BRAND_COLORS['success'],
        ])
    ))
    fig.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        font=dict(family="Inter", size=13),
        plot_bgcolor='white',
        paper_bgcolor='white',
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No funnel data available yet.")

st.divider()

# --- Section C: Permission Grant Rates ---
st.subheader("🔐 Permission Grant Rates")

permission_data = {
    'Permission': ['Location', 'Notifications', 'Contacts'],
    'Granted (%)': [location_grant_rate, notification_grant_rate, contacts_grant_rate],
    'Skipped (%)': [100 - location_grant_rate, 100 - notification_grant_rate, 100 - contacts_grant_rate]
}
perm_df = pd.DataFrame(permission_data)

fig = go.Figure()
fig.add_trace(go.Bar(
    name='Enabled',
    x=perm_df['Permission'],
    y=perm_df['Granted (%)'],
    marker_color=BRAND_COLORS['success'],
    text=[f"{v:.1f}%" for v in perm_df['Granted (%)']],
    textposition='inside'
))
fig.add_trace(go.Bar(
    name='Skipped',
    x=perm_df['Permission'],
    y=perm_df['Skipped (%)'],
    marker_color=BRAND_COLORS['text_tertiary'],
    text=[f"{v:.1f}%" for v in perm_df['Skipped (%)']],
    textposition='inside'
))
fig.update_layout(
    barmode='group',
    yaxis_title="Percentage (%)",
    font=dict(family="Inter", size=12),
    plot_bgcolor='white',
    paper_bgcolor='white',
    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    margin=dict(l=40, r=20, t=40, b=40),
    yaxis=dict(gridcolor=BRAND_COLORS['border'], range=[0, 100]),
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- Section D: Completion Rate Trend ---
st.subheader("📉 Completion Rate Trend")

if not summary_df.empty:
    trend_df = summary_df.sort_values('onboarding_date')

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_df['onboarding_date'],
        y=trend_df['completion_rate'],
        name='Daily Completion Rate',
        line=dict(color=BRAND_COLORS['info'], width=1),
        mode='lines'
    ))
    fig.add_trace(go.Scatter(
        x=trend_df['onboarding_date'],
        y=trend_df['completion_rate_7d_avg'],
        name='7-Day Rolling Avg',
        line=dict(color='#E91E8C', width=2.5),
        mode='lines'
    ))
    fig.update_layout(
        yaxis_title="Completion Rate (%)",
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
else:
    st.info("No trend data available yet.")

st.divider()

# --- Section E: Feature Router Selection ---
st.subheader("🎯 Feature Router Selection")

try:
    feature_df = load_onboarding_feature_distribution()
    if not feature_df.empty:
        fig = px.pie(
            feature_df,
            values='user_count',
            names='feature',
            color_discrete_sequence=['#E91E8C', '#2383E2', '#16A34A', '#F59E0B', '#8B5CF6', '#6B7280'],
            hole=0.4
        )
        fig.update_layout(
            font=dict(family="Inter", size=12),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=20, r=20, t=20, b=20),
        )
        fig.update_traces(textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No feature selection data available yet.")
except Exception as e:
    st.warning(f"Could not load feature distribution: {str(e)}")

st.divider()

# --- Section F: Time to Complete Distribution ---
st.subheader("⏱️ Time to Complete Distribution")

try:
    time_df = load_onboarding_time_distribution(
        start_date=str(start_date),
        end_date=str(end_date),
        app_version=app_version
    )
    if not time_df.empty and len(time_df) > 0:
        median_minutes = time_df['time_to_complete_minutes'].median()

        fig = px.histogram(
            time_df,
            x='time_to_complete_minutes',
            nbins=30,
            color_discrete_sequence=['#E91E8C']
        )
        fig.add_vline(
            x=median_minutes,
            line_dash="dash",
            line_color=BRAND_COLORS['text_primary'],
            annotation_text=f"Median: {median_minutes:.1f} min",
            annotation_position="top"
        )
        fig.update_layout(
            xaxis_title="Time to Complete (minutes)",
            yaxis_title="Number of Users",
            font=dict(family="Inter", size=12),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=40, r=20, t=40, b=40),
            yaxis=dict(gridcolor=BRAND_COLORS['border']),
            xaxis=dict(gridcolor=BRAND_COLORS['border']),
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No time distribution data available yet.")
except Exception as e:
    st.warning(f"Could not load time distribution: {str(e)}")

st.divider()

# --- Section G: User Journey Table ---
with st.expander("🔍 Individual User Journeys"):
    try:
        journeys_df = load_onboarding_user_journeys(limit=100)
        if not journeys_df.empty:
            # Format the dataframe for display
            display_df = journeys_df.copy()

            # Convert boolean columns to Yes/No for readability
            bool_cols = ['completed_onboarding', 'reached_welcome', 'reached_referral',
                         'reached_location', 'reached_notification', 'reached_contacts',
                         'reached_feature_router', 'reached_completion',
                         'location_granted', 'notification_granted', 'contacts_granted',
                         'referral_submitted']
            for col in bool_cols:
                if col in display_df.columns:
                    display_df[col] = display_df[col].map({True: 'Yes', False: 'No'})

            # Format time to complete
            if 'time_to_complete_seconds' in display_df.columns:
                display_df['time_to_complete'] = display_df['time_to_complete_seconds'].apply(
                    lambda x: f"{int(x // 60)}m {int(x % 60)}s" if pd.notna(x) and x > 0 else "N/A"
                )
                display_df = display_df.drop(columns=['time_to_complete_seconds'])

            st.dataframe(display_df, use_container_width=True, hide_index=True)

            # Download button
            csv = journeys_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="onboarding_user_journeys.csv",
                mime="text/csv"
            )
        else:
            st.info("No user journey data available yet.")
    except Exception as e:
        st.warning(f"Could not load user journeys: {str(e)}")

add_deck_footer()
