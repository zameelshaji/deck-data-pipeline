"""DECK Activation & Retention Analytics Dashboard

Comprehensive view of user activation funnel and cohort-based retention metrics.
Designed for EQT Ventures framework analysis and pre-seed preparation.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from datetime import date, timedelta
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.data_loader import (
    load_user_activation,
    load_retention_by_cohort_week,
    load_signup_activation_funnel,
    load_activation_summary_metrics,
    load_activation_type_distribution,
    load_time_to_activation_distribution,
    load_retention_by_activation_type,
    load_worst_performing_cohorts,
)

st.set_page_config(
    page_title="Activation & Retention | DECK Analytics",
    page_icon="🎯",
    layout="wide"
)

apply_deck_branding()

st.title("🎯 Activation & Retention")
st.markdown("*Weekly cohort analysis for investor-grade retention metrics*")

# --- Load Data ---
try:
    summary_df = load_activation_summary_metrics()
    cohort_df = load_retention_by_cohort_week()
    funnel_df = load_signup_activation_funnel()
    activation_type_df = load_activation_type_distribution()
    time_to_activation_df = load_time_to_activation_distribution()
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("The activation/retention tables may not be populated yet. Run `dbt run --select +fct_user_activation +fct_retention_by_cohort_week +fct_signup_to_activation_funnel` first.")
    st.stop()

if summary_df.empty:
    st.warning("No activation data available. Please ensure the dbt models have been run.")
    st.info("Run: `dbt run --select +fct_user_activation +fct_retention_by_cohort_week +fct_signup_to_activation_funnel`")
    st.stop()

# =============================================================================
# Section A: Executive Summary Cards
# =============================================================================
st.subheader("📊 Executive Summary")

summary = summary_df.iloc[0] if not summary_df.empty else {}

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total_activated = int(summary.get('total_activated', 0))
    st.metric(
        label="Total Activated Users",
        value=f"{total_activated:,}",
        help="Users who have saved, shared, or been prompted to save (all time)"
    )

with col2:
    activation_rate = float(summary.get('activation_rate_7d', 0)) * 100
    st.metric(
        label="7-Day Activation Rate",
        value=f"{activation_rate:.1f}%",
        help="% of signups who activate within 7 days"
    )

with col3:
    median_hours = summary.get('median_hours_to_activation', None)
    if median_hours is not None and pd.notna(median_hours):
        hours_val = float(median_hours)
        if hours_val >= 24:
            time_str = f"{hours_val/24:.1f}d"
        else:
            time_str = f"{hours_val:.1f}h"
    else:
        time_str = "N/A"
    st.metric(
        label="Median Time to Activation",
        value=time_str,
        help="Median time from signup to first activation"
    )

with col4:
    d7_rate = float(summary.get('retention_rate_d7', 0)) * 100
    st.metric(
        label="D7 Retention",
        value=f"{d7_rate:.1f}%",
        help="% of activated users who return within 7 days"
    )

with col5:
    d30_rate = float(summary.get('retention_rate_d30', 0)) * 100
    st.metric(
        label="D30 Retention",
        value=f"{d30_rate:.1f}%",
        help="% of activated users who return within 30 days"
    )

# Benchmark comparison
st.caption("*EQT Benchmarks: Best-in-class D7 >40%, D30 >20% | Acceptable early-stage: D7 >20%, D30 >10%*")

st.divider()

# =============================================================================
# Section B: Sign-up → Activation Funnel
# =============================================================================
st.subheader("🚀 Sign-up → Activation Funnel")

if not funnel_df.empty:
    # Aggregate across all weeks for funnel visualization
    funnel_totals = funnel_df.agg({
        'total_signups': 'sum',
        'had_app_open_7d': 'sum',
        'had_planning_initiated_7d': 'sum',
        'had_content_engagement_7d': 'sum',
        'had_activation_7d': 'sum',
    }).to_dict()

    col_funnel, col_breakdown = st.columns([2, 1])

    with col_funnel:
        st.markdown("**Funnel Visualization (All Time)**")

        f0 = int(funnel_totals.get('total_signups', 0))
        f1 = int(funnel_totals.get('had_app_open_7d', 0))
        f2 = int(funnel_totals.get('had_planning_initiated_7d', 0))
        f3 = int(funnel_totals.get('had_content_engagement_7d', 0))
        f4 = int(funnel_totals.get('had_activation_7d', 0))

        if f0 > 0:
            fig_funnel = go.Figure(go.Funnel(
                y=['F0: Signup', 'F1: App Open (7d)', 'F2: Planning Initiated', 'F3: Content Engaged', 'F4: Activated'],
                x=[f0, f1, f2, f3, f4],
                textinfo="value+percent initial+percent previous",
                marker=dict(color=[
                    BRAND_COLORS['text_tertiary'],
                    BRAND_COLORS['info'],
                    '#6B7280',
                    BRAND_COLORS['warning'],
                    BRAND_COLORS['success'],
                ])
            ))
            fig_funnel.update_layout(
                margin=dict(l=20, r=20, t=20, b=20),
                font=dict(family="Inter", size=12),
                plot_bgcolor='white',
                paper_bgcolor='white',
                height=350,
            )
            st.plotly_chart(fig_funnel, use_container_width=True)
        else:
            st.info("No funnel data available.")

    with col_breakdown:
        st.markdown("**Activation Type Breakdown**")

        if not activation_type_df.empty:
            fig_pie = px.pie(
                activation_type_df,
                values='user_count',
                names='activation_type',
                color='activation_type',
                color_discrete_map={
                    'save_prompted': BRAND_COLORS['success'],
                    'saved': BRAND_COLORS['info'],
                    'shared': BRAND_COLORS['warning'],
                    'multiple': BRAND_COLORS['text_tertiary'],
                }
            )
            fig_pie.update_layout(
                margin=dict(l=20, r=20, t=20, b=20),
                font=dict(family="Inter", size=11),
                showlegend=True,
                legend=dict(
                    orientation='h',
                    yanchor='bottom',
                    y=-0.2,
                    xanchor='center',
                    x=0.5
                ),
                height=350,
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No activation type data available.")

    # Activation rate trend by signup week
    st.markdown("**Activation Rate Trend by Signup Week**")

    trend_df = funnel_df.sort_values('signup_week').copy()
    trend_df = trend_df[trend_df['total_signups'] >= 5]  # Filter for meaningful cohorts

    if not trend_df.empty:
        trend_df['activation_rate_pct'] = trend_df['overall_activation_rate_7d'].astype(float) * 100

        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=trend_df['signup_week'],
            y=trend_df['activation_rate_pct'],
            mode='lines+markers',
            name='7-Day Activation Rate',
            line=dict(color=BRAND_COLORS['success'], width=2),
            marker=dict(size=6)
        ))

        # Add benchmark lines
        fig_trend.add_hline(y=20, line_dash="dash", line_color=BRAND_COLORS['warning'],
                           annotation_text="Acceptable (20%)", annotation_position="right")
        fig_trend.add_hline(y=40, line_dash="dash", line_color=BRAND_COLORS['success'],
                           annotation_text="Best-in-class (40%)", annotation_position="right")

        fig_trend.update_layout(
            yaxis_title="Activation Rate (%)",
            xaxis_title="Signup Week",
            font=dict(family="Inter", size=12),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=40, r=100, t=20, b=40),
            yaxis=dict(gridcolor=BRAND_COLORS['border'], range=[0, 100]),
            xaxis=dict(gridcolor=BRAND_COLORS['border']),
            height=300,
            showlegend=False,
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("Not enough data for trend visualization.")

else:
    st.info("No funnel data available.")

st.divider()

# =============================================================================
# Section C: Weekly Cohort Retention Heatmap
# =============================================================================
st.subheader("📅 Weekly Cohort Retention Heatmap")

if not cohort_df.empty:
    # Filter to cohorts with meaningful data (at least some mature users)
    heatmap_df = cohort_df[cohort_df['mature_d7'] >= 3].copy()
    heatmap_df = heatmap_df.sort_values('cohort_week', ascending=False).head(20)  # Last 20 weeks

    if not heatmap_df.empty:
        # Prepare data for heatmap
        heatmap_df['cohort_label'] = heatmap_df.apply(
            lambda r: f"{r['cohort_week'].strftime('%Y-%m-%d')} (n={int(r['cohort_size'])})", axis=1
        )

        # Create retention matrix
        retention_cols = ['retention_rate_d7', 'retention_rate_d30', 'retention_rate_d60', 'retention_rate_d90']
        retention_labels = ['D7', 'D30', 'D60', 'D90']

        z_data = []
        hover_data = []
        for _, row in heatmap_df.iterrows():
            row_vals = []
            row_hover = []
            for col, label in zip(retention_cols, retention_labels):
                val = row.get(col)
                if val is not None and pd.notna(val):
                    row_vals.append(float(val) * 100)
                    mature_col = f"mature_{label.lower()}"
                    retained_col = f"retained_{label.lower()}"
                    row_hover.append(
                        f"{label}: {float(val)*100:.1f}%<br>"
                        f"Retained: {int(row.get(retained_col, 0))} / {int(row.get(mature_col, 0))}"
                    )
                else:
                    row_vals.append(None)
                    row_hover.append(f"{label}: Not mature yet")
            z_data.append(row_vals)
            hover_data.append(row_hover)

        fig_heatmap = go.Figure(data=go.Heatmap(
            z=z_data,
            x=retention_labels,
            y=heatmap_df['cohort_label'].tolist(),
            colorscale=[
                [0, '#FEE2E2'],      # Light red for low retention
                [0.2, '#FEF3C7'],    # Light yellow
                [0.4, '#D1FAE5'],    # Light green
                [0.6, '#6EE7B7'],    # Medium green
                [1.0, BRAND_COLORS['success']],  # Dark green for high retention
            ],
            colorbar=dict(
                title="Retention %",
                ticksuffix="%"
            ),
            hovertext=hover_data,
            hoverinfo='text',
            zmin=0,
            zmax=100,
        ))

        fig_heatmap.update_layout(
            font=dict(family="Inter", size=11),
            margin=dict(l=200, r=20, t=20, b=40),
            height=max(400, len(heatmap_df) * 25 + 100),
            plot_bgcolor='white',
            paper_bgcolor='white',
            xaxis=dict(side='top'),
            yaxis=dict(autorange='reversed'),
        )

        # Add text annotations
        for i, row_vals in enumerate(z_data):
            for j, val in enumerate(row_vals):
                if val is not None:
                    fig_heatmap.add_annotation(
                        x=retention_labels[j],
                        y=heatmap_df['cohort_label'].iloc[i],
                        text=f"{val:.0f}%",
                        showarrow=False,
                        font=dict(size=10, color='white' if val > 50 else BRAND_COLORS['text_primary'])
                    )

        st.plotly_chart(fig_heatmap, use_container_width=True)
    else:
        st.info("Not enough mature cohorts for heatmap visualization.")
else:
    st.info("No cohort retention data available.")

st.divider()

# =============================================================================
# Section D: Retention Curves
# =============================================================================
st.subheader("📈 Retention Curves by Cohort")

if not cohort_df.empty:
    # Get recent cohorts with D30 data
    curves_df = cohort_df[cohort_df['mature_d30'] >= 5].copy()
    curves_df = curves_df.sort_values('cohort_week', ascending=False).head(8)  # Last 8 cohorts

    if not curves_df.empty:
        fig_curves = go.Figure()

        colors = px.colors.qualitative.Set2

        for i, (_, row) in enumerate(curves_df.iterrows()):
            cohort_label = row['cohort_week'].strftime('%Y-%m-%d')

            # Build retention curve points
            days = [0, 7, 30, 60, 90]
            retention = [100]  # Start at 100% on day 0

            for col in ['retention_rate_d7', 'retention_rate_d30', 'retention_rate_d60', 'retention_rate_d90']:
                val = row.get(col)
                if val is not None and pd.notna(val):
                    retention.append(float(val) * 100)
                else:
                    retention.append(None)

            # Only plot points that have data
            valid_days = [d for d, r in zip(days, retention) if r is not None]
            valid_retention = [r for r in retention if r is not None]

            if len(valid_days) > 1:
                fig_curves.add_trace(go.Scatter(
                    x=valid_days,
                    y=valid_retention,
                    mode='lines+markers',
                    name=f'{cohort_label} (n={int(row["cohort_size"])})',
                    line=dict(color=colors[i % len(colors)], width=2),
                    marker=dict(size=6)
                ))

        fig_curves.update_layout(
            yaxis_title="% Retained",
            xaxis_title="Days Since Activation",
            font=dict(family="Inter", size=12),
            plot_bgcolor='white',
            paper_bgcolor='white',
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=-0.3,
                xanchor='center',
                x=0.5
            ),
            margin=dict(l=40, r=20, t=20, b=100),
            yaxis=dict(gridcolor=BRAND_COLORS['border'], range=[0, 100]),
            xaxis=dict(gridcolor=BRAND_COLORS['border'], tickvals=[0, 7, 30, 60, 90]),
            height=400,
        )

        st.plotly_chart(fig_curves, use_container_width=True)

        # Show trend indicator
        if len(curves_df) >= 2:
            recent_d7 = curves_df.iloc[0].get('retention_rate_d7')
            older_d7 = curves_df.iloc[-1].get('retention_rate_d7')

            if recent_d7 is not None and older_d7 is not None:
                delta = (float(recent_d7) - float(older_d7)) * 100
                if delta > 0:
                    st.success(f"D7 Retention improved by {delta:.1f}pp from oldest to most recent cohort")
                elif delta < 0:
                    st.warning(f"D7 Retention declined by {abs(delta):.1f}pp from oldest to most recent cohort")
                else:
                    st.info("D7 Retention stable across cohorts")
    else:
        st.info("Not enough mature cohorts for retention curves.")
else:
    st.info("No cohort retention data available.")

st.divider()

# =============================================================================
# Section E: Diagnostic Drill-downs
# =============================================================================
st.subheader("🔍 Diagnostic Drill-downs")

tab1, tab2, tab3 = st.tabs(["By Activation Type", "Time to Activation", "Worst Cohorts"])

with tab1:
    st.markdown("**Retention by Activation Type**")
    st.caption("Do users who activate via different paths retain differently?")

    try:
        retention_by_type_df = load_retention_by_activation_type()

        if not retention_by_type_df.empty:
            col1, col2 = st.columns(2)

            with col1:
                fig_type_d7 = go.Figure()
                for _, row in retention_by_type_df.iterrows():
                    if row.get('retention_rate_d7') is not None:
                        fig_type_d7.add_trace(go.Bar(
                            x=[row['activation_type']],
                            y=[float(row['retention_rate_d7']) * 100],
                            name=row['activation_type'],
                            text=f"{float(row['retention_rate_d7'])*100:.1f}%",
                            textposition='outside',
                        ))

                fig_type_d7.update_layout(
                    title="D7 Retention by Activation Type",
                    yaxis_title="Retention Rate (%)",
                    font=dict(family="Inter", size=11),
                    plot_bgcolor='white',
                    paper_bgcolor='white',
                    showlegend=False,
                    margin=dict(l=40, r=20, t=40, b=40),
                    yaxis=dict(gridcolor=BRAND_COLORS['border'], range=[0, 100]),
                    height=300,
                )
                st.plotly_chart(fig_type_d7, use_container_width=True)

            with col2:
                fig_type_d30 = go.Figure()
                for _, row in retention_by_type_df.iterrows():
                    if row.get('retention_rate_d30') is not None:
                        fig_type_d30.add_trace(go.Bar(
                            x=[row['activation_type']],
                            y=[float(row['retention_rate_d30']) * 100],
                            name=row['activation_type'],
                            text=f"{float(row['retention_rate_d30'])*100:.1f}%",
                            textposition='outside',
                        ))

                fig_type_d30.update_layout(
                    title="D30 Retention by Activation Type",
                    yaxis_title="Retention Rate (%)",
                    font=dict(family="Inter", size=11),
                    plot_bgcolor='white',
                    paper_bgcolor='white',
                    showlegend=False,
                    margin=dict(l=40, r=20, t=40, b=40),
                    yaxis=dict(gridcolor=BRAND_COLORS['border'], range=[0, 100]),
                    height=300,
                )
                st.plotly_chart(fig_type_d30, use_container_width=True)

            # Show data table
            display_df = retention_by_type_df[['activation_type', 'total_users', 'mature_d7', 'retained_d7', 'retention_rate_d7', 'mature_d30', 'retained_d30', 'retention_rate_d30']].copy()
            display_df['retention_rate_d7'] = display_df['retention_rate_d7'].apply(lambda x: f"{float(x)*100:.1f}%" if pd.notna(x) else "N/A")
            display_df['retention_rate_d30'] = display_df['retention_rate_d30'].apply(lambda x: f"{float(x)*100:.1f}%" if pd.notna(x) else "N/A")
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.info("No retention by activation type data available.")
    except Exception as e:
        st.error(f"Error loading retention by activation type: {str(e)}")

with tab2:
    st.markdown("**Time to Activation Distribution**")
    st.caption("How long does it take users to activate?")

    if not time_to_activation_df.empty:
        fig_hist = go.Figure()

        fig_hist.add_trace(go.Bar(
            x=time_to_activation_df['days_to_activation'],
            y=time_to_activation_df['user_count'],
            marker_color=BRAND_COLORS['info'],
        ))

        fig_hist.update_layout(
            xaxis_title="Days to Activation",
            yaxis_title="Number of Users",
            font=dict(family="Inter", size=12),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=40, r=20, t=20, b=40),
            yaxis=dict(gridcolor=BRAND_COLORS['border']),
            xaxis=dict(gridcolor=BRAND_COLORS['border']),
            height=300,
        )

        st.plotly_chart(fig_hist, use_container_width=True)

        # Summary stats
        total_users = time_to_activation_df['user_count'].sum()
        same_day = time_to_activation_df[time_to_activation_df['days_to_activation'] == 0]['user_count'].sum()
        within_7d = time_to_activation_df[time_to_activation_df['days_to_activation'] <= 7]['user_count'].sum()

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Same Day Activation", f"{same_day:,}", f"{same_day/total_users*100:.1f}% of activated")
        with col2:
            st.metric("Within 7 Days", f"{within_7d:,}", f"{within_7d/total_users*100:.1f}% of activated")
        with col3:
            st.metric("Total Activated (30d window)", f"{total_users:,}")
    else:
        st.info("No time to activation data available.")

with tab3:
    st.markdown("**Worst Performing Cohorts**")
    st.caption("Cohorts requiring investigation")

    try:
        worst_df = load_worst_performing_cohorts(limit=10)

        if not worst_df.empty:
            display_worst = worst_df.copy()
            display_worst['cohort_week'] = display_worst['cohort_week'].dt.strftime('%Y-%m-%d')
            display_worst['retention_rate_d7'] = display_worst['retention_rate_d7'].apply(
                lambda x: f"{float(x)*100:.1f}%" if pd.notna(x) else "N/A"
            )
            display_worst['retention_rate_d30'] = display_worst['retention_rate_d30'].apply(
                lambda x: f"{float(x)*100:.1f}%" if pd.notna(x) else "N/A"
            )

            st.dataframe(
                display_worst[['cohort_week', 'cohort_size', 'mature_d7', 'retained_d7', 'retention_rate_d7', 'retention_rate_d30']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    'cohort_week': st.column_config.TextColumn("Cohort Week"),
                    'cohort_size': st.column_config.NumberColumn("Cohort Size"),
                    'mature_d7': st.column_config.NumberColumn("Mature D7"),
                    'retained_d7': st.column_config.NumberColumn("Retained D7"),
                    'retention_rate_d7': st.column_config.TextColumn("D7 Retention"),
                    'retention_rate_d30': st.column_config.TextColumn("D30 Retention"),
                }
            )

            st.caption("*Cohorts sorted by D7 retention rate (ascending). Minimum 5 mature users required.*")
        else:
            st.info("No cohort data available for analysis.")
    except Exception as e:
        st.error(f"Error loading worst performing cohorts: {str(e)}")

add_deck_footer()
