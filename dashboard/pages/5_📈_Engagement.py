"""DECK Engagement Analytics Dashboard

User engagement trajectory and session depth metrics.
Tracks action frequency, session quality, and cohort-level engagement patterns.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.filters import render_sidebar_filters
from utils.data_loader import (
    load_engagement_trajectory_weekly,
    load_session_depth_weekly,
    load_engagement_quality_weekly,
    load_swipe_to_save_weekly,
    load_engagement_cohort_heatmap,
    load_engagement_frequency_distribution,
)

st.set_page_config(
    page_title="Engagement | DECK Analytics",
    page_icon="\U0001F4C8",
    layout="wide"
)

apply_deck_branding()

st.title("\U0001F4C8 Engagement")
st.markdown("*User engagement trajectory and session depth metrics*")

# --- Chart styling constants ---
CHART_FONT = dict(family="Inter, system-ui, sans-serif", size=13, color='#37352F')
CHART_COLORS = {
    'pink': '#E91E8C',
    'blue': '#2383E2',
    'green': '#0F7B6C',
    'orange': '#D9730D',
}

# --- Sidebar Filters ---
filters = render_sidebar_filters(
    show_date_range=True,
    show_app_version=True,
    show_activation_cohort=True,
)

# =============================================================================
# Helper: create a simple line chart
# =============================================================================

def _make_line_chart(df, x_col, y_col, title, color, y_title=None, y_suffix="", y_tickformat=None):
    """Create a styled line chart using go.Figure."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[x_col],
        y=df[y_col],
        mode='lines+markers',
        line=dict(color=color, width=2),
        marker=dict(size=5, color=color),
        hovertemplate=f"%{{x|%b %d, %Y}}<br>{title}: %{{y:.2f}}{y_suffix}<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text=title, font=dict(size=14, color=BRAND_COLORS['text_primary'])),
        font=CHART_FONT,
        plot_bgcolor='white',
        paper_bgcolor='white',
        margin=dict(l=40, r=20, t=50, b=40),
        xaxis=dict(
            showgrid=False,
            linecolor=BRAND_COLORS['border'],
            tickfont=dict(size=11, color=BRAND_COLORS['text_secondary']),
        ),
        yaxis=dict(
            title=y_title,
            showgrid=True,
            gridcolor=BRAND_COLORS['border'],
            linecolor=BRAND_COLORS['border'],
            tickfont=dict(size=11, color=BRAND_COLORS['text_secondary']),
            tickformat=y_tickformat,
        ),
        height=320,
        showlegend=False,
    )
    return fig


# =============================================================================
# Section A: Action Frequency Over Time
# =============================================================================
st.subheader("Action Frequency Over Time")
st.caption("Average per-user weekly action counts")

try:
    trajectory_df = load_engagement_trajectory_weekly(
        start_date=filters['start_date'],
        end_date=filters['end_date'],
        activation_week=filters['activation_week'],
    )

    if trajectory_df.empty:
        st.info("No data available for the selected filters.")
    else:
        row1_col1, row1_col2 = st.columns(2)
        row2_col1, row2_col2 = st.columns(2)

        with row1_col1:
            fig = _make_line_chart(
                trajectory_df, 'activity_week', 'avg_prompts_per_user',
                'Avg Prompts per User', CHART_COLORS['pink'],
            )
            st.plotly_chart(fig, use_container_width=True)

        with row1_col2:
            fig = _make_line_chart(
                trajectory_df, 'activity_week', 'avg_saves_per_user',
                'Avg Saves per User', CHART_COLORS['blue'],
            )
            st.plotly_chart(fig, use_container_width=True)

        with row2_col1:
            fig = _make_line_chart(
                trajectory_df, 'activity_week', 'avg_sessions_per_user',
                'Avg Sessions per User', CHART_COLORS['green'],
            )
            st.plotly_chart(fig, use_container_width=True)

        with row2_col2:
            fig = _make_line_chart(
                trajectory_df, 'activity_week', 'avg_shares_per_user',
                'Avg Shares per User', CHART_COLORS['orange'],
            )
            st.plotly_chart(fig, use_container_width=True)

        # Summary metrics row
        latest = trajectory_df.iloc[-1]
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric(
                label="Prompts / User (latest week)",
                value=f"{latest['avg_prompts_per_user']:.2f}",
                help="Average number of AI prompts per active user in the most recent week",
            )
        with m2:
            st.metric(
                label="Saves / User (latest week)",
                value=f"{latest['avg_saves_per_user']:.2f}",
                help="Average number of place saves per active user in the most recent week",
            )
        with m3:
            st.metric(
                label="Sessions / User (latest week)",
                value=f"{latest['avg_sessions_per_user']:.2f}",
                help="Average number of sessions per active user in the most recent week",
            )
        with m4:
            st.metric(
                label="Shares / User (latest week)",
                value=f"{latest['avg_shares_per_user']:.2f}",
                help="Average number of shares per active user in the most recent week",
            )

except Exception as e:
    st.error(f"Error loading action frequency data: {str(e)}")

st.divider()

# =============================================================================
# Section B: Session Depth Trends
# =============================================================================
st.subheader("Session Depth Trends")
st.caption("Weekly averages for swipes per session, session duration, and time to first save")

try:
    depth_df = load_session_depth_weekly(
        start_date=filters['start_date'],
        end_date=filters['end_date'],
        activation_week=filters['activation_week'],
    )

    if depth_df.empty:
        st.info("No data available for the selected filters.")
    else:
        col_a, col_b, col_c = st.columns(3)

        with col_a:
            fig = _make_line_chart(
                depth_df, 'activity_week', 'avg_swipes_per_session',
                'Avg Swipes per Session', CHART_COLORS['pink'],
                y_title='Swipes',
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            fig = _make_line_chart(
                depth_df, 'activity_week', 'avg_session_duration',
                'Avg Session Duration (s)', CHART_COLORS['blue'],
                y_title='Seconds',
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_c:
            fig = _make_line_chart(
                depth_df, 'activity_week', 'avg_ttfs',
                'Avg Time to First Save (s)', CHART_COLORS['green'],
                y_title='Seconds',
            )
            st.plotly_chart(fig, use_container_width=True)

        # Summary metrics
        latest_depth = depth_df.iloc[-1]
        dm1, dm2, dm3 = st.columns(3)
        with dm1:
            st.metric(
                label="Swipes / Session (latest)",
                value=f"{latest_depth['avg_swipes_per_session']:.1f}",
                help="Average number of card swipes per session in the most recent week",
            )
        with dm2:
            st.metric(
                label="Session Duration (latest)",
                value=f"{latest_depth['avg_session_duration']:.0f}s",
                help="Average session duration in seconds for the most recent week",
            )
        with dm3:
            st.metric(
                label="Time to First Save (latest)",
                value=f"{latest_depth['avg_ttfs']:.0f}s",
                help="Average seconds from session start to first save in the most recent week",
            )

except Exception as e:
    st.error(f"Error loading session depth data: {str(e)}")

st.divider()

# =============================================================================
# Section C: Engagement Quality - Session Composition
# =============================================================================
st.subheader("Engagement Quality \u2014 Session Composition")
st.caption("100% stacked area showing how sessions break down by action type each week")

try:
    quality_df = load_engagement_quality_weekly(
        start_date=filters['start_date'],
        end_date=filters['end_date'],
        activation_week=filters['activation_week'],
    )

    if quality_df.empty:
        st.info("No data available for the selected filters.")
    else:
        fig = go.Figure()

        traces = [
            ('avg_pct_zero_actions', 'Zero Actions', BRAND_COLORS['text_tertiary']),
            ('avg_pct_swipe_no_save', 'Swipe Only (No Save)', CHART_COLORS['orange']),
            ('avg_pct_with_save', 'With Save', CHART_COLORS['blue']),
            ('avg_pct_with_share', 'With Share', CHART_COLORS['green']),
        ]

        for col, name, color in traces:
            fig.add_trace(go.Scatter(
                x=quality_df['activity_week'],
                y=quality_df[col] * 100,
                mode='lines',
                name=name,
                stackgroup='one',
                groupnorm='percent',
                line=dict(width=0.5, color=color),
                fillcolor=color,
                hovertemplate=f"{name}: %{{y:.1f}}%<extra></extra>",
            ))

        fig.update_layout(
            title=dict(text='Session Composition Over Time', font=dict(size=14, color=BRAND_COLORS['text_primary'])),
            font=CHART_FONT,
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=40, r=20, t=50, b=40),
            xaxis=dict(
                showgrid=False,
                linecolor=BRAND_COLORS['border'],
                tickfont=dict(size=11, color=BRAND_COLORS['text_secondary']),
            ),
            yaxis=dict(
                title='% of Sessions',
                showgrid=True,
                gridcolor=BRAND_COLORS['border'],
                linecolor=BRAND_COLORS['border'],
                tickfont=dict(size=11, color=BRAND_COLORS['text_secondary']),
                ticksuffix='%',
                range=[0, 100],
            ),
            height=400,
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=-0.25,
                xanchor='center',
                x=0.5,
                font=dict(size=11, color=BRAND_COLORS['text_secondary']),
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error loading engagement quality data: {str(e)}")

st.divider()

# =============================================================================
# Section D: Swipe-to-Save Conversion
# =============================================================================
st.subheader("Swipe-to-Save Conversion")
st.caption("Weekly average conversion rate from card swipes to saves")

try:
    sts_df = load_swipe_to_save_weekly(
        start_date=filters['start_date'],
        end_date=filters['end_date'],
        activation_week=filters['activation_week'],
    )

    if sts_df.empty:
        st.info("No data available for the selected filters.")
    else:
        sts_df['rate_pct'] = sts_df['avg_swipe_to_save_rate'] * 100

        fig = _make_line_chart(
            sts_df, 'activity_week', 'rate_pct',
            'Swipe-to-Save Rate (%)', CHART_COLORS['pink'],
            y_title='Conversion Rate',
            y_suffix='%',
            y_tickformat='.1f',
        )
        fig.update_layout(
            yaxis_ticksuffix='%',
            height=380,
        )

        st.plotly_chart(fig, use_container_width=True)

        # Summary metric
        latest_sts = sts_df.iloc[-1]
        st.metric(
            label="Swipe-to-Save Rate (latest week)",
            value=f"{latest_sts['rate_pct']:.1f}%",
            help="Percentage of card swipes that result in a save action, averaged across users for the most recent week",
        )

except Exception as e:
    st.error(f"Error loading swipe-to-save data: {str(e)}")

st.divider()

# =============================================================================
# Section E: Engagement by Cohort Heatmap
# =============================================================================
if filters['activation_week'] is None:
    st.subheader("Engagement by Cohort Heatmap")
    st.caption("Average saves per user by activation cohort and weeks since activation")

    try:
        heatmap_df = load_engagement_cohort_heatmap()

        if heatmap_df.empty:
            st.info("No data available for the selected filters.")
        else:
            # Pivot for heatmap: rows = activation_week, columns = weeks_since_activation
            pivot_df = heatmap_df.pivot_table(
                index='activation_week',
                columns='weeks_since_activation',
                values='avg_saves',
                aggfunc='mean',
            )

            # Sort by activation_week descending for readability
            pivot_df = pivot_df.sort_index(ascending=False)

            fig = go.Figure(data=go.Heatmap(
                z=pivot_df.values,
                x=[f"W{int(c)}" for c in pivot_df.columns],
                y=[str(w) for w in pivot_df.index],
                colorscale=[
                    [0, BRAND_COLORS['bg_secondary']],
                    [0.5, CHART_COLORS['blue']],
                    [1, CHART_COLORS['pink']],
                ],
                hovertemplate=(
                    "Cohort: %{y}<br>"
                    "Week: %{x}<br>"
                    "Avg Saves: %{z:.2f}<extra></extra>"
                ),
                colorbar=dict(
                    title=dict(text='Avg Saves', font=dict(size=12, color=BRAND_COLORS['text_secondary'])),
                    tickfont=dict(size=11, color=BRAND_COLORS['text_secondary']),
                ),
            ))

            fig.update_layout(
                title=dict(
                    text='Avg Saves by Cohort & Weeks Since Activation',
                    font=dict(size=14, color=BRAND_COLORS['text_primary']),
                ),
                font=CHART_FONT,
                plot_bgcolor='white',
                paper_bgcolor='white',
                margin=dict(l=100, r=20, t=50, b=60),
                xaxis=dict(
                    title='Weeks Since Activation',
                    tickfont=dict(size=11, color=BRAND_COLORS['text_secondary']),
                    side='bottom',
                ),
                yaxis=dict(
                    title='Activation Week',
                    tickfont=dict(size=11, color=BRAND_COLORS['text_secondary']),
                    autorange='reversed',
                ),
                height=max(400, len(pivot_df) * 28 + 120),
            )

            st.plotly_chart(fig, use_container_width=True)

            st.metric(
                label="Cohorts Tracked",
                value=f"{len(pivot_df)}",
                help="Number of distinct activation cohort weeks shown in the heatmap",
            )

    except Exception as e:
        st.error(f"Error loading cohort heatmap data: {str(e)}")

# =============================================================================
# Section F: Engagement Frequency Distribution (monthly snapshots)
# =============================================================================
st.divider()
st.subheader("Engagement Frequency Distribution")
st.caption(
    "For each month, the share of MAU split by number of active days in a "
    "reference week. Compare two months to see whether users are engaging "
    "more or less frequently over time."
)

try:
    all_freq = load_engagement_frequency_distribution()
    if all_freq.empty:
        st.info("No engagement frequency data available.")
    else:
        all_freq['snapshot_month'] = pd.to_datetime(all_freq['snapshot_month'])
        month_options = (
            all_freq['snapshot_month']
            .dt.strftime('%Y-%m')
            .drop_duplicates()
            .sort_values(ascending=False)
            .tolist()
        )
        if len(month_options) < 1:
            st.info("Not enough months of data for the overlay yet.")
        else:
            default_recent = month_options[0]
            default_compare = month_options[-1] if len(month_options) > 1 else month_options[0]

            c1, c2 = st.columns(2)
            with c1:
                month_a = st.selectbox(
                    "Month A (recent)", month_options, index=0, key='freq_month_a'
                )
            with c2:
                default_idx_b = min(len(month_options) - 1, 12)
                month_b = st.selectbox(
                    "Month B (comparison)",
                    month_options,
                    index=default_idx_b,
                    key='freq_month_b',
                )

            selected = all_freq[
                all_freq['snapshot_month'].dt.strftime('%Y-%m').isin([month_a, month_b])
            ].copy()
            selected['month_label'] = selected['snapshot_month'].dt.strftime('%b %Y')
            selected['pct_of_users_display'] = selected['pct_of_users'] * 100

            fig_freq = go.Figure()
            colors = [CHART_COLORS['pink'], CHART_COLORS['blue']]
            for idx, label in enumerate([
                pd.to_datetime(month_a + '-01').strftime('%b %Y'),
                pd.to_datetime(month_b + '-01').strftime('%b %Y'),
            ]):
                sub = (
                    selected[selected['month_label'] == label]
                    .sort_values('days_active_bucket')
                )
                if sub.empty:
                    continue
                fig_freq.add_trace(
                    go.Bar(
                        x=sub['days_active_bucket'].astype(str),
                        y=sub['pct_of_users_display'],
                        name=label,
                        marker_color=colors[idx % len(colors)],
                        text=[f"{v:.0f}%" for v in sub['pct_of_users_display']],
                        textposition='outside',
                        hovertemplate=(
                            f"{label}"
                            "<br>Days active: %{x}"
                            "<br>% of MAU: %{y:.1f}%"
                            "<br>Users: %{customdata}<extra></extra>"
                        ),
                        customdata=sub['users_in_bucket'],
                    )
                )

            fig_freq.update_layout(
                barmode='group',
                xaxis_title="Days active in reference week",
                yaxis_title="% of monthly active users",
                yaxis=dict(ticksuffix="%", gridcolor=BRAND_COLORS["border"]),
                font=CHART_FONT,
                plot_bgcolor='white',
                paper_bgcolor='white',
                margin=dict(l=40, r=20, t=40, b=40),
                height=420,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_freq, use_container_width=True)
            st.caption(
                "Bucket 0 = users who were active somewhere in the month but "
                "not in the reference week. Buckets 1–7 = distinct days active "
                "in the reference week."
            )
except Exception as e:
    st.error(f"Error loading engagement frequency distribution: {str(e)}")

# --- Footer ---
add_deck_footer()
