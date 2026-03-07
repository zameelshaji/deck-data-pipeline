"""DECK Analytics Dashboard - Users & Cohorts

User segmentation, archetypes, cohort quality, and retention analysis.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.filters import render_sidebar_filters
from utils.data_loader import (
    load_archetype_distribution,
    load_top_users,
    load_activation_summary,
    load_cohort_quality_table,
    load_retention_heatmap_data,
    load_churn_analysis,
    load_churn_risk_distribution,
    load_planner_vs_passenger,
)

st.set_page_config(
    page_title="Users & Cohorts | DECK Analytics",
    page_icon="\U0001f465",
    layout="wide",
)

apply_deck_branding()

st.title("Users & Cohorts")
st.caption("User segmentation, archetypes, cohort quality, and retention analysis")

# --- Sidebar Filters ---
filters = render_sidebar_filters(
    show_date_range=True,
    show_activation_cohort=True,
    show_app_version=False,
)

activation_week = filters['activation_week']

# --- Load Data ---
try:
    archetype_df = load_archetype_distribution(activation_week=activation_week)
    activation_df = load_activation_summary(activation_week=activation_week)
    cohort_quality_df = load_cohort_quality_table()
    retention_df = load_retention_heatmap_data()
    churn_df = load_churn_analysis(activation_week=activation_week)
    churn_risk_df = load_churn_risk_distribution(activation_week=activation_week)
    planner_passenger_df = load_planner_vs_passenger(activation_week=activation_week)
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info(
        "The required tables may not be populated yet. "
        "Run `dbt run --select +fct_user_segments +fct_cohort_quality +fct_retention_by_cohort_week` first."
    )
    st.stop()

# =============================================================================
# Section A: User Archetype Distribution
# =============================================================================
st.subheader("User Archetype Distribution")

if archetype_df.empty:
    st.info("No archetype data available. Ensure fct_user_segments has been populated.")
else:
    col_chart, col_metrics = st.columns([1, 1])

    with col_chart:
        archetype_colors = {
            'one_and_done': BRAND_COLORS['error'],
            'browser': BRAND_COLORS['warning'],
            'saver': BRAND_COLORS['info'],
            'planner': BRAND_COLORS['success'],
            'power_planner': BRAND_COLORS['primary'],
        }

        fig_donut = px.pie(
            archetype_df,
            values='user_count',
            names='user_archetype',
            hole=0.5,
            color='user_archetype',
            color_discrete_map=archetype_colors,
        )
        fig_donut.update_layout(
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=20, r=20, t=30, b=20),
            showlegend=True,
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=-0.15,
                xanchor='center',
                x=0.5,
            ),
            height=350,
        )
        fig_donut.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_donut, use_container_width=True)

    with col_metrics:
        # Show 4 archetype metric cards (2x2 grid)
        archetype_order = ['one_and_done', 'browser', 'saver', 'planner']
        archetype_labels = {
            'one_and_done': 'One & Done',
            'browser': 'Browser',
            'saver': 'Saver',
            'planner': 'Planner',
            'power_planner': 'Power Planner',
        }
        archetype_help = {
            'one_and_done': 'Users who used the app only once',
            'browser': 'Users who browse but rarely save',
            'saver': 'Users who save places but have not shared',
            'planner': 'Users who have both saved and shared',
            'power_planner': 'Highly engaged users with many saves and shares',
        }

        # Build lookup
        arch_lookup = {}
        for _, row in archetype_df.iterrows():
            arch_lookup[row['user_archetype']] = {
                'count': int(row['user_count']),
                'pct': float(row['pct']),
            }

        row1_c1, row1_c2 = st.columns(2)
        row2_c1, row2_c2 = st.columns(2)
        metric_cols = [row1_c1, row1_c2, row2_c1, row2_c2]

        for i, arch in enumerate(archetype_order):
            data = arch_lookup.get(arch, {'count': 0, 'pct': 0.0})
            with metric_cols[i]:
                st.metric(
                    label=archetype_labels.get(arch, arch),
                    value=f"{data['count']:,}",
                    delta=f"{data['pct']:.1f}%",
                    help=archetype_help.get(arch, ''),
                )

st.divider()

# =============================================================================
# Section B: Top Users Leaderboard
# =============================================================================
st.subheader("Top Users Leaderboard")

tab_saves, tab_sessions, tab_shares = st.tabs(["Top by Saves", "Top by Sessions", "Top by Shares"])

with tab_saves:
    top_saves_df = load_top_users(sort_by='total_saves', activation_week=activation_week, limit=15)
    if top_saves_df.empty:
        st.info("No user data available.")
    else:
        st.dataframe(top_saves_df, use_container_width=True, hide_index=True)

with tab_sessions:
    top_sessions_df = load_top_users(sort_by='total_sessions', activation_week=activation_week, limit=15)
    if top_sessions_df.empty:
        st.info("No user data available.")
    else:
        st.dataframe(top_sessions_df, use_container_width=True, hide_index=True)

with tab_shares:
    top_shares_df = load_top_users(sort_by='total_shares', activation_week=activation_week, limit=15)
    if top_shares_df.empty:
        st.info("No user data available.")
    else:
        st.dataframe(top_shares_df, use_container_width=True, hide_index=True)

st.divider()

# =============================================================================
# Section C: Activation Analysis
# =============================================================================
st.subheader("Activation Analysis")

if activation_df.empty:
    st.info("No activation data available.")
else:
    act = activation_df.iloc[0]

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            label="Total Activated",
            value=f"{int(act.get('total_activated', 0)):,}",
            help="Users who have prompted, saved, or shared at least once",
        )
    with col2:
        pct_first = act.get('pct_first_session', 0)
        st.metric(
            label="% Activated in First Session",
            value=f"{float(pct_first):.1f}%" if pd.notna(pct_first) else "N/A",
            help="Percentage of activated users who activated during their very first session",
        )
    with col3:
        median_days = act.get('median_days_to_activation', None)
        st.metric(
            label="Median Days to Activation",
            value=f"{float(median_days):.1f}" if pd.notna(median_days) else "N/A",
            help="Median number of days from signup to first activation event",
        )

    # Activation trigger bar chart
    trigger_save = int(act.get('trigger_save', 0))
    trigger_share = int(act.get('trigger_share', 0))
    trigger_prompt = int(act.get('trigger_prompt', 0))

    if trigger_save + trigger_share + trigger_prompt > 0:
        st.markdown("**Activation Trigger Distribution**")

        fig_triggers = go.Figure()
        fig_triggers.add_trace(go.Bar(
            x=['Save', 'Share', 'Prompt'],
            y=[trigger_save, trigger_share, trigger_prompt],
            marker_color=[BRAND_COLORS['info'], BRAND_COLORS['warning'], BRAND_COLORS['success']],
            text=[trigger_save, trigger_share, trigger_prompt],
            textposition='outside',
        ))
        fig_triggers.update_layout(
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=40, r=20, t=20, b=40),
            yaxis=dict(gridcolor=BRAND_COLORS['border'], title="Users"),
            xaxis=dict(title="Activation Trigger"),
            height=300,
            showlegend=False,
        )
        st.plotly_chart(fig_triggers, use_container_width=True)

st.divider()

# =============================================================================
# Section D: Cohort Quality Table
# =============================================================================
st.subheader("Cohort Quality")

if cohort_quality_df.empty:
    st.info("No cohort quality data available. Ensure fct_cohort_quality has been populated.")
else:
    display_cq = cohort_quality_df.copy()

    # Identify retention rate columns and multiply by 100 for percentage display
    retention_cols = [c for c in display_cq.columns if 'retention_rate' in c or 'psr_rate' in c or '_rate' in c]
    for col in retention_cols:
        if display_cq[col].dtype in ['float64', 'float32']:
            # Only multiply if values appear to be 0-1 range (not already percentages)
            max_val = display_cq[col].max()
            if pd.notna(max_val) and max_val <= 1.0:
                display_cq[col] = display_cq[col] * 100

    # Build column config for formatting
    col_config = {}
    for col in display_cq.columns:
        if 'rate' in col or 'pct' in col:
            col_config[col] = st.column_config.NumberColumn(format="%.1f%%")
        elif 'week' in col or 'date' in col:
            col_config[col] = st.column_config.DateColumn()
        elif 'size' in col or 'count' in col:
            col_config[col] = st.column_config.NumberColumn(format="%d")

    st.dataframe(
        display_cq,
        use_container_width=True,
        hide_index=True,
        column_config=col_config,
    )

st.divider()

# =============================================================================
# Section E: Retention Heatmap
# =============================================================================
st.subheader("Retention Heatmap")

if retention_df.empty:
    st.info("No retention heatmap data available. Ensure fct_retention_by_cohort_week has been populated.")
else:
    heatmap_df = retention_df.sort_values('cohort_week', ascending=False).head(20).copy()

    if not heatmap_df.empty:
        heatmap_df['cohort_label'] = heatmap_df.apply(
            lambda r: f"{pd.to_datetime(r['cohort_week']).strftime('%Y-%m-%d')} (n={int(r['cohort_size'])})",
            axis=1,
        )

        retention_labels = ['D7', 'D30']
        retention_cols_map = ['retention_rate_d7', 'retention_rate_d30']

        z_data = []
        text_data = []
        for _, row in heatmap_df.iterrows():
            row_vals = []
            row_text = []
            for col in retention_cols_map:
                val = row.get(col)
                if val is not None and pd.notna(val):
                    pct_val = float(val) * 100
                    row_vals.append(pct_val)
                    row_text.append(f"{pct_val:.0f}%")
                else:
                    row_vals.append(None)
                    row_text.append("")
            z_data.append(row_vals)
            text_data.append(row_text)

        fig_heatmap = go.Figure(data=go.Heatmap(
            z=z_data,
            x=retention_labels,
            y=heatmap_df['cohort_label'].tolist(),
            colorscale=[
                [0, '#FEE2E2'],       # Red (low)
                [0.25, '#FEF3C7'],    # Yellow
                [0.5, '#D1FAE5'],     # Light green
                [0.75, '#6EE7B7'],    # Medium green
                [1.0, BRAND_COLORS['success']],  # Dark green (high)
            ],
            colorbar=dict(title="Retention %", ticksuffix="%"),
            zmin=0,
            zmax=100,
            hovertemplate="Cohort: %{y}<br>Window: %{x}<br>Retention: %{z:.1f}%<extra></extra>",
        ))

        # Add text annotations
        for i, (row_vals, row_text) in enumerate(zip(z_data, text_data)):
            for j, (val, txt) in enumerate(zip(row_vals, row_text)):
                if val is not None:
                    fig_heatmap.add_annotation(
                        x=retention_labels[j],
                        y=heatmap_df['cohort_label'].iloc[i],
                        text=txt,
                        showarrow=False,
                        font=dict(
                            size=11,
                            color='white' if val > 50 else BRAND_COLORS['text_primary'],
                        ),
                    )

        fig_heatmap.update_layout(
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=200, r=20, t=20, b=40),
            height=max(400, len(heatmap_df) * 25 + 100),
            xaxis=dict(side='top'),
            yaxis=dict(autorange='reversed'),
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)
    else:
        st.info("Not enough mature cohorts for heatmap visualization.")

st.divider()

# =============================================================================
# Section F: Churn Analysis
# =============================================================================
st.subheader("Churn Analysis")

if churn_df.empty:
    st.info("No churn data available.")
else:
    churn = churn_df.iloc[0]
    total_churned = int(churn.get('total_churned', 0))
    pct_zero = float(churn.get('pct_zero_saves', 0)) if pd.notna(churn.get('pct_zero_saves')) else 0
    pct_with_saves = 100.0 - pct_zero if total_churned > 0 else 0

    col_c1, col_c2 = st.columns(2)
    with col_c1:
        st.metric(
            label="% Churned with 0 Saves",
            value=f"{pct_zero:.1f}%",
            delta=f"{int(churn.get('churned_zero_saves', 0)):,} users",
            help="Percentage of churned users who never saved a single place",
        )
    with col_c2:
        st.metric(
            label="% Churned with 1+ Saves",
            value=f"{pct_with_saves:.1f}%",
            delta=f"{int(churn.get('churned_with_saves', 0)):,} users",
            help="Percentage of churned users who saved at least one place before churning",
        )

    # Churn risk distribution bar chart
    if not churn_risk_df.empty:
        st.markdown("**Churn Risk Distribution**")

        risk_colors = {
            'high': BRAND_COLORS['error'],
            'medium': BRAND_COLORS['warning'],
            'low': BRAND_COLORS['success'],
        }

        fig_churn = go.Figure()
        fig_churn.add_trace(go.Bar(
            x=churn_risk_df['churn_risk'],
            y=churn_risk_df['user_count'],
            marker_color=[risk_colors.get(r, BRAND_COLORS['text_secondary']) for r in churn_risk_df['churn_risk']],
            text=churn_risk_df['user_count'],
            textposition='outside',
        ))
        fig_churn.update_layout(
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(l=40, r=20, t=20, b=40),
            yaxis=dict(gridcolor=BRAND_COLORS['border'], title="Users"),
            xaxis=dict(title="Churn Risk Level"),
            height=300,
            showlegend=False,
        )
        st.plotly_chart(fig_churn, use_container_width=True)
    else:
        st.info("No churn risk distribution data available.")

st.divider()

# =============================================================================
# Section G: Planner vs Passenger Comparison
# =============================================================================
st.subheader("Planner vs Passenger Comparison")

if planner_passenger_df.empty:
    st.info("No planner/passenger data available.")
else:
    col_left, col_right = st.columns(2)

    for _, row in planner_passenger_df.iterrows():
        segment = row.get('segment', 'Unknown')
        target_col = col_left if segment == 'Planner' else col_right

        with target_col:
            st.markdown(f"### {segment}")
            st.metric(
                label="Users",
                value=f"{int(row.get('user_count', 0)):,}",
                help=f"Total number of {segment.lower()}s",
            )
            st.metric(
                label="Avg Sessions",
                value=f"{float(row.get('avg_sessions', 0)):.1f}",
                help=f"Average sessions per {segment.lower()}",
            )
            st.metric(
                label="Avg Saves",
                value=f"{float(row.get('avg_saves', 0)):.1f}",
                help=f"Average saves per {segment.lower()}",
            )
            st.metric(
                label="Avg Shares",
                value=f"{float(row.get('avg_shares', 0)):.1f}",
                help=f"Average shares per {segment.lower()}",
            )
            retention_d30 = row.get('retention_d30_pct', 0)
            st.metric(
                label="D30 Retention",
                value=f"{float(retention_d30):.1f}%" if pd.notna(retention_d30) else "N/A",
                help=f"Percentage of {segment.lower()}s retained at day 30",
            )
            avg_dur = row.get('avg_duration', 0)
            if pd.notna(avg_dur) and float(avg_dur) > 0:
                dur_secs = int(float(avg_dur))
                if dur_secs >= 3600:
                    dur_str = f"{dur_secs // 3600}h {(dur_secs % 3600) // 60}m"
                elif dur_secs >= 60:
                    dur_str = f"{dur_secs // 60}m {dur_secs % 60}s"
                else:
                    dur_str = f"{dur_secs}s"
            else:
                dur_str = "N/A"
            st.metric(
                label="Avg Session Duration",
                value=dur_str,
                help=f"Average session duration for {segment.lower()}s",
            )

    st.caption(
        "**Planner** = user with total saves > 0 AND total shares > 0. "
        "**Passenger** = activated but not a planner."
    )

# --- Footer ---
add_deck_footer()
