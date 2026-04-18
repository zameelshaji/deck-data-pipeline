"""DECK A/B Experiment Dashboard — Control vs Treatment comparison"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.db_connection import get_database_connection
from sqlalchemy import text

try:
    from statsmodels.stats.proportion import proportions_ztest
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

st.set_page_config(
    page_title="A/B Experiment | DECK Analytics",
    page_icon="🧪",
    layout="wide"
)

apply_deck_branding()

# --- Colors for arms ---
CONTROL_COLOR = BRAND_COLORS.get('text_secondary', '#787774')
TREATMENT_COLOR = BRAND_COLORS.get('accent', '#2383E2')

# --- Data Loading ---
@st.cache_data(ttl=300)
def load_experiment_metadata():
    engine = get_database_connection()
    with engine.connect() as conn:
        return pd.read_sql(text(
            "SELECT * FROM analytics_prod.experiments WHERE status = 'active' ORDER BY experiment_id DESC LIMIT 1"
        ), conn)

@st.cache_data(ttl=300)
def load_experiment_results():
    engine = get_database_connection()
    with engine.connect() as conn:
        return pd.read_sql(text(
            "SELECT * FROM analytics_prod_gold.fct_experiment_results ORDER BY metric_date, experiment_arm"
        ), conn)

@st.cache_data(ttl=300)
def load_experiment_timeseries():
    engine = get_database_connection()
    with engine.connect() as conn:
        return pd.read_sql(text(
            "SELECT * FROM analytics_prod_gold.vis_experiment_dashboard ORDER BY metric_date, experiment_arm"
        ), conn)


# --- Page Content ---
st.title("A/B Experiment")

try:
    meta_df = load_experiment_metadata()
    results_df = load_experiment_results()
    ts_df = load_experiment_timeseries()
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Run `dbt seed --select experiments && dbt run --select +vis_experiment_dashboard` first.")
    st.stop()

if meta_df.empty:
    st.warning("No active experiments found. Add a row to `seeds/experiments.csv` with `status=active`.")
    st.stop()

# --- Header ---
exp = meta_df.iloc[0]
days_running = (pd.Timestamp.now().date() - pd.to_datetime(exp['start_date']).date()).days

col1, col2, col3, col4 = st.columns(4)
col1.metric("Experiment", exp['experiment_name'])
col2.metric("Status", exp['status'].upper())
col3.metric("Days Running", days_running)
col4.metric("Assignment", exp['assignment_method'].replace('_', ' '))

with st.expander("Experiment Details"):
    st.markdown(f"**Control:** {exp['control_description']}")
    st.markdown(f"**Treatment:** {exp['treatment_description']}")
    st.markdown(f"**Start date:** {exp['start_date']}")

st.divider()

if results_df.empty:
    st.info("No experiment data yet. Waiting for packs with `experiment_arm` set (requires iOS changes from Ash).")
    st.stop()

# --- Restrict to days where BOTH arms have data ---
# Avoids misleading totals/rates on days where only one arm was live.
arm_dates = results_df.groupby('metric_date')['experiment_arm'].nunique()
shared_dates = set(arm_dates[arm_dates >= 2].index)

if not shared_dates:
    st.info("No overlapping days with both control and treatment data yet.")
    st.stop()

results_df = results_df[results_df['metric_date'].isin(shared_dates)].copy()
ts_df = ts_df[ts_df['metric_date'].isin(shared_dates)].copy() if not ts_df.empty else ts_df

overlap_start = min(shared_dates)
overlap_end = max(shared_dates)
st.caption(
    f"Showing {len(shared_dates)} day(s) where both arms were live "
    f"({overlap_start} → {overlap_end})."
)

# --- Compute Totals ---
totals = results_df.groupby('experiment_arm').agg({
    'packs_created': 'sum',
    'unique_users': 'sum',
    'total_swipes': 'sum',
    'total_likes': 'sum',
    'total_saves': 'sum',
    'avg_like_rate': 'mean',
    'avg_save_rate': 'mean',
    'psr_broad_rate': 'mean',
    'avg_swipe_duration_ms': 'mean',
}).reset_index()

arms = totals['experiment_arm'].tolist()

def get_arm_val(arm, col):
    row = totals[totals['experiment_arm'] == arm]
    if row.empty:
        return 0
    return row.iloc[0][col]

# --- KPI Cards ---
st.subheader("Key Metrics by Arm")

for arm in arms:
    total_packs = get_arm_val(arm, 'packs_created')
    total_users = get_arm_val(arm, 'unique_users')
    st.caption(f"**{arm.upper()}**: {int(total_packs)} packs, {int(total_users)} user-days")

kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

metrics = [
    ('avg_like_rate', 'Like Rate', kpi_col1),
    ('avg_save_rate', 'Save Rate', kpi_col2),
    ('psr_broad_rate', 'PSR Broad', kpi_col3),
    ('avg_swipe_duration_ms', 'Avg Swipe (ms)', kpi_col4),
]

for col_name, label, st_col in metrics:
    vals = {arm: get_arm_val(arm, col_name) for arm in arms}
    for arm in arms:
        v = vals[arm]
        fmt = f"{v:.1f}" if col_name == 'avg_swipe_duration_ms' else f"{v:.1%}" if v and v < 1 else f"{v:.4f}"
        st_col.metric(f"{label} ({arm})", fmt)

# --- Statistical Significance ---
st.divider()
st.subheader("Statistical Significance")

if HAS_SCIPY and 'control' in arms and 'treatment' in arms:
    control_likes = int(get_arm_val('control', 'total_likes'))
    treatment_likes = int(get_arm_val('treatment', 'total_likes'))
    control_swipes = int(get_arm_val('control', 'total_swipes'))
    treatment_swipes = int(get_arm_val('treatment', 'total_swipes'))

    if control_swipes > 0 and treatment_swipes > 0:
        count = [treatment_likes, control_likes]
        nobs = [treatment_swipes, control_swipes]
        z_stat, p_value = proportions_ztest(count, nobs)

        control_rate = control_likes / control_swipes
        treatment_rate = treatment_likes / treatment_swipes
        lift = (treatment_rate - control_rate) / control_rate * 100 if control_rate > 0 else 0

        sig_col1, sig_col2, sig_col3, sig_col4 = st.columns(4)
        sig_col1.metric("Control Like Rate", f"{control_rate:.3%}")
        sig_col2.metric("Treatment Like Rate", f"{treatment_rate:.3%}")
        sig_col3.metric("Lift", f"{lift:+.1f}%")
        sig_col4.metric("p-value", f"{p_value:.4f}", delta="Significant" if p_value < 0.05 else "Not yet")
    else:
        st.info("Not enough swipe data for significance testing.")
elif not HAS_SCIPY:
    st.warning("Install `statsmodels` for statistical significance testing: `pip install statsmodels`")
else:
    st.info("Waiting for both control and treatment data to compute significance.")

# --- Time Series ---
st.divider()
st.subheader("Metrics Over Time")

if not ts_df.empty:
    ts_df['metric_date'] = pd.to_datetime(ts_df['metric_date'])

    # Like Rate time series
    fig = make_subplots(rows=2, cols=2, subplot_titles=[
        'Like Rate (7d avg)', 'Save Rate (7d avg)', 'PSR Broad (7d avg)', 'Cumulative Packs'
    ], vertical_spacing=0.12, horizontal_spacing=0.08)

    chart_configs = [
        ('like_rate_7d_avg', 1, 1),
        ('save_rate_7d_avg', 1, 2),
        ('psr_broad_7d_avg', 2, 1),
        ('cumulative_packs', 2, 2),
    ]

    for col_name, row, col in chart_configs:
        for arm in arms:
            arm_data = ts_df[ts_df['experiment_arm'] == arm]
            color = TREATMENT_COLOR if arm == 'treatment' else CONTROL_COLOR
            fig.add_trace(go.Scatter(
                x=arm_data['metric_date'],
                y=arm_data[col_name],
                name=arm,
                line=dict(color=color, width=2),
                showlegend=(row == 1 and col == 1),
            ), row=row, col=col)

    fig.update_layout(height=600, template='plotly_white', legend=dict(orientation='h', y=1.08))
    st.plotly_chart(fig, use_container_width=True)

# --- Detailed Metrics Table ---
st.divider()
st.subheader("Daily Breakdown")

if not results_df.empty:
    display_cols = [
        'metric_date', 'experiment_arm', 'packs_created', 'unique_users',
        'total_swipes', 'total_likes', 'avg_like_rate', 'avg_save_rate',
        'psr_broad_rate', 'total_refined_batches', 'avg_refinement_latency_ms'
    ]
    available_cols = [c for c in display_cols if c in results_df.columns]
    st.dataframe(
        results_df[available_cols].sort_values('metric_date', ascending=False),
        use_container_width=True,
        hide_index=True,
    )

add_deck_footer()
