"""Shared sidebar filters for DECK Analytics Dashboard."""

import streamlit as st
from datetime import date, timedelta
from utils.data_loader import load_app_versions_with_dates


def render_sidebar_filters(
    show_date_range=True,
    show_app_version=True,
    show_activation_cohort=False,
    show_data_source=False,
    show_session_type=False,
    default_days=90,
):
    """Render sidebar filters and return dict of selected values.

    Returns:
        dict with keys: start_date, end_date, app_version, activation_week,
                        data_source, session_type
    """
    filters = {}

    with st.sidebar:
        st.header("Filters")

        if show_date_range:
            date_range = st.date_input(
                "Date Range",
                value=(date.today() - timedelta(days=default_days), date.today()),
                help="Filter metrics to this date range"
            )
            if isinstance(date_range, tuple) and len(date_range) == 2:
                filters['start_date'] = str(date_range[0])
                filters['end_date'] = str(date_range[1])
            else:
                filters['start_date'] = str(date.today() - timedelta(days=default_days))
                filters['end_date'] = str(date.today())
        else:
            filters['start_date'] = None
            filters['end_date'] = None

        if show_app_version:
            app_version_map = load_app_versions_with_dates()
            app_version_options = list(app_version_map.keys())
            app_version_label = st.selectbox(
                "App Version",
                options=["All Versions"] + app_version_options,
                index=0,
                help="Filter by app version (release date shown)"
            )
            filters['app_version'] = None if app_version_label == "All Versions" else app_version_map.get(app_version_label)
        else:
            filters['app_version'] = None

        if show_activation_cohort:
            cohort_weeks = _get_activation_cohort_weeks()
            cohort_label = st.selectbox(
                "Activation Cohort Week",
                options=["All Cohorts"] + cohort_weeks,
                index=0,
                help="Filter by the week users first activated (prompted, saved, or shared)"
            )
            filters['activation_week'] = None if cohort_label == "All Cohorts" else cohort_label
        else:
            filters['activation_week'] = None

        if show_data_source:
            ds_label = st.radio(
                "Data Source",
                options=["All Data", "Native Sessions Only", "Inferred Sessions Only"],
                index=0,
                help="Native = from planning_sessions table. Inferred = gap detection from events."
            )
            ds_map = {"All Data": "all", "Native Sessions Only": "native", "Inferred Sessions Only": "inferred"}
            filters['data_source'] = ds_map[ds_label]
        else:
            filters['data_source'] = 'all'

        if show_session_type:
            st_label = st.radio(
                "Session Type",
                options=["All Sessions", "Prompt Sessions Only", "Non-Prompt Sessions"],
                index=0,
                help="Prompt sessions = initiated via Dextr AI"
            )
            st_map = {"All Sessions": "all", "Prompt Sessions Only": "prompt", "Non-Prompt Sessions": "non_prompt"}
            filters['session_type'] = st_map[st_label]
        else:
            filters['session_type'] = 'all'

    return filters


@st.cache_data(ttl=300)
def _get_activation_cohort_weeks():
    """Get distinct activation weeks from fct_user_segments."""
    from utils.db_connection import get_database_connection
    from sqlalchemy import text
    import pandas as pd

    engine = get_database_connection()
    query = """
    SELECT DISTINCT activation_week
    FROM analytics_prod_gold.fct_user_segments
    WHERE activation_week IS NOT NULL
    ORDER BY activation_week DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return [str(w) for w in df['activation_week'].tolist()]
    except Exception:
        return []
