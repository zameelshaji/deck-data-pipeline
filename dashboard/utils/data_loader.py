"""Data loading functions for DECK Analytics Dashboard"""

import pandas as pd
import streamlit as st
from sqlalchemy import text, bindparam
from .db_connection import get_database_connection


@st.cache_data(ttl=300)
def load_executive_summary():
    """Load executive summary metrics"""
    engine = get_database_connection()

    query = """
    SELECT *
    FROM analytics_prod_gold.vis_executive_summary
    ORDER BY report_date DESC
    LIMIT 1
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


@st.cache_data(ttl=300)
def load_headline_metrics(days=None):
    """Load headline metrics, optionally filtered by days"""
    engine = get_database_connection()

    if days:
        query = f"""
        SELECT *
        FROM analytics_prod_gold.vis_headline_metrics
        WHERE metric_date >= current_date - interval '{days} days'
        ORDER BY metric_date DESC
        """
    else:
        query = """
        SELECT *
        FROM analytics_prod_gold.vis_headline_metrics
        ORDER BY metric_date DESC
        """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


@st.cache_data(ttl=300)
def load_daily_active_users(days=90):
    """Load daily active users data"""
    engine = get_database_connection()

    query = f"""
    SELECT *
    FROM analytics_prod_gold.vis_daily_active_users
    WHERE activity_date >= current_date - interval '{days} days'
    ORDER BY activity_date DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


@st.cache_data(ttl=300)
def load_weekly_active_users(weeks=12):
    """Load weekly active users data"""
    engine = get_database_connection()

    query = f"""
    SELECT *
    FROM analytics_prod_gold.vis_weekly_active_users
    WHERE activity_week >= current_date - interval '{weeks} weeks'
    ORDER BY activity_week DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


@st.cache_data(ttl=300)
def load_monthly_active_users(months=12):
    """Load monthly active users data"""
    engine = get_database_connection()

    query = f"""
    SELECT *
    FROM analytics_prod_gold.vis_monthly_active_users
    WHERE activity_month >= current_date - interval '{months} months'
    ORDER BY activity_month DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


@st.cache_data(ttl=300)
def load_user_acquisition_funnel(days=90):
    """Load user acquisition funnel data"""
    engine = get_database_connection()

    query = f"""
    SELECT *
    FROM analytics_prod_gold.vis_user_acquisition_funnel
    WHERE signup_date >= current_date - interval '{days} days'
    ORDER BY signup_date DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


@st.cache_data(ttl=300)
def load_dextr_performance(days=90):
    """Load Dextr AI performance data"""
    engine = get_database_connection()

    query = f"""
    SELECT *
    FROM analytics_prod_gold.vis_dextr_performance
    WHERE query_date >= current_date - interval '{days} days'
    ORDER BY query_date DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


@st.cache_data(ttl=300)
def load_supplier_performance():
    """Load supplier performance data.

    NOTE: vis_supplier_performance is disabled in dbt_project.yml.
    This function will error if called. Kept as stub for reference.
    """
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_content_performance():
    """Load content performance data"""
    engine = get_database_connection()

    query = """
    SELECT *
    FROM analytics_prod_gold.vis_content_performance
    WHERE metric_type = 'individual_cards'
    ORDER BY engagement_score DESC
    LIMIT 100
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


@st.cache_data(ttl=300)
def load_latest_mau():
    """Load the latest Monthly Active Users (MAU) metric"""
    engine = get_database_connection()

    query = """
    SELECT
        monthly_active_users,
        mom_growth_percent
    FROM analytics_prod_gold.vis_monthly_active_users
    ORDER BY activity_month DESC
    LIMIT 1
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading MAU data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_latest_wau():
    """Load the latest Weekly Active Users (WAU) metric with growth data"""
    engine = get_database_connection()

    query = """
    SELECT
        weekly_active_users,
        wow_growth_percent,
        growth_vs_4_weeks_ago_percent
    FROM analytics_prod_gold.vis_weekly_active_users
    ORDER BY activity_week DESC
    LIMIT 1
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading WAU data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_total_multiplayer_sessions():
    """Load total multiplayer sessions to date from gold layer."""
    engine = get_database_connection()

    query = """
    SELECT total_multiplayer_sessions
    FROM analytics_prod_gold.vis_homepage_totals
    LIMIT 1
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading multiplayer sessions data: {str(e)}")
        return pd.DataFrame({'total_multiplayer_sessions': [0]})


@st.cache_data(ttl=300)
def load_total_decks_created():
    """Load total number of decks (boards) created that are not default"""
    engine = get_database_connection()

    query = """
    SELECT COUNT(*) as total_decks_created
    FROM analytics_prod_bronze.src_boards
    WHERE is_default = false OR is_default IS NULL
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading decks created data: {str(e)}")
        return pd.DataFrame({'total_decks_created': [0]})


@st.cache_data(ttl=300)
def load_referral_metrics():
    """Load referral metrics for Home page from gold layer."""
    engine = get_database_connection()

    query = """
    SELECT
        COALESCE(SUM(referrals_used), 0) as total_referrals_made
    FROM analytics_prod_gold.vis_referral_analytics
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading referral metrics: {str(e)}")
        return pd.DataFrame({'total_referrals_given': [0], 'total_referrals_claimed': [0]})

@st.cache_data(ttl=300)
def load_giveaway_metrics():
    """Load giveaway metrics for Home page"""
    engine = get_database_connection()

    query = """
    SELECT
        count(*) as giveaways_claimed
    FROM analytics_prod_bronze.src_giveaway_claims
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading giveaway metrics: {str(e)}")
        return pd.DataFrame({'giveaways_claimed': [0]})


@st.cache_data(ttl=300)
def load_cohort_retention_monthly(months=12):
    """Load monthly user cohort retention data.

    NOTE: vis_user_cohort_retention_monthly (user_cohort_retention_monthly) is
    disabled in dbt_project.yml. This function will return empty. Use
    fct_retention_by_cohort_week for weekly cohort retention instead.
    """
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_north_star_daily(data_source='all', session_type='all', start_date=None, end_date=None, app_version=None):
    """Load daily North Star metrics."""
    engine = get_database_connection()

    conditions = ["1=1"]
    if data_source != 'all':
        conditions.append(f"data_source = '{data_source}'")
    else:
        conditions.append("data_source = 'all'")
    if session_type != 'all':
        conditions.append(f"session_type = '{session_type}'")
    else:
        conditions.append("session_type = 'all'")
    if app_version:
        conditions.append(f"app_version = '{app_version}'")
    else:
        conditions.append("app_version = 'all'")
    if start_date:
        conditions.append(f"metric_date >= '{start_date}'")
    if end_date:
        conditions.append(f"metric_date <= '{end_date}'")

    where_clause = " AND ".join(conditions)
    query = f"""
    SELECT * FROM analytics_prod_gold.fct_north_star_daily
    WHERE {where_clause}
    ORDER BY metric_date DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading North Star daily data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_north_star_weekly(data_source='all', session_type='all', app_version=None):
    """Load weekly North Star metrics."""
    engine = get_database_connection()

    av_filter = f"'{app_version}'" if app_version else "'all'"
    query = f"""
    SELECT * FROM analytics_prod_gold.fct_north_star_weekly
    WHERE data_source = '{data_source}'
      AND session_type = '{session_type}'
      AND app_version = {av_filter}
    ORDER BY metric_week DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading North Star weekly data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_north_star_headline(data_source='all', session_type='all', app_version=None, start_date=None, end_date=None):
    """Load aggregate headline metrics for the selected period and filters."""
    engine = get_database_connection()

    av_filter = f"'{app_version}'" if app_version else "'all'"
    date_conditions = ""
    if start_date and end_date:
        date_conditions = f"AND metric_date >= '{start_date}' AND metric_date <= '{end_date}'"

    query = f"""
    SELECT
        COALESCE(SUM(total_sessions), 0) as total_sessions,
        COALESCE(SUM(sessions_with_save), 0) as sessions_with_save,
        COALESCE(SUM(sessions_with_share), 0) as sessions_with_share,
        COALESCE(SUM(sessions_with_psr_broad), 0) as sessions_with_psr_broad,
        COALESCE(SUM(no_value_sessions), 0) as no_value_sessions,
        COALESCE(SUM(genuine_planning_sessions), 0) as genuine_planning_sessions
    FROM analytics_prod_gold.fct_north_star_daily
    WHERE data_source = '{data_source}'
      AND session_type = '{session_type}'
      AND app_version = {av_filter}
      {date_conditions}
    """

    # Unique active planners — distinct users with save/share/prompt in the filtered period
    # Uses fct_session_outcomes so date, data_source, session_type, and app_version filters apply
    session_conditions = ["1=1"]
    if data_source != 'all':
        session_conditions.append(f"data_source = '{data_source}'")
    if session_type == 'prompt':
        session_conditions.append("is_prompt_session = true")
    elif session_type == 'non_prompt':
        session_conditions.append("is_prompt_session = false")
    if app_version:
        session_conditions.append(f"effective_app_version = '{app_version}'")
    if start_date:
        session_conditions.append(f"session_date >= '{start_date}'")
    if end_date:
        session_conditions.append(f"session_date <= '{end_date}'")

    uap_where = " AND ".join(session_conditions)
    uap_query = f"""
    SELECT COUNT(DISTINCT user_id) as unique_active_planners
    FROM analytics_prod_gold.fct_session_outcomes
    WHERE {uap_where}
      AND (has_save OR has_share OR is_prompt_session)
    """

    try:
        with engine.connect() as conn:
            agg_df = pd.read_sql(text(query), conn)
            uap_df = pd.read_sql(text(uap_query), conn)

        result = agg_df.iloc[0].to_dict()
        result['unique_active_planners'] = int(uap_df.iloc[0]['unique_active_planners'])

        total = result['total_sessions']
        result['ssr'] = result['sessions_with_save'] / total if total > 0 else 0
        result['shr'] = result['sessions_with_share'] / total if total > 0 else 0
        result['psr_broad'] = result['sessions_with_psr_broad'] / total if total > 0 else 0
        result['nvr'] = result['no_value_sessions'] / total if total > 0 else 0

        return result
    except Exception as e:
        st.error(f"Error loading headline metrics: {str(e)}")
        return {}


@st.cache_data(ttl=300)
def load_psr_ladder_current(data_source='all', session_type='all', days=30, app_version=None, start_date=None, end_date=None):
    """Load current PSR ladder metrics for funnel visualization."""
    engine = get_database_connection()

    av_filter = f"'{app_version}'" if app_version else "'all'"
    if start_date and end_date:
        date_filter = f"metric_date >= '{start_date}' AND metric_date <= '{end_date}'"
    else:
        date_filter = f"metric_date >= current_date - interval '{days} days'"
    query = f"""
    SELECT
        COALESCE(SUM(total_sessions), 0) as total_sessions,
        COALESCE(SUM(sessions_with_3plus_swipes), 0) as sessions_with_3plus_swipes,
        COALESCE(SUM(sessions_with_save), 0) as sessions_with_save,
        COALESCE(SUM(sessions_with_share), 0) as sessions_with_share,
        COALESCE(SUM(sessions_with_psr_broad), 0) as sessions_with_psr_broad,
        COALESCE(SUM(sessions_with_psr_strict), 0) as sessions_with_psr_strict,
        COALESCE(SUM(no_value_sessions), 0) as no_value_sessions,
        COALESCE(SUM(genuine_planning_sessions), 0) as genuine_planning_sessions
    FROM analytics_prod_gold.fct_north_star_daily
    WHERE data_source = '{data_source}'
      AND session_type = '{session_type}'
      AND app_version = {av_filter}
      AND {date_filter}
    """

    # Get prompt session count based on session_type filter
    if session_type == 'prompt':
        prompt_query = None  # prompt count = total_sessions
    elif session_type == 'non_prompt':
        prompt_query = None  # prompt count = 0
    else:
        prompt_query = f"""
        SELECT COALESCE(SUM(total_sessions), 0) as sessions_with_prompt
        FROM analytics_prod_gold.fct_north_star_daily
        WHERE data_source = '{data_source}'
          AND session_type = 'prompt'
          AND app_version = {av_filter}
          AND {date_filter}
        """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
            if session_type == 'prompt':
                df['sessions_with_prompt'] = df['total_sessions']
            elif session_type == 'non_prompt':
                df['sessions_with_prompt'] = 0
            else:
                prompt_df = pd.read_sql(text(prompt_query), conn)
                df['sessions_with_prompt'] = int(prompt_df.iloc[0]['sessions_with_prompt']) if not prompt_df.empty else 0
        return df
    except Exception as e:
        st.error(f"Error loading PSR ladder: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_activation_funnel_data(data_source='all', session_type='all', start_date=None, end_date=None):
    """Load activation funnel data, filtered by signup date range and session filters."""
    engine = get_database_connection()

    session_conditions = []
    if data_source != 'all':
        session_conditions.append(f"s.data_source = '{data_source}'")
    if session_type == 'prompt':
        session_conditions.append("s.is_prompt_session = true")
    elif session_type == 'non_prompt':
        session_conditions.append("s.is_prompt_session = false")

    session_where = ""
    if session_conditions:
        session_where = "WHERE " + " AND ".join(session_conditions)

    user_date_filter = ""
    if start_date and end_date:
        user_date_filter = f"WHERE u.signup_date >= '{start_date}' AND u.signup_date <= '{end_date}'"
    elif start_date:
        user_date_filter = f"WHERE u.signup_date >= '{start_date}'"
    elif end_date:
        user_date_filter = f"WHERE u.signup_date <= '{end_date}'"

    # Always use the filtered query approach so we can apply both session and date filters
    query = f"""
    WITH filtered_sessions AS (
        SELECT user_id, session_date, has_save, has_share, has_post_share_interaction, is_prompt_session
        FROM analytics_prod_gold.fct_session_outcomes s
        {session_where}
    ),
    users AS (
        SELECT user_id, signup_date, signup_week
        FROM analytics_prod_gold.fct_activation_funnel u
        {user_date_filter}
    ),
    user_activity AS (
        SELECT
            u.user_id,
            u.signup_date,
            BOOL_OR(s.session_date BETWEEN u.signup_date AND u.signup_date + 7) as has_planning_initiation_7d,
            BOOL_OR(
                (s.has_save OR s.has_share OR s.is_prompt_session)
                AND s.session_date BETWEEN u.signup_date AND u.signup_date + 7
            ) as has_activation_7d,
            BOOL_OR(
                (s.has_save OR s.has_share OR s.is_prompt_session)
                AND s.session_date BETWEEN u.signup_date AND u.signup_date + 30
            ) as has_activation_30d,
            BOOL_OR(
                s.is_prompt_session AND s.session_date BETWEEN u.signup_date AND u.signup_date + 7
            ) as has_prompt_7d,
            BOOL_OR(
                s.has_share AND s.session_date BETWEEN u.signup_date AND u.signup_date + 7
            ) as has_first_share_7d,
            BOOL_OR(
                s.has_post_share_interaction AND s.session_date BETWEEN u.signup_date AND u.signup_date + 7
            ) as has_first_validation_7d,
            MIN(s.session_date) FILTER (WHERE s.has_save OR s.has_share OR s.is_prompt_session) as first_activation_date
        FROM users u
        LEFT JOIN filtered_sessions s ON u.user_id = s.user_id
        GROUP BY u.user_id, u.signup_date
    )
    SELECT
        COUNT(*) as total_users,
        COUNT(*) FILTER (WHERE COALESCE(has_planning_initiation_7d, false)) as f1_planning_initiation,
        COUNT(*) FILTER (WHERE COALESCE(has_activation_7d, false)) as f2_activated,
        COUNT(*) FILTER (WHERE COALESCE(has_prompt_7d, false)) as f2b_prompted,
        COUNT(*) FILTER (WHERE COALESCE(has_first_share_7d, false)) as f3_first_share,
        COUNT(*) FILTER (WHERE COALESCE(has_first_validation_7d, false)) as f4_first_validation,
        COUNT(*) FILTER (WHERE COALESCE(has_activation_30d, false)) as activated_30d,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY first_activation_date - signup_date)
            FILTER (WHERE first_activation_date IS NOT NULL) as median_tta
    FROM user_activity
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading activation funnel: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_signup_to_activation_funnel(start_date=None, end_date=None):
    """Load signup-to-activation funnel from fct_signup_to_activation_funnel."""
    engine = get_database_connection()

    date_filter = "1=1"
    if start_date and end_date:
        date_filter = f"signup_week >= '{start_date}' AND signup_week <= '{end_date}'"
    elif start_date:
        date_filter = f"signup_week >= '{start_date}'"
    elif end_date:
        date_filter = f"signup_week <= '{end_date}'"

    query = f"""
    SELECT
        COALESCE(SUM(total_signups), 0) as total_signups,
        COALESCE(SUM(had_app_open_7d), 0) as had_app_open_7d,
        COALESCE(SUM(had_planning_initiated_7d), 0) as had_planning_initiated_7d,
        COALESCE(SUM(had_content_engagement_7d), 0) as had_content_engagement_7d,
        COALESCE(SUM(had_activation_7d), 0) as had_activation_7d
    FROM analytics_prod_gold.fct_signup_to_activation_funnel
    WHERE {date_filter}
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading signup-to-activation funnel: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_retention_activated_summary():
    """Load retention summary for activated users."""
    engine = get_database_connection()

    query = """
    SELECT
        COUNT(*) FILTER (WHERE is_mature_d7) as mature_d7_count,
        COUNT(*) FILTER (WHERE is_mature_d7 AND had_activity_d7) as retained_d7,
        COUNT(*) FILTER (WHERE is_mature_d30) as mature_d30_count,
        COUNT(*) FILTER (WHERE is_mature_d30 AND had_activity_d30) as retained_d30,
        COUNT(*) FILTER (WHERE is_mature_d60) as mature_d60_count,
        COUNT(*) FILTER (WHERE is_mature_d60 AND had_activity_d60) as retained_d60
    FROM analytics_prod_gold.fct_retention_activated
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading retention data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_active_planners_trend():
    """Load WAP/MAP trend data."""
    engine = get_database_connection()

    query = """
    SELECT * FROM analytics_prod_gold.fct_active_planners
    ORDER BY period_start DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading active planners: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_available_app_versions():
    """Get list of app versions from the release schedule seed, descending."""
    engine = get_database_connection()

    query = """
    SELECT app_version
    FROM analytics_prod_seeds.app_version_releases
    ORDER BY release_date DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df['app_version'].tolist()
    except Exception:
        # Fallback: return known versions if seed table not available
        return ['2.6', '2.5', '2.4', '2.3', '2.2', '2.1', '2.0',
                '1.9', '1.8', '1.7', '1.6', '1.5', '1.4', '1.3', '1.2', '1.1', '1.0']


@st.cache_data(ttl=300)
def load_app_versions_with_dates():
    """Get app versions with release dates for display in filters.

    Returns:
        dict: Mapping of display label (e.g., "2.6 (Jan 27, 2026)") to version (e.g., "2.6")
    """
    engine = get_database_connection()

    query = """
    SELECT app_version, release_date
    FROM analytics_prod_seeds.app_version_releases
    ORDER BY release_date DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)

        # Create display labels with release date
        version_map = {}
        for _, row in df.iterrows():
            version = row['app_version']
            release_date = pd.to_datetime(row['release_date'])
            label = f"{version} ({release_date.strftime('%b %d, %Y')})"
            version_map[label] = version
        return version_map
    except Exception:
        # Fallback: return known versions without dates
        fallback_versions = ['2.6', '2.5', '2.4', '2.3', '2.2', '2.1', '2.0',
                             '1.9', '1.8', '1.7', '1.6', '1.5', '1.4', '1.3', '1.2', '1.1', '1.0']
        return {v: v for v in fallback_versions}


@st.cache_data(ttl=300)
def load_monthly_retention_summary_metrics():
    """Load summary metrics for monthly retention performance.

    NOTE: vis_user_cohort_retention_monthly (user_cohort_retention_monthly) is
    disabled in dbt_project.yml. This function will return empty. Use
    fct_retention_by_cohort_week for weekly cohort retention instead.
    """
    return pd.DataFrame()


# ============================================================================
# Activation & Retention Dashboard Data Loaders
# ============================================================================

@st.cache_data(ttl=300)
def load_user_activation():
    """Load user-level activation data from fct_user_activation."""
    engine = get_database_connection()

    query = """
    SELECT *
    FROM analytics_prod_gold.fct_user_activation
    ORDER BY activation_date DESC NULLS LAST
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading user activation data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_retention_by_cohort_week(start_date=None, end_date=None):
    """Load weekly cohort retention summary from fct_retention_by_cohort_week.

    Args:
        start_date: Optional start date filter (inclusive)
        end_date: Optional end date filter (inclusive)
    """
    engine = get_database_connection()

    where_clauses = []
    if start_date:
        where_clauses.append(f"cohort_week >= '{start_date}'")
    if end_date:
        where_clauses.append(f"cohort_week <= '{end_date}'")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
    SELECT *
    FROM analytics_prod_gold.fct_retention_by_cohort_week
    {where_sql}
    ORDER BY cohort_week DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading retention by cohort week: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_signup_activation_funnel(start_date=None, end_date=None):
    """Load signup to activation funnel data from fct_signup_to_activation_funnel.

    Args:
        start_date: Optional start date filter (inclusive)
        end_date: Optional end date filter (inclusive)
    """
    engine = get_database_connection()

    where_clauses = []
    if start_date:
        where_clauses.append(f"signup_week >= '{start_date}'")
    if end_date:
        where_clauses.append(f"signup_week <= '{end_date}'")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
    SELECT *
    FROM analytics_prod_gold.fct_signup_to_activation_funnel
    {where_sql}
    ORDER BY signup_week DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading signup activation funnel: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_activation_summary_metrics(start_date=None, end_date=None):
    """Load activation and retention summary metrics, optionally filtered by date range."""
    engine = get_database_connection()

    # Build WHERE clauses for activation (by signup_date)
    activation_where_clauses = []
    if start_date:
        activation_where_clauses.append(f"signup_date >= '{start_date}'")
    if end_date:
        activation_where_clauses.append(f"signup_date <= '{end_date}'")
    activation_where = f"WHERE {' AND '.join(activation_where_clauses)}" if activation_where_clauses else ""

    # Build WHERE clauses for retention (by cohort_week)
    retention_where_clauses = []
    if start_date:
        retention_where_clauses.append(f"cohort_week >= '{start_date}'")
    if end_date:
        retention_where_clauses.append(f"cohort_week <= '{end_date}'")
    retention_where = f"WHERE {' AND '.join(retention_where_clauses)}" if retention_where_clauses else ""

    query = f"""
    WITH activation_metrics AS (
        SELECT
            COUNT(*) AS total_users,
            COUNT(*) FILTER (WHERE is_activated) AS total_activated,
            COUNT(*) FILTER (WHERE is_activated AND days_to_activation <= 7) AS activated_within_7d,
            AVG(days_to_activation * 1440.0) FILTER (WHERE is_activated) AS avg_minutes_to_activation,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_activation * 1440.0)
                FILTER (WHERE is_activated) AS median_minutes_to_activation
        FROM analytics_prod_gold.fct_user_activation
        {activation_where}
    ),
    retention_metrics AS (
        SELECT
            SUM(mature_d7) AS total_mature_d7,
            SUM(retained_d7) AS total_retained_d7,
            SUM(mature_d30) AS total_mature_d30,
            SUM(retained_d30) AS total_retained_d30,
            SUM(mature_d60) AS total_mature_d60,
            SUM(retained_d60) AS total_retained_d60,
            SUM(mature_d90) AS total_mature_d90,
            SUM(retained_d90) AS total_retained_d90
        FROM analytics_prod_gold.fct_retention_by_cohort_week
        {retention_where}
    )
    SELECT
        a.total_users,
        a.total_activated,
        a.activated_within_7d,
        CASE WHEN a.total_users > 0
            THEN a.activated_within_7d::numeric / a.total_users
            ELSE 0 END AS activation_rate_7d,
        a.avg_minutes_to_activation,
        a.median_minutes_to_activation,
        r.total_mature_d7,
        r.total_retained_d7,
        CASE WHEN r.total_mature_d7 > 0
            THEN r.total_retained_d7::numeric / r.total_mature_d7
            ELSE 0 END AS retention_rate_d7,
        r.total_mature_d30,
        r.total_retained_d30,
        CASE WHEN r.total_mature_d30 > 0
            THEN r.total_retained_d30::numeric / r.total_mature_d30
            ELSE 0 END AS retention_rate_d30,
        r.total_mature_d60,
        r.total_retained_d60,
        CASE WHEN r.total_mature_d60 > 0
            THEN r.total_retained_d60::numeric / r.total_mature_d60
            ELSE 0 END AS retention_rate_d60,
        r.total_mature_d90,
        r.total_retained_d90,
        CASE WHEN r.total_mature_d90 > 0
            THEN r.total_retained_d90::numeric / r.total_mature_d90
            ELSE 0 END AS retention_rate_d90
    FROM activation_metrics a
    CROSS JOIN retention_metrics r
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading activation summary metrics: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_activation_type_distribution():
    """Load distribution of activation types."""
    engine = get_database_connection()

    query = """
    SELECT
        activation_type,
        COUNT(*) AS user_count
    FROM analytics_prod_gold.fct_user_activation
    WHERE is_activated = true
      AND activation_type IS NOT NULL
    GROUP BY activation_type
    ORDER BY user_count DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading activation type distribution: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_time_to_activation_distribution():
    """Load time to activation distribution for histogram."""
    engine = get_database_connection()

    query = """
    SELECT
        days_to_activation,
        COUNT(*) AS user_count
    FROM analytics_prod_gold.fct_user_activation
    WHERE is_activated = true
      AND days_to_activation IS NOT NULL
      AND days_to_activation <= 30  -- Focus on first 30 days
    GROUP BY days_to_activation
    ORDER BY days_to_activation
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading time to activation distribution: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_retention_by_activation_type():
    """Load retention rates broken down by activation type."""
    engine = get_database_connection()

    query = """
    WITH user_retention AS (
        SELECT
            ua.user_id,
            ua.activation_date,
            ua.activation_type,
            -- Maturity flags
            current_date >= ua.activation_date + 7 AS is_mature_d7,
            current_date >= ua.activation_date + 30 AS is_mature_d30,
            -- Retention flags
            BOOL_OR(s.session_date BETWEEN ua.activation_date + 1 AND ua.activation_date + 7) AS had_activity_d7,
            BOOL_OR(s.session_date BETWEEN ua.activation_date + 1 AND ua.activation_date + 30) AS had_activity_d30
        FROM analytics_prod_gold.fct_user_activation ua
        LEFT JOIN analytics_prod_gold.fct_session_outcomes s
            ON ua.user_id = s.user_id AND (s.has_save OR s.has_share OR s.is_prompt_session)
        WHERE ua.is_activated = true
        GROUP BY ua.user_id, ua.activation_date, ua.activation_type
    )
    SELECT
        activation_type,
        COUNT(*) AS total_users,
        COUNT(*) FILTER (WHERE is_mature_d7) AS mature_d7,
        COUNT(*) FILTER (WHERE is_mature_d7 AND COALESCE(had_activity_d7, false)) AS retained_d7,
        COUNT(*) FILTER (WHERE is_mature_d30) AS mature_d30,
        COUNT(*) FILTER (WHERE is_mature_d30 AND COALESCE(had_activity_d30, false)) AS retained_d30,
        CASE WHEN COUNT(*) FILTER (WHERE is_mature_d7) > 0
            THEN COUNT(*) FILTER (WHERE is_mature_d7 AND COALESCE(had_activity_d7, false))::numeric
                 / COUNT(*) FILTER (WHERE is_mature_d7)
            ELSE NULL END AS retention_rate_d7,
        CASE WHEN COUNT(*) FILTER (WHERE is_mature_d30) > 0
            THEN COUNT(*) FILTER (WHERE is_mature_d30 AND COALESCE(had_activity_d30, false))::numeric
                 / COUNT(*) FILTER (WHERE is_mature_d30)
            ELSE NULL END AS retention_rate_d30
    FROM user_retention
    WHERE activation_type IS NOT NULL
    GROUP BY activation_type
    ORDER BY total_users DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading retention by activation type: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_worst_performing_cohorts(limit=10):
    """Load worst performing cohorts for investigation."""
    engine = get_database_connection()

    query = f"""
    SELECT
        cohort_week,
        cohort_size,
        mature_d7,
        retained_d7,
        retention_rate_d7,
        mature_d30,
        retained_d30,
        retention_rate_d30
    FROM analytics_prod_gold.fct_retention_by_cohort_week
    WHERE mature_d7 >= 5  -- Only cohorts with enough users
      AND retention_rate_d7 IS NOT NULL
    ORDER BY retention_rate_d7 ASC
    LIMIT {limit}
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading worst performing cohorts: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_homepage_totals():
    """Load all homepage metrics from gold_homepage_totals (single row)."""
    engine = get_database_connection()

    query = """
    SELECT *
    FROM analytics_prod_gold.vis_homepage_totals
    LIMIT 1
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading homepage totals: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Onboarding Analytics Data Loaders
# ============================================================================

@st.cache_data(ttl=300)
def load_onboarding_funnel_summary():
    """Load daily onboarding summary from onboarding_daily_summary."""
    engine = get_database_connection()

    query = """
    SELECT *
    FROM analytics_prod_gold.vis_onboarding_daily_summary
    ORDER BY onboarding_date DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading onboarding funnel summary: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_onboarding_funnel_current(start_date=None, end_date=None, app_version=None):
    """Load current overall funnel totals for headline metrics.

    Args:
        start_date: Optional start date filter (inclusive)
        end_date: Optional end date filter (inclusive)
        app_version: Optional app version filter
    """
    engine = get_database_connection()

    conditions = ["1=1"]
    if start_date:
        conditions.append(f"onboarding_date >= '{start_date}'::date")
    if end_date:
        conditions.append(f"onboarding_date <= '{end_date}'::date")
    if app_version:
        conditions.append(f"app_version = '{app_version}'")

    where_clause = " AND ".join(conditions)

    query = f"""
    SELECT
        COUNT(*) as total_users_started,
        COUNT(*) FILTER (WHERE completed_onboarding) as total_completed,
        CASE WHEN COUNT(*) > 0
            THEN ROUND(100.0 * COUNT(*) FILTER (WHERE completed_onboarding) / COUNT(*), 2)
            ELSE 0 END as completion_rate,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_to_complete_seconds)
            FILTER (WHERE completed_onboarding AND time_to_complete_seconds IS NOT NULL) as median_time_to_complete_seconds,
        CASE WHEN COUNT(*) FILTER (WHERE reached_location) > 0
            THEN ROUND(100.0 * COUNT(*) FILTER (WHERE location_granted) / COUNT(*) FILTER (WHERE reached_location), 2)
            ELSE 0 END as location_grant_rate,
        CASE WHEN COUNT(*) FILTER (WHERE reached_notification) > 0
            THEN ROUND(100.0 * COUNT(*) FILTER (WHERE notification_granted) / COUNT(*) FILTER (WHERE reached_notification), 2)
            ELSE 0 END as notification_grant_rate,
        CASE WHEN COUNT(*) FILTER (WHERE reached_contacts) > 0
            THEN ROUND(100.0 * COUNT(*) FILTER (WHERE contacts_granted) / COUNT(*) FILTER (WHERE reached_contacts), 2)
            ELSE 0 END as contacts_grant_rate,
        -- Step counts for funnel (reached = viewed)
        COUNT(*) FILTER (WHERE reached_welcome) as reached_welcome,
        COUNT(*) FILTER (WHERE reached_referral) as reached_referral,
        COUNT(*) FILTER (WHERE reached_location) as reached_location,
        COUNT(*) FILTER (WHERE reached_notification) as reached_notification,
        COUNT(*) FILTER (WHERE reached_contacts) as reached_contacts,
        COUNT(*) FILTER (WHERE reached_feature_router) as reached_feature_router,
        COUNT(*) FILTER (WHERE reached_completion) as reached_completion,
        -- Accepted counts for funnel (took positive action)
        COUNT(*) FILTER (WHERE accepted_welcome) as accepted_welcome,
        COUNT(*) FILTER (WHERE accepted_referral) as accepted_referral,
        COUNT(*) FILTER (WHERE accepted_location) as accepted_location,
        COUNT(*) FILTER (WHERE accepted_notification) as accepted_notification,
        COUNT(*) FILTER (WHERE accepted_contacts) as accepted_contacts,
        COUNT(*) FILTER (WHERE accepted_feature_router) as accepted_feature_router
    FROM analytics_prod_gold.fct_onboarding_funnel
    WHERE {where_clause}
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading onboarding funnel current: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_onboarding_user_journeys(limit=100):
    """Load individual user onboarding journeys."""
    engine = get_database_connection()

    query = f"""
    SELECT
        user_id,
        signup_date,
        onboarding_date,
        completed_onboarding,
        time_to_complete_seconds,
        reached_welcome,
        reached_referral,
        reached_location,
        reached_notification,
        reached_contacts,
        reached_feature_router,
        reached_completion,
        location_granted,
        notification_granted,
        contacts_granted,
        referral_submitted,
        feature_selected
    FROM analytics_prod_gold.fct_onboarding_funnel
    ORDER BY onboarding_date DESC
    LIMIT {limit}
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading onboarding user journeys: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_onboarding_feature_distribution():
    """Load distribution of feature selections during onboarding."""
    engine = get_database_connection()

    query = """
    SELECT
        COALESCE(feature_selected, 'Skipped') as feature,
        COUNT(*) as user_count
    FROM analytics_prod_gold.fct_onboarding_funnel
    WHERE reached_feature_router = true
    GROUP BY COALESCE(feature_selected, 'Skipped')
    ORDER BY user_count DESC
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading feature distribution: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_onboarding_time_distribution(start_date=None, end_date=None, app_version=None):
    """Load time to complete distribution for histogram.

    Args:
        start_date: Optional start date filter (inclusive)
        end_date: Optional end date filter (inclusive)
        app_version: Optional app version filter
    """
    engine = get_database_connection()

    conditions = [
        "completed_onboarding = true",
        "time_to_complete_seconds IS NOT NULL",
        "time_to_complete_seconds > 0",
        "time_to_complete_seconds <= 3600"  # Cap at 1 hour
    ]
    if start_date:
        conditions.append(f"onboarding_date >= '{start_date}'::date")
    if end_date:
        conditions.append(f"onboarding_date <= '{end_date}'::date")
    if app_version:
        conditions.append(f"app_version = '{app_version}'")

    where_clause = " AND ".join(conditions)

    query = f"""
    SELECT
        time_to_complete_seconds / 60.0 as time_to_complete_minutes
    FROM analytics_prod_gold.fct_onboarding_funnel
    WHERE {where_clause}
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading time distribution: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_onboarding_completion_rate_prior_7d(app_version=None):
    """Load completion rate for the prior 7-day period for delta comparison.

    Args:
        app_version: Optional app version filter
    """
    engine = get_database_connection()

    app_version_filter = f"AND app_version = '{app_version}'" if app_version else ""

    query = f"""
    WITH recent_period AS (
        SELECT
            COUNT(*) as users_started,
            COUNT(*) FILTER (WHERE completed_onboarding) as completed
        FROM analytics_prod_gold.fct_onboarding_funnel
        WHERE onboarding_date >= current_date - 7
          AND onboarding_date < current_date
          {app_version_filter}
    ),
    prior_period AS (
        SELECT
            COUNT(*) as users_started,
            COUNT(*) FILTER (WHERE completed_onboarding) as completed
        FROM analytics_prod_gold.fct_onboarding_funnel
        WHERE onboarding_date >= current_date - 14
          AND onboarding_date < current_date - 7
          {app_version_filter}
    )
    SELECT
        CASE WHEN r.users_started > 0
            THEN ROUND(100.0 * r.completed / r.users_started, 2)
            ELSE 0 END as recent_completion_rate,
        CASE WHEN p.users_started > 0
            THEN ROUND(100.0 * p.completed / p.users_started, 2)
            ELSE 0 END as prior_completion_rate
    FROM recent_period r, prior_period p
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading prior period completion rate: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# SQL Filter Helpers
# ============================================================================

def _build_date_clause(column, start_date, end_date):
    """Build date range WHERE clause."""
    parts = []
    if start_date:
        parts.append(f"{column} >= '{start_date}'")
    if end_date:
        parts.append(f"{column} <= '{end_date}'")
    return " AND ".join(parts) if parts else "1=1"


def _build_app_version_clause(column, app_version):
    """Build app version WHERE clause."""
    if app_version:
        return f"{column} = '{app_version}'"
    return "1=1"


def _build_activation_week_clause(column, activation_week):
    """Build activation cohort week WHERE clause."""
    if activation_week:
        return f"{column} = '{activation_week}'"
    return "1=1"


def _build_data_source_clause(data_source):
    """Build data source WHERE clause for fct_north_star_daily."""
    if data_source and data_source != 'all':
        return f"data_source = '{data_source}'"
    return "data_source = 'all'"


def _build_session_type_clause(session_type):
    """Build session type WHERE clause for fct_north_star_daily."""
    if session_type and session_type != 'all':
        return f"session_type = '{session_type}'"
    return "session_type = 'all'"


def _build_ns_app_version_clause(app_version):
    """Build app version clause for fct_north_star_daily (uses 'all' default)."""
    if app_version:
        return f"app_version = '{app_version}'"
    return "app_version = 'all'"


# ============================================================================
# Home Page — New Data Loaders
# ============================================================================

@st.cache_data(ttl=300)
def load_growth_snapshot():
    """Load DAU/WAU/MAU with growth deltas for Home page."""
    engine = get_database_connection()
    query = """
    SELECT 'dau' as metric,
           daily_active_users as value,
           dau_wow_growth_percent as delta
    FROM analytics_prod_gold.vis_daily_active_users
    ORDER BY activity_date DESC LIMIT 1
    """
    query_wau = """
    SELECT 'wau' as metric,
           weekly_active_users as value,
           wow_growth_percent as delta
    FROM analytics_prod_gold.vis_weekly_active_users
    ORDER BY activity_week DESC LIMIT 1
    """
    query_mau = """
    SELECT 'mau' as metric,
           monthly_active_users as value,
           mom_growth_percent as delta
    FROM analytics_prod_gold.vis_monthly_active_users
    ORDER BY activity_month DESC LIMIT 1
    """
    try:
        with engine.connect() as conn:
            dau = pd.read_sql(text(query), conn)
            wau = pd.read_sql(text(query_wau), conn)
            mau = pd.read_sql(text(query_mau), conn)
        return pd.concat([dau, wau, mau], ignore_index=True)
    except Exception as e:
        st.error(f"Error loading growth snapshot: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_dau_sparkline(days=30):
    """Load last N days of DAU for sparkline on Home page."""
    engine = get_database_connection()
    query = f"""
    SELECT activity_date, daily_active_users
    FROM analytics_prod_gold.vis_daily_active_users
    WHERE activity_date >= current_date - {days}
    ORDER BY activity_date
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading DAU sparkline: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_weekly_health_comparison():
    """Load this week vs last week PSR ladder metrics for Home page."""
    engine = get_database_connection()
    query = """
    WITH this_week AS (
        SELECT
            SUM(total_sessions) as total_sessions,
            SUM(sessions_with_save) as saves,
            SUM(sessions_with_share) as shares,
            SUM(sessions_with_psr_broad) as psr,
            SUM(no_value_sessions) as nvr
        FROM analytics_prod_gold.fct_north_star_daily
        WHERE metric_date >= current_date - 7
          AND data_source = 'all' AND session_type = 'all' AND app_version = 'all'
    ),
    last_week AS (
        SELECT
            SUM(total_sessions) as total_sessions,
            SUM(sessions_with_save) as saves,
            SUM(sessions_with_share) as shares,
            SUM(sessions_with_psr_broad) as psr,
            SUM(no_value_sessions) as nvr
        FROM analytics_prod_gold.fct_north_star_daily
        WHERE metric_date >= current_date - 14 AND metric_date < current_date - 7
          AND data_source = 'all' AND session_type = 'all' AND app_version = 'all'
    )
    SELECT
        tw.saves::numeric / NULLIF(tw.total_sessions, 0) as ssr_this_week,
        lw.saves::numeric / NULLIF(lw.total_sessions, 0) as ssr_last_week,
        tw.shares::numeric / NULLIF(tw.total_sessions, 0) as shr_this_week,
        lw.shares::numeric / NULLIF(lw.total_sessions, 0) as shr_last_week,
        tw.psr::numeric / NULLIF(tw.total_sessions, 0) as psr_this_week,
        lw.psr::numeric / NULLIF(lw.total_sessions, 0) as psr_last_week,
        tw.nvr::numeric / NULLIF(tw.total_sessions, 0) as nvr_this_week,
        lw.nvr::numeric / NULLIF(lw.total_sessions, 0) as nvr_last_week
    FROM this_week tw, last_week lw
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading weekly health comparison: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_top_places_this_week():
    """Load top 5 places saved this week for Home page."""
    engine = get_database_connection()
    query = """
    SELECT place_name, category, saves_last_7d,
           ROUND(save_rate * 100, 1) as save_rate_pct
    FROM analytics_prod_gold.fct_place_performance
    WHERE saves_last_7d > 0
    ORDER BY saves_last_7d DESC
    LIMIT 5
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading top places: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Page 1: North Star — Session Diagnostics Loader
# ============================================================================

@st.cache_data(ttl=300)
def load_session_diagnostics(start_date=None, end_date=None, app_version=None):
    """Load session diagnostics aggregated by week."""
    engine = get_database_connection()
    date_clause = _build_date_clause('session_date', start_date, end_date)
    av_clause = _build_app_version_clause('effective_app_version', app_version)

    query = f"""
    SELECT
        session_week,
        COUNT(*) as total_sessions,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_no_value_session) / NULLIF(COUNT(*), 0), 1) as pct_zero_action,
        ROUND(100.0 * COUNT(*) FILTER (WHERE save_count = 0 AND NOT is_no_value_session) / NULLIF(COUNT(*), 0), 1) as pct_swipe_no_save,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_genuine_planning_attempt) / NULLIF(COUNT(*), 0), 1) as pct_genuine_planning,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_to_first_save_seconds)
            FILTER (WHERE time_to_first_save_seconds IS NOT NULL AND time_to_first_save_seconds > 0) as median_ttfs,
        AVG(session_duration_seconds) FILTER (WHERE session_duration_seconds > 0) as avg_duration
    FROM analytics_prod_gold.fct_session_outcomes
    WHERE {date_clause}
      AND {av_clause}
    GROUP BY session_week
    ORDER BY session_week
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading session diagnostics: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Page 2: Engagement — Data Loaders
# ============================================================================

@st.cache_data(ttl=300)
def load_engagement_trajectory_weekly(start_date=None, end_date=None, activation_week=None):
    """Load weekly engagement trajectory aggregated across users."""
    engine = get_database_connection()
    date_clause = _build_date_clause('activity_week', start_date, end_date)
    aw_clause = _build_activation_week_clause('activation_week', activation_week)

    query = f"""
    SELECT
        activity_week,
        COUNT(DISTINCT user_id) as active_users,
        ROUND(AVG(sessions_count), 2) as avg_sessions_per_user,
        ROUND(AVG(prompts_count), 2) as avg_prompts_per_user,
        ROUND(AVG(saves_count), 2) as avg_saves_per_user,
        ROUND(AVG(shares_count), 2) as avg_shares_per_user
    FROM analytics_prod_gold.fct_user_engagement_trajectory
    WHERE {date_clause}
      AND {aw_clause}
    GROUP BY activity_week
    ORDER BY activity_week
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading engagement trajectory: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_session_depth_weekly(start_date=None, end_date=None, activation_week=None):
    """Load weekly session depth metrics."""
    engine = get_database_connection()
    date_clause = _build_date_clause('activity_week', start_date, end_date)
    aw_clause = _build_activation_week_clause('activation_week', activation_week)

    query = f"""
    SELECT
        activity_week,
        ROUND(AVG(swipes_count / NULLIF(sessions_count, 0)), 1) as avg_swipes_per_session,
        ROUND(AVG(avg_session_duration_seconds), 0) as avg_session_duration,
        ROUND(AVG(avg_time_to_first_save_seconds), 0) as avg_ttfs
    FROM analytics_prod_gold.fct_user_engagement_trajectory
    WHERE {date_clause}
      AND {aw_clause}
    GROUP BY activity_week
    ORDER BY activity_week
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading session depth: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_engagement_quality_weekly(start_date=None, end_date=None, activation_week=None):
    """Load session quality composition per week."""
    engine = get_database_connection()
    date_clause = _build_date_clause('activity_week', start_date, end_date)
    aw_clause = _build_activation_week_clause('activation_week', activation_week)

    query = f"""
    SELECT
        activity_week,
        ROUND(AVG(pct_sessions_zero_actions), 3) as avg_pct_zero_actions,
        ROUND(AVG(pct_sessions_swipe_no_save), 3) as avg_pct_swipe_no_save,
        ROUND(AVG(pct_sessions_with_save), 3) as avg_pct_with_save,
        ROUND(AVG(pct_sessions_with_share), 3) as avg_pct_with_share
    FROM analytics_prod_gold.fct_user_engagement_trajectory
    WHERE {date_clause}
      AND {aw_clause}
    GROUP BY activity_week
    ORDER BY activity_week
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading engagement quality: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_swipe_to_save_weekly(start_date=None, end_date=None, activation_week=None):
    """Load swipe-to-save conversion rate by week."""
    engine = get_database_connection()
    date_clause = _build_date_clause('activity_week', start_date, end_date)
    aw_clause = _build_activation_week_clause('activation_week', activation_week)

    query = f"""
    SELECT
        activity_week,
        ROUND(AVG(swipe_to_save_rate), 4) as avg_swipe_to_save_rate
    FROM analytics_prod_gold.fct_user_engagement_trajectory
    WHERE {date_clause}
      AND {aw_clause}
      AND swipe_to_save_rate IS NOT NULL
    GROUP BY activity_week
    ORDER BY activity_week
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading swipe-to-save: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_engagement_cohort_heatmap():
    """Load engagement by cohort heatmap data."""
    engine = get_database_connection()
    query = """
    SELECT
        activation_week,
        weeks_since_activation,
        ROUND(AVG(saves_count), 2) as avg_saves,
        ROUND(AVG(sessions_count), 2) as avg_sessions,
        COUNT(DISTINCT user_id) as user_count
    FROM analytics_prod_gold.fct_user_engagement_trajectory
    WHERE weeks_since_activation <= 12
    GROUP BY activation_week, weeks_since_activation
    ORDER BY activation_week, weeks_since_activation
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading engagement cohort heatmap: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Page 3: Users & Cohorts — Data Loaders
# ============================================================================

@st.cache_data(ttl=300)
def load_archetype_distribution(activation_week=None):
    """Load user archetype distribution."""
    engine = get_database_connection()
    aw_clause = _build_activation_week_clause('activation_week', activation_week)

    query = f"""
    SELECT
        user_archetype,
        COUNT(*) as user_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
    FROM analytics_prod_gold.fct_user_segments
    WHERE is_activated = true
      AND {aw_clause}
    GROUP BY user_archetype
    ORDER BY user_count DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading archetype distribution: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_top_users(sort_by='total_saves', activation_week=None, limit=15):
    """Load top users by a specified metric."""
    engine = get_database_connection()
    aw_clause = _build_activation_week_clause('activation_week', activation_week)
    allowed_sorts = {'total_saves', 'total_sessions', 'total_shares'}
    sort_col = sort_by if sort_by in allowed_sorts else 'total_saves'

    query = f"""
    SELECT COALESCE(username, email) as display_name,
           total_saves, total_sessions, total_shares,
           activation_date, user_archetype, last_activity_date, retained_d30
    FROM analytics_prod_gold.fct_user_segments
    WHERE is_activated = true
      AND {aw_clause}
    ORDER BY {sort_col} DESC
    LIMIT {limit}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading top users: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_activation_summary(activation_week=None):
    """Load activation analysis summary."""
    engine = get_database_connection()
    aw_clause = _build_activation_week_clause('activation_week', activation_week)

    query = f"""
    SELECT
        COUNT(*) as total_activated,
        ROUND(100.0 * COUNT(*) FILTER (WHERE activated_in_first_session) / NULLIF(COUNT(*), 0), 1) as pct_first_session,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_activation)
            FILTER (WHERE days_to_activation IS NOT NULL) as median_days_to_activation,
        COUNT(*) FILTER (WHERE activation_trigger = 'save') as trigger_save,
        COUNT(*) FILTER (WHERE activation_trigger = 'share') as trigger_share,
        COUNT(*) FILTER (WHERE activation_trigger = 'prompt') as trigger_prompt
    FROM analytics_prod_gold.fct_user_segments
    WHERE is_activated = true
      AND {aw_clause}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading activation summary: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_cohort_quality_table():
    """Load full cohort quality table."""
    engine = get_database_connection()
    query = """
    SELECT * FROM analytics_prod_gold.fct_cohort_quality
    ORDER BY activation_week DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading cohort quality: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_retention_heatmap_data():
    """Load retention data for heatmap."""
    engine = get_database_connection()
    query = """
    SELECT
        cohort_week,
        cohort_size,
        retention_rate_d7,
        retention_rate_d30,
        mature_d7,
        mature_d30,
        retained_d7,
        retained_d30
    FROM analytics_prod_gold.fct_retention_by_cohort_week
    WHERE mature_d7 >= 3
    ORDER BY cohort_week DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading retention heatmap: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_churn_analysis(activation_week=None):
    """Load churn analysis metrics."""
    engine = get_database_connection()
    aw_clause = _build_activation_week_clause('activation_week', activation_week)

    query = f"""
    SELECT
        COUNT(*) as total_churned,
        COUNT(*) FILTER (WHERE total_saves = 0) as churned_zero_saves,
        COUNT(*) FILTER (WHERE total_saves > 0) as churned_with_saves,
        ROUND(100.0 * COUNT(*) FILTER (WHERE total_saves = 0) / NULLIF(COUNT(*), 0), 1) as pct_zero_saves
    FROM analytics_prod_gold.fct_user_segments
    WHERE is_churned = true
      AND {aw_clause}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading churn analysis: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_churn_risk_distribution(activation_week=None):
    """Load churn risk distribution."""
    engine = get_database_connection()
    aw_clause = _build_activation_week_clause('activation_week', activation_week)

    query = f"""
    SELECT churn_risk, COUNT(*) as user_count
    FROM analytics_prod_gold.fct_user_segments
    WHERE is_activated = true AND {aw_clause}
    GROUP BY churn_risk
    ORDER BY CASE churn_risk WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading churn risk distribution: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_planner_vs_passenger(activation_week=None):
    """Load planner vs passenger comparison."""
    engine = get_database_connection()
    aw_clause = _build_activation_week_clause('activation_week', activation_week)

    query = f"""
    SELECT
        CASE WHEN is_planner THEN 'Planner' ELSE 'Passenger' END as segment,
        COUNT(*) as user_count,
        ROUND(AVG(total_sessions), 1) as avg_sessions,
        ROUND(AVG(total_saves), 1) as avg_saves,
        ROUND(AVG(total_shares), 1) as avg_shares,
        ROUND(100.0 * COUNT(*) FILTER (WHERE retained_d30) / NULLIF(COUNT(*), 0), 1) as retention_d30_pct,
        ROUND(AVG(avg_session_duration_seconds), 0) as avg_duration
    FROM analytics_prod_gold.fct_user_segments
    WHERE is_activated = true AND (is_planner OR is_passenger)
      AND {aw_clause}
    GROUP BY CASE WHEN is_planner THEN 'Planner' ELSE 'Passenger' END
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading planner vs passenger: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Page 4: AI & Prompts — Data Loaders
# ============================================================================

@st.cache_data(ttl=300)
def load_prompt_headline_kpis(start_date=None, end_date=None, app_version=None, activation_week=None):
    """Load prompt headline KPIs."""
    engine = get_database_connection()
    date_clause = _build_date_clause('query_date', start_date, end_date)
    av_clause = _build_app_version_clause('app_version', app_version)
    aw_clause = _build_activation_week_clause('user_activation_week', activation_week)

    query = f"""
    SELECT
        COUNT(DISTINCT query_id) as total_prompts,
        ROUND(100.0 * COUNT(DISTINCT query_id) FILTER (WHERE zero_save_prompt) / NULLIF(COUNT(DISTINCT query_id), 0), 1) as zero_save_pct,
        ROUND(AVG(save_rate), 4) as avg_save_rate,
        ROUND(AVG(cards_generated), 1) as avg_cards_generated,
        COUNT(DISTINCT query_id) FILTER (WHERE led_to_save) as prompts_leading_to_save,
        COUNT(DISTINCT query_id) FILTER (WHERE led_to_share) as prompts_leading_to_share
    FROM analytics_prod_gold.fct_prompt_analysis
    WHERE {date_clause}
      AND {av_clause}
      AND {aw_clause}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading prompt KPIs: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_prompt_action_funnel(start_date=None, end_date=None, app_version=None, activation_week=None):
    """Load prompt-to-action funnel."""
    engine = get_database_connection()
    date_clause = _build_date_clause('query_date', start_date, end_date)
    av_clause = _build_app_version_clause('app_version', app_version)
    aw_clause = _build_activation_week_clause('user_activation_week', activation_week)

    query = f"""
    SELECT
        COUNT(DISTINCT query_id) as total_prompts,
        COALESCE(SUM(cards_generated), 0) as total_cards_generated,
        COALESCE(SUM(cards_shown), 0) as total_cards_shown,
        COALESCE(SUM(cards_saved), 0) as total_cards_saved,
        COUNT(DISTINCT query_id) FILTER (WHERE led_to_share) as led_to_share
    FROM analytics_prod_gold.fct_prompt_analysis
    WHERE {date_clause}
      AND {av_clause}
      AND {aw_clause}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading prompt funnel: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_prompt_intent_performance(start_date=None, end_date=None, app_version=None, activation_week=None):
    """Load prompt performance by intent."""
    engine = get_database_connection()
    date_clause = _build_date_clause('query_date', start_date, end_date)
    av_clause = _build_app_version_clause('app_version', app_version)
    aw_clause = _build_activation_week_clause('user_activation_week', activation_week)

    query = f"""
    SELECT
        COALESCE(prompt_intent, 'unknown') as prompt_intent,
        COUNT(DISTINCT query_id) as prompt_count,
        ROUND(AVG(save_rate), 4) as avg_save_rate,
        ROUND(AVG(cards_generated), 1) as avg_cards_generated,
        ROUND(100.0 * COUNT(DISTINCT query_id) FILTER (WHERE zero_save_prompt) / NULLIF(COUNT(DISTINCT query_id), 0), 1) as zero_save_pct,
        COUNT(DISTINCT query_id) FILTER (WHERE led_to_share) as led_to_share_count
    FROM analytics_prod_gold.fct_prompt_analysis
    WHERE {date_clause}
      AND {av_clause}
      AND {aw_clause}
    GROUP BY COALESCE(prompt_intent, 'unknown')
    ORDER BY prompt_count DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading prompt intent performance: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_prompt_specificity(start_date=None, end_date=None, app_version=None, activation_week=None):
    """Load prompt specificity analysis."""
    engine = get_database_connection()
    date_clause = _build_date_clause('query_date', start_date, end_date)
    av_clause = _build_app_version_clause('app_version', app_version)
    aw_clause = _build_activation_week_clause('user_activation_week', activation_week)

    query = f"""
    SELECT
        prompt_specificity,
        COUNT(DISTINCT query_id) as count,
        ROUND(AVG(save_rate), 4) as avg_save_rate,
        ROUND(AVG(cards_generated), 1) as avg_cards_generated,
        ROUND(100.0 * COUNT(DISTINCT query_id) FILTER (WHERE zero_save_prompt) / NULLIF(COUNT(DISTINCT query_id), 0), 1) as zero_save_pct
    FROM analytics_prod_gold.fct_prompt_analysis
    WHERE prompt_specificity IS NOT NULL
      AND {date_clause}
      AND {av_clause}
      AND {aw_clause}
    GROUP BY prompt_specificity
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading prompt specificity: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_zero_save_trend(start_date=None, end_date=None, app_version=None):
    """Load zero-save prompt trend by week."""
    engine = get_database_connection()
    date_clause = _build_date_clause('query_date', start_date, end_date)
    av_clause = _build_app_version_clause('app_version', app_version)

    query = f"""
    SELECT
        DATE_TRUNC('week', query_date)::date as prompt_week,
        COUNT(DISTINCT query_id) as total_prompts,
        COUNT(DISTINCT query_id) FILTER (WHERE zero_save_prompt) as zero_save_count,
        ROUND(100.0 * COUNT(DISTINCT query_id) FILTER (WHERE zero_save_prompt) / NULLIF(COUNT(DISTINCT query_id), 0), 1) as zero_save_pct
    FROM analytics_prod_gold.fct_prompt_analysis
    WHERE {date_clause}
      AND {av_clause}
    GROUP BY DATE_TRUNC('week', query_date)::date
    ORDER BY prompt_week
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading zero-save trend: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_zero_save_prompts_detail(start_date=None, end_date=None, app_version=None, limit=20):
    """Load most common zero-save prompts."""
    engine = get_database_connection()
    date_clause = _build_date_clause('query_date', start_date, end_date)
    av_clause = _build_app_version_clause('app_version', app_version)

    query = f"""
    SELECT
        query_text,
        COUNT(*) as occurrences,
        ROUND(AVG(cards_generated), 1) as avg_cards_gen,
        ROUND(AVG(cards_shown), 1) as avg_cards_shown
    FROM analytics_prod_gold.fct_prompt_analysis
    WHERE zero_save_prompt = true
      AND {date_clause}
      AND {av_clause}
    GROUP BY query_text
    ORDER BY occurrences DESC
    LIMIT {limit}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading zero-save prompts: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_reprompting_analysis(start_date=None, end_date=None, app_version=None):
    """Load re-prompting analysis."""
    engine = get_database_connection()
    date_clause = _build_date_clause('query_date', start_date, end_date)
    av_clause = _build_app_version_clause('app_version', app_version)

    query = f"""
    SELECT
        CASE WHEN total_prompts_in_session > 1 THEN '2+ prompts' ELSE '1 prompt' END as session_type,
        COUNT(DISTINCT session_id) as session_count,
        ROUND(AVG(save_rate), 4) as avg_save_rate,
        ROUND(100.0 * COUNT(DISTINCT query_id) FILTER (WHERE led_to_save) / NULLIF(COUNT(DISTINCT query_id), 0), 1) as pct_led_to_save
    FROM analytics_prod_gold.fct_prompt_analysis
    WHERE session_id IS NOT NULL
      AND {date_clause}
      AND {av_clause}
    GROUP BY CASE WHEN total_prompts_in_session > 1 THEN '2+ prompts' ELSE '1 prompt' END
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading re-prompting analysis: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_pack_performance_top_bottom():
    """Load top and bottom packs by save rate."""
    engine = get_database_connection()
    query = """
    SELECT * FROM analytics_prod_gold.fct_pack_performance
    WHERE total_cards_generated >= 3
    ORDER BY save_rate DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading pack performance: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Page 5: Content & Places — Data Loaders
# ============================================================================

@st.cache_data(ttl=300)
def load_distinct_categories():
    """Get distinct categories from fct_place_performance."""
    engine = get_database_connection()
    query = """
    SELECT DISTINCT category
    FROM analytics_prod_gold.fct_place_performance
    WHERE category IS NOT NULL
    ORDER BY category
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df['category'].tolist()
    except Exception:
        return []


@st.cache_data(ttl=300)
def load_content_overview_kpis(categories=None):
    """Load content overview KPIs."""
    engine = get_database_connection()
    cat_filter = f"AND category = ANY(ARRAY{categories})" if categories else ""

    query = f"""
    SELECT
        COUNT(*) as total_places,
        COUNT(*) FILTER (WHERE total_impressions > 0) as places_with_impressions,
        ROUND(AVG(save_rate) FILTER (WHERE total_impressions >= 1), 4) as avg_save_rate,
        ROUND(AVG(right_swipe_rate) FILTER (WHERE total_impressions >= 1), 4) as avg_right_swipe_rate,
        ROUND(AVG(total_impressions), 1) as avg_impressions
    FROM analytics_prod_gold.fct_place_performance
    WHERE 1=1 {cat_filter}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading content KPIs: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_top_places(categories=None, min_impressions=1, limit=20, sort_by='save_rate', sort_order='DESC'):
    """Load top performing places."""
    engine = get_database_connection()
    cat_filter = f"AND category = ANY(ARRAY{categories})" if categories else ""
    allowed_sorts = {'save_rate', 'total_impressions', 'total_saves', 'right_swipe_rate'}
    sort_col = sort_by if sort_by in allowed_sorts else 'save_rate'

    query = f"""
    SELECT place_name, category, neighborhood, total_impressions,
           ROUND(save_rate * 100, 1) as save_rate_pct,
           ROUND(right_swipe_rate * 100, 1) as swipe_rate_pct,
           total_saves, viral_score, rating
    FROM analytics_prod_gold.fct_place_performance
    WHERE total_impressions >= {min_impressions}
      {cat_filter}
    ORDER BY {sort_col} {sort_order}
    LIMIT {limit}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading top places: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_bad_recommendations(categories=None):
    """Load places with high impressions but low saves."""
    engine = get_database_connection()
    cat_filter = f"AND category = ANY(ARRAY{categories})" if categories else ""

    query = f"""
    SELECT place_name, category, neighborhood, total_impressions,
           ROUND(save_rate * 100, 1) as save_rate_pct, total_saves, total_left_swipes
    FROM analytics_prod_gold.fct_place_performance
    WHERE total_impressions >= 3 AND save_rate < 0.05
      {cat_filter}
    ORDER BY total_impressions DESC
    LIMIT 20
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading bad recommendations: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_category_performance():
    """Load category-level performance."""
    engine = get_database_connection()
    query = """
    SELECT
        category,
        COUNT(*) as place_count,
        SUM(total_impressions) as total_impressions,
        SUM(total_saves) as total_saves,
        ROUND(100.0 * SUM(total_saves) / NULLIF(SUM(total_impressions), 0), 1) as save_rate_pct,
        ROUND(100.0 * SUM(total_right_swipes) / NULLIF(SUM(total_impressions), 0), 1) as swipe_rate_pct,
        SUM(total_shares) as total_shares
    FROM analytics_prod_gold.fct_place_performance
    WHERE total_impressions >= 1 AND category IS NOT NULL
    GROUP BY category
    ORDER BY total_impressions DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading category performance: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_neighborhood_performance():
    """Load neighborhood-level performance."""
    engine = get_database_connection()
    query = """
    SELECT
        neighborhood,
        COUNT(*) as place_count,
        SUM(total_impressions) as total_impressions,
        SUM(total_saves) as total_saves,
        ROUND(100.0 * SUM(total_saves) / NULLIF(SUM(total_impressions), 0), 1) as save_rate_pct,
        ROUND(100.0 * SUM(total_right_swipes) / NULLIF(SUM(total_impressions), 0), 1) as swipe_rate_pct,
        SUM(total_shares) as total_shares
    FROM analytics_prod_gold.fct_place_performance
    WHERE total_impressions >= 1 AND neighborhood IS NOT NULL
    GROUP BY neighborhood
    HAVING SUM(total_impressions) >= 10
    ORDER BY total_impressions DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading neighborhood performance: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_price_level_performance():
    """Load price level performance."""
    engine = get_database_connection()
    query = """
    SELECT
        price_level,
        COUNT(*) as place_count,
        ROUND(100.0 * SUM(total_saves) / NULLIF(SUM(total_impressions), 0), 1) as save_rate_pct,
        SUM(total_impressions) as total_impressions,
        SUM(total_saves) as total_saves
    FROM analytics_prod_gold.fct_place_performance
    WHERE price_level IS NOT NULL AND total_impressions >= 1
    GROUP BY price_level
    ORDER BY price_level
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading price level performance: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_viral_content():
    """Load top viral content."""
    engine = get_database_connection()
    query = """
    SELECT place_name, category, viral_score, total_saves, total_shares,
           ROUND(save_rate * 100, 1) as save_rate_pct
    FROM analytics_prod_gold.fct_place_performance
    WHERE viral_score > 0
    ORDER BY viral_score DESC
    LIMIT 10
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading viral content: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_scatter_data(categories=None):
    """Load data for impressions vs saves scatter plot."""
    engine = get_database_connection()
    cat_filter = f"AND category = ANY(ARRAY{categories})" if categories else ""

    query = f"""
    SELECT place_name, category, neighborhood, rating,
           total_impressions, save_rate, total_saves
    FROM analytics_prod_gold.fct_place_performance
    WHERE total_impressions >= 3
      {cat_filter}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading scatter data: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Page 6: Conversion & Viral — Data Loaders
# ============================================================================

@st.cache_data(ttl=300)
def load_conversion_overview():
    """Load conversion signals overview."""
    engine = get_database_connection()
    query = """
    SELECT
        COUNT(*) as total_conversions,
        COUNT(*) FILTER (WHERE action_type = 'opened_website') as opened_website,
        COUNT(*) FILTER (WHERE action_type = 'book_with_deck') as book_with_deck,
        COUNT(*) FILTER (WHERE action_type = 'click_directions') as click_directions,
        COUNT(*) FILTER (WHERE action_type = 'click_phone') as click_phone
    FROM analytics_prod_gold.fct_conversion_signals
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading conversion overview: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_conversion_context():
    """Load conversion context metrics."""
    engine = get_database_connection()
    query = """
    SELECT
        COUNT(*) as total,
        ROUND(100.0 * COUNT(*) FILTER (WHERE was_saved_first) / NULLIF(COUNT(*), 0), 1) as pct_saved_first,
        ROUND(AVG(time_from_save_to_conversion_minutes) FILTER (WHERE time_from_save_to_conversion_minutes IS NOT NULL), 1) as avg_minutes_to_convert,
        ROUND(100.0 * COUNT(*) FILTER (WHERE was_prompt_initiated) / NULLIF(COUNT(*), 0), 1) as pct_prompt_initiated
    FROM analytics_prod_gold.fct_conversion_signals
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading conversion context: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_conversion_by_category():
    """Load conversions by place category."""
    engine = get_database_connection()
    query = """
    SELECT
        COALESCE(place_category, 'Unknown') as place_category,
        COUNT(*) as conversion_count
    FROM analytics_prod_gold.fct_conversion_signals
    GROUP BY COALESCE(place_category, 'Unknown')
    ORDER BY conversion_count DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading conversion by category: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_viral_loop_summary():
    """Load viral loop summary metrics."""
    engine = get_database_connection()
    query = """
    SELECT
        COUNT(*) as total_shares,
        ROUND(AVG(unique_viewers), 1) as avg_viewers,
        ROUND(AVG(signup_conversion_rate), 4) as avg_signup_rate,
        ROUND(AVG(effective_k_factor), 4) as avg_k_factor,
        SUM(viewers_who_signed_up) as total_signups_from_shares
    FROM analytics_prod_gold.fct_viral_loop
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading viral loop summary: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_viral_loop_detail():
    """Load viral loop detail table."""
    engine = get_database_connection()
    query = """
    SELECT share_link_id, share_type, sharer_archetype,
           unique_viewers, viewers_who_signed_up,
           ROUND(signup_conversion_rate * 100, 1) as signup_rate_pct,
           ROUND(effective_k_factor, 3) as k_factor
    FROM analytics_prod_gold.fct_viral_loop
    ORDER BY unique_viewers DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading viral loop detail: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Daily Page Loaders (CEO Daily Report)
# ============================================================================
#
# All loaders accept report_date as a 'YYYY-MM-DD' string and filter to that
# single day (with the exception of the 7-day trend and weekly intensity
# loaders, which return a rolling window ending on report_date).
#
# Test users are excluded in every loader via join to stg_users.is_test_user = 0.


@st.cache_data(ttl=300)
def load_daily_topline_kpis(report_date):
    """Load the 8 top-line KPI tiles for the Daily page.

    Returns a single-row dict with:
      new_signups, dau, total_swipes, total_right_swipes, like_rate,
      saves, prompts, onboarding_completed, total_events
    """
    engine = get_database_connection()
    query = f"""
    WITH signups AS (
        SELECT COUNT(*)::bigint AS new_signups
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND DATE(created_at) = DATE '{report_date}'
    ),
    event_tallies AS (
        SELECT
            COUNT(DISTINCT e.user_id)::bigint AS dau,
            COUNT(*) FILTER (WHERE e.event_type IN ('swipe_right', 'swipe_left'))::bigint AS total_swipes,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_right')::bigint AS total_right_swipes,
            COUNT(*) FILTER (WHERE e.event_type IN ('save', 'saved'))::bigint AS saves,
            COUNT(*) FILTER (WHERE e.event_type = 'query')::bigint AS prompts,
            COUNT(*)::bigint AS total_events
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) = DATE '{report_date}'
    ),
    onboarding AS (
        SELECT COUNT(*)::bigint AS onboarding_completed
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND onboarding_completed = true
          AND DATE(created_at) = DATE '{report_date}'
    )
    SELECT
        s.new_signups,
        COALESCE(e.dau, 0) AS dau,
        COALESCE(e.total_swipes, 0) AS total_swipes,
        COALESCE(e.total_right_swipes, 0) AS total_right_swipes,
        CASE WHEN COALESCE(e.total_swipes, 0) > 0
             THEN e.total_right_swipes::numeric / e.total_swipes
             ELSE NULL END AS like_rate,
        COALESCE(e.saves, 0) AS saves,
        COALESCE(e.prompts, 0) AS prompts,
        COALESCE(o.onboarding_completed, 0) AS onboarding_completed,
        COALESCE(e.total_events, 0) AS total_events
    FROM signups s
    CROSS JOIN event_tallies e
    CROSS JOIN onboarding o
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Error loading daily topline KPIs: {str(e)}")
        return {}


@st.cache_data(ttl=300)
def load_daily_7day_trend(report_date):
    """Return the 7 days ending on report_date with DAU, new_signups, total_events, saves, prompts, swipes."""
    engine = get_database_connection()
    query = f"""
    WITH days AS (
        SELECT generate_series(
            DATE '{report_date}' - INTERVAL '6 days',
            DATE '{report_date}',
            INTERVAL '1 day'
        )::date AS day
    ),
    signups AS (
        SELECT DATE(created_at) AS day, COUNT(*)::bigint AS new_signups
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND DATE(created_at) BETWEEN DATE '{report_date}' - 6 AND DATE '{report_date}'
        GROUP BY DATE(created_at)
    ),
    events AS (
        SELECT
            DATE(e.event_timestamp) AS day,
            COUNT(DISTINCT e.user_id)::bigint AS dau,
            COUNT(*)::bigint AS total_events,
            COUNT(*) FILTER (WHERE e.event_type IN ('save', 'saved'))::bigint AS saves,
            COUNT(*) FILTER (WHERE e.event_type = 'query')::bigint AS prompts,
            COUNT(*) FILTER (WHERE e.event_type IN ('swipe_right', 'swipe_left'))::bigint AS total_swipes,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_right')::bigint AS right_swipes
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) BETWEEN DATE '{report_date}' - 6 AND DATE '{report_date}'
        GROUP BY DATE(e.event_timestamp)
    )
    SELECT
        d.day,
        COALESCE(s.new_signups, 0) AS new_signups,
        COALESCE(e.dau, 0) AS dau,
        COALESCE(e.total_events, 0) AS total_events,
        COALESCE(e.saves, 0) AS saves,
        COALESCE(e.prompts, 0) AS prompts,
        COALESCE(e.total_swipes, 0) AS total_swipes,
        CASE WHEN COALESCE(e.total_swipes, 0) > 0
             THEN e.right_swipes::numeric / e.total_swipes
             ELSE NULL END AS like_rate
    FROM days d
    LEFT JOIN signups s ON s.day = d.day
    LEFT JOIN events e ON e.day = d.day
    ORDER BY d.day
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading 7-day trend: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_daily_activation_checklist(report_date):
    """Activation checklist funnel for the cohort of users who signed up on report_date.

    Steps from iOS FirstSessionChecklistManager:
        1. deck_created
        2. places_saved  (maps to "3+ places saved")
        3. multiplayer_started

    A user is counted as completing a step if they emitted the corresponding
    checklist_task_completed event with that task_name on or after signup
    (any time — since this is a first-session checklist, we don't cap the
    window; it unlocks once the user does the thing).

    Returns dict:
        new_signups, deck_created, places_saved, multiplayer_started, all_three,
        stuck_no_deck, stuck_at_saves, stuck_at_mp
    """
    engine = get_database_connection()
    query = f"""
    WITH cohort AS (
        SELECT user_id
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND DATE(created_at) = DATE '{report_date}'
    ),
    task_events AS (
        SELECT
            c.user_id,
            BOOL_OR(e.task_name = 'deck_created') AS did_deck,
            BOOL_OR(e.task_name = 'places_saved') AS did_saves,
            BOOL_OR(e.task_name = 'multiplayer_started') AS did_mp
        FROM cohort c
        LEFT JOIN analytics_prod_silver.stg_app_events_enriched e
            ON c.user_id = e.user_id
           AND e.event_name = 'checklist_task_completed'
        GROUP BY c.user_id
    )
    SELECT
        COUNT(*)::bigint AS new_signups,
        COUNT(*) FILTER (WHERE COALESCE(did_deck, false))::bigint AS deck_created,
        COUNT(*) FILTER (WHERE COALESCE(did_saves, false))::bigint AS places_saved,
        COUNT(*) FILTER (WHERE COALESCE(did_mp, false))::bigint AS multiplayer_started,
        COUNT(*) FILTER (
            WHERE COALESCE(did_deck, false)
              AND COALESCE(did_saves, false)
              AND COALESCE(did_mp, false)
        )::bigint AS all_three,
        COUNT(*) FILTER (WHERE NOT COALESCE(did_deck, false))::bigint AS stuck_no_deck,
        COUNT(*) FILTER (
            WHERE COALESCE(did_deck, false) AND NOT COALESCE(did_saves, false)
        )::bigint AS stuck_at_saves,
        COUNT(*) FILTER (
            WHERE COALESCE(did_deck, false) AND COALESCE(did_saves, false) AND NOT COALESCE(did_mp, false)
        )::bigint AS stuck_at_mp
    FROM task_events
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Error loading daily activation checklist: {str(e)}")
        return {}


@st.cache_data(ttl=300)
def load_daily_new_signups_status(report_date):
    """Per-user onboarding & checklist status for users who signed up on report_date."""
    engine = get_database_connection()
    query = f"""
    WITH cohort AS (
        SELECT user_id, email, username, full_name, onboarding_completed
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND DATE(created_at) = DATE '{report_date}'
    ),
    task_events AS (
        SELECT
            user_id,
            BOOL_OR(task_name = 'deck_created') AS did_deck,
            BOOL_OR(task_name = 'places_saved') AS did_saves,
            BOOL_OR(task_name = 'multiplayer_started') AS did_mp
        FROM analytics_prod_silver.stg_app_events_enriched
        WHERE event_name = 'checklist_task_completed'
          AND user_id IN (SELECT user_id FROM cohort)
        GROUP BY user_id
    )
    SELECT
        COALESCE(NULLIF(c.full_name, ''), NULLIF(c.username, ''), c.email, 'Unknown') AS display_name,
        COALESCE(c.onboarding_completed, false) AS onboarded,
        COALESCE(te.did_deck, false) AS deck_created,
        COALESCE(te.did_saves, false) AS places_saved,
        COALESCE(te.did_mp, false) AS multiplayer_started,
        (COALESCE(te.did_deck, false)
         AND COALESCE(te.did_saves, false)
         AND COALESCE(te.did_mp, false)) AS all_three
    FROM cohort c
    LEFT JOIN task_events te ON c.user_id = te.user_id
    ORDER BY all_three DESC, deck_created DESC, places_saved DESC, display_name
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading new signups status: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_daily_category_popularity(report_date):
    """Category-level likes/dislikes for swipes on report_date.

    Uses stg_cards.is_* flags for category assignment. A card can belong
    to multiple categories, so a single swipe can count in multiple rows —
    this matches how category-level reporting is done elsewhere in the app.
    """
    engine = get_database_connection()
    query = f"""
    WITH swipes AS (
        SELECT
            e.card_id,
            e.event_type
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) = DATE '{report_date}'
          AND e.event_type IN ('swipe_right', 'swipe_left')
          AND e.card_id IS NOT NULL
    ),
    joined AS (
        SELECT
            c.is_drinks, c.is_dining, c.is_entertainment,
            c.is_culture, c.is_adventure, c.is_health,
            s.event_type
        FROM swipes s
        INNER JOIN analytics_prod_silver.stg_cards c ON c.card_id = s.card_id
    ),
    unpivoted AS (
        SELECT 'Drinks' AS category, event_type FROM joined WHERE is_drinks
        UNION ALL SELECT 'Dining', event_type FROM joined WHERE is_dining
        UNION ALL SELECT 'Entertainment', event_type FROM joined WHERE is_entertainment
        UNION ALL SELECT 'Culture', event_type FROM joined WHERE is_culture
        UNION ALL SELECT 'Adventure', event_type FROM joined WHERE is_adventure
        UNION ALL SELECT 'Health', event_type FROM joined WHERE is_health
    )
    SELECT
        category,
        COUNT(*) FILTER (WHERE event_type = 'swipe_right')::bigint AS likes,
        COUNT(*) FILTER (WHERE event_type = 'swipe_left')::bigint AS dislikes,
        COUNT(*)::bigint AS total,
        CASE WHEN COUNT(*) > 0
             THEN COUNT(*) FILTER (WHERE event_type = 'swipe_right')::numeric / COUNT(*)
             ELSE NULL END AS like_pct
    FROM unpivoted
    GROUP BY category
    ORDER BY total DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading category popularity: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_daily_places_flagged(report_date, min_swipes=4):
    """Places with more dislikes than likes on report_date, with min swipe count."""
    engine = get_database_connection()
    query = f"""
    WITH swipes AS (
        SELECT
            e.card_id,
            pr.resolved_place_id,
            e.event_type
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        LEFT JOIN analytics_prod_silver.int_place_resolver pr
            ON e.card_id = pr.original_card_id
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) = DATE '{report_date}'
          AND e.event_type IN ('swipe_right', 'swipe_left')
          AND e.card_id IS NOT NULL
    ),
    agg AS (
        SELECT
            resolved_place_id,
            COUNT(*) FILTER (WHERE event_type = 'swipe_right')::bigint AS likes,
            COUNT(*) FILTER (WHERE event_type = 'swipe_left')::bigint AS dislikes,
            COUNT(*)::bigint AS total_swipes
        FROM swipes
        WHERE resolved_place_id IS NOT NULL
        GROUP BY resolved_place_id
    )
    SELECT
        p.place_id AS id,
        p.name,
        split_part(p.formatted_address, ',', 1) AS area,
        a.likes,
        a.dislikes,
        a.total_swipes AS total,
        a.dislikes::numeric / NULLIF(a.total_swipes, 0) AS dislike_pct
    FROM agg a
    INNER JOIN analytics_prod_bronze.src_places p ON a.resolved_place_id = p.place_id
    WHERE a.total_swipes >= {int(min_swipes)}
      AND a.dislikes > a.likes
    ORDER BY dislike_pct DESC, a.total_swipes DESC
    LIMIT 20
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading places flagged: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_daily_top_liked_places(report_date, limit=10):
    """Top liked places on report_date (ordered by likes desc, then like ratio)."""
    engine = get_database_connection()
    query = f"""
    WITH swipes AS (
        SELECT
            e.card_id,
            pr.resolved_place_id,
            e.event_type
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        LEFT JOIN analytics_prod_silver.int_place_resolver pr
            ON e.card_id = pr.original_card_id
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) = DATE '{report_date}'
          AND e.event_type IN ('swipe_right', 'swipe_left')
          AND e.card_id IS NOT NULL
    ),
    agg AS (
        SELECT
            resolved_place_id,
            COUNT(*) FILTER (WHERE event_type = 'swipe_right')::bigint AS likes,
            COUNT(*) FILTER (WHERE event_type = 'swipe_left')::bigint AS dislikes
        FROM swipes
        WHERE resolved_place_id IS NOT NULL
        GROUP BY resolved_place_id
    )
    SELECT
        p.place_id AS id,
        p.name,
        split_part(p.formatted_address, ',', 1) AS area,
        a.likes,
        a.dislikes
    FROM agg a
    INNER JOIN analytics_prod_bronze.src_places p ON a.resolved_place_id = p.place_id
    WHERE a.likes > 0
    ORDER BY a.likes DESC, (a.likes::numeric / NULLIF(a.likes + a.dislikes, 0)) DESC
    LIMIT {int(limit)}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading top liked places: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_daily_weekly_intensity(report_date, weeks=12):
    """Per-week avg activity per active activated user, for N weeks ending in report_date's week.

    Denominator: activated users who were active in the week.
    Metrics: avg swipes, avg saves, avg shares per such user.
    """
    engine = get_database_connection()
    query = f"""
    WITH weeks AS (
        SELECT generate_series(
            date_trunc('week', DATE '{report_date}')::date - ({int(weeks) - 1} * 7),
            date_trunc('week', DATE '{report_date}')::date,
            INTERVAL '1 week'
        )::date AS week_start
    ),
    activated_user_activity AS (
        SELECT
            date_trunc('week', e.event_timestamp)::date AS week_start,
            e.user_id,
            COUNT(*) FILTER (WHERE e.event_type IN ('swipe_right', 'swipe_left'))::bigint AS swipes,
            COUNT(*) FILTER (WHERE e.event_type IN ('save', 'saved'))::bigint AS saves,
            COUNT(*) FILTER (WHERE e.event_category = 'Share')::bigint AS shares
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        INNER JOIN analytics_prod_gold.fct_user_activation fua ON e.user_id = fua.user_id
        WHERE u.is_test_user = 0
          AND fua.is_activated = true
          AND e.event_timestamp >= (date_trunc('week', DATE '{report_date}')::date - ({int(weeks) - 1} * 7))
          AND e.event_timestamp <  (date_trunc('week', DATE '{report_date}')::date + INTERVAL '7 days')
        GROUP BY 1, 2
    ),
    weekly_avgs AS (
        SELECT
            week_start,
            COUNT(DISTINCT user_id)::bigint AS active_activated_users,
            ROUND(AVG(swipes)::numeric, 2) AS avg_swipes,
            ROUND(AVG(saves)::numeric, 2) AS avg_saves,
            ROUND(AVG(shares)::numeric, 2) AS avg_shares
        FROM activated_user_activity
        GROUP BY week_start
    )
    SELECT
        w.week_start,
        COALESCE(wa.active_activated_users, 0) AS active_activated_users,
        COALESCE(wa.avg_swipes, 0) AS avg_swipes,
        COALESCE(wa.avg_saves, 0) AS avg_saves,
        COALESCE(wa.avg_shares, 0) AS avg_shares
    FROM weeks w
    LEFT JOIN weekly_avgs wa ON wa.week_start = w.week_start
    ORDER BY w.week_start
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading weekly intensity: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_daily_user_activity(report_date):
    """Per-user activity on report_date. Includes a 'new' flag if the user signed up on report_date."""
    engine = get_database_connection()
    query = f"""
    WITH event_agg AS (
        SELECT
            e.user_id,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_right')::bigint AS likes,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_left')::bigint AS dislikes,
            COUNT(*) FILTER (WHERE e.event_type IN ('save', 'saved'))::bigint AS saves,
            COUNT(*) FILTER (WHERE e.event_type = 'query')::bigint AS prompts,
            COUNT(*)::bigint AS total_events
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) = DATE '{report_date}'
        GROUP BY e.user_id
    ),
    boards_agg AS (
        SELECT user_id, COUNT(*)::bigint AS boards_created
        FROM analytics_prod_bronze.src_boards
        WHERE DATE(created_at) = DATE '{report_date}'
          AND (is_default = false OR is_default IS NULL)
        GROUP BY user_id
    )
    SELECT
        COALESCE(NULLIF(u.full_name, ''), NULLIF(u.username, ''), u.email, 'Unknown') AS display_name,
        ea.likes,
        ea.dislikes,
        CASE WHEN (ea.likes + ea.dislikes) > 0
             THEN ea.likes::numeric / (ea.likes + ea.dislikes)
             ELSE NULL END AS like_rate,
        ea.saves,
        COALESCE(ba.boards_created, 0) AS boards_created,
        ea.prompts,
        ea.total_events,
        (DATE(u.created_at) = DATE '{report_date}') AS is_new
    FROM event_agg ea
    INNER JOIN analytics_prod_silver.stg_users u ON ea.user_id = u.user_id
    LEFT JOIN boards_agg ba ON ba.user_id = u.user_id
    WHERE u.is_test_user = 0
    ORDER BY ea.total_events DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading daily user activity: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Weekly Report loaders
# ============================================================================
# Each mirrors a daily loader but aggregates over a Mon–Sun week.
# week_start is a date string for the Monday of the target week.


@st.cache_data(ttl=300)
def load_weekly_topline_kpis(week_start):
    """Top-line KPIs aggregated over a Mon–Sun week. Returns dict with WAU instead of DAU."""
    engine = get_database_connection()
    query = f"""
    WITH signups AS (
        SELECT COUNT(*)::bigint AS new_signups
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND DATE(created_at) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6
    ),
    event_tallies AS (
        SELECT
            COUNT(DISTINCT e.user_id)::bigint AS wau,
            COUNT(*) FILTER (WHERE e.event_type IN ('swipe_right', 'swipe_left'))::bigint AS total_swipes,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_right')::bigint AS total_right_swipes,
            COUNT(*) FILTER (WHERE e.event_type IN ('save', 'saved'))::bigint AS saves,
            COUNT(*) FILTER (WHERE e.event_type = 'query')::bigint AS prompts,
            COUNT(*)::bigint AS total_events
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6
    ),
    onboarding AS (
        SELECT COUNT(*)::bigint AS onboarding_completed
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND onboarding_completed = true
          AND DATE(created_at) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6
    )
    SELECT
        s.new_signups,
        COALESCE(e.wau, 0) AS wau,
        COALESCE(e.total_swipes, 0) AS total_swipes,
        COALESCE(e.total_right_swipes, 0) AS total_right_swipes,
        CASE WHEN COALESCE(e.total_swipes, 0) > 0
             THEN e.total_right_swipes::numeric / e.total_swipes
             ELSE NULL END AS like_rate,
        COALESCE(e.saves, 0) AS saves,
        COALESCE(e.prompts, 0) AS prompts,
        COALESCE(o.onboarding_completed, 0) AS onboarding_completed,
        COALESCE(e.total_events, 0) AS total_events
    FROM signups s
    CROSS JOIN event_tallies e
    CROSS JOIN onboarding o
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Error loading weekly topline KPIs: {str(e)}")
        return {}


@st.cache_data(ttl=300)
def load_weekly_multiweek_trend(week_start, num_weeks=8):
    """Return num_weeks weeks ending on week_start's week with WAU, new_signups, etc."""
    engine = get_database_connection()
    query = f"""
    WITH weeks AS (
        SELECT generate_series(
            DATE '{week_start}' - ({int(num_weeks) - 1} * 7),
            DATE '{week_start}',
            INTERVAL '1 week'
        )::date AS week_start
    ),
    signups AS (
        SELECT
            date_trunc('week', created_at)::date AS week_start,
            COUNT(*)::bigint AS new_signups
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND DATE(created_at) BETWEEN DATE '{week_start}' - ({int(num_weeks) - 1} * 7)
                                    AND DATE '{week_start}' + 6
        GROUP BY 1
    ),
    events AS (
        SELECT
            date_trunc('week', e.event_timestamp)::date AS week_start,
            COUNT(DISTINCT e.user_id)::bigint AS wau,
            COUNT(*)::bigint AS total_events,
            COUNT(*) FILTER (WHERE e.event_type IN ('save', 'saved'))::bigint AS saves,
            COUNT(*) FILTER (WHERE e.event_type = 'query')::bigint AS prompts,
            COUNT(*) FILTER (WHERE e.event_type IN ('swipe_right', 'swipe_left'))::bigint AS total_swipes,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_right')::bigint AS right_swipes
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) BETWEEN DATE '{week_start}' - ({int(num_weeks) - 1} * 7)
                                           AND DATE '{week_start}' + 6
        GROUP BY 1
    )
    SELECT
        w.week_start,
        COALESCE(s.new_signups, 0) AS new_signups,
        COALESCE(e.wau, 0) AS wau,
        COALESCE(e.total_events, 0) AS total_events,
        COALESCE(e.saves, 0) AS saves,
        COALESCE(e.prompts, 0) AS prompts,
        COALESCE(e.total_swipes, 0) AS total_swipes,
        CASE WHEN COALESCE(e.total_swipes, 0) > 0
             THEN e.right_swipes::numeric / e.total_swipes
             ELSE NULL END AS like_rate
    FROM weeks w
    LEFT JOIN signups s ON s.week_start = w.week_start
    LEFT JOIN events e ON e.week_start = w.week_start
    ORDER BY w.week_start
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading multi-week trend: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_weekly_activation_checklist(week_start):
    """Activation checklist funnel for users who signed up during the week."""
    engine = get_database_connection()
    query = f"""
    WITH cohort AS (
        SELECT user_id
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND DATE(created_at) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6
    ),
    task_events AS (
        SELECT
            c.user_id,
            BOOL_OR(e.task_name = 'deck_created') AS did_deck,
            BOOL_OR(e.task_name = 'places_saved') AS did_saves,
            BOOL_OR(e.task_name = 'multiplayer_started') AS did_mp
        FROM cohort c
        LEFT JOIN analytics_prod_silver.stg_app_events_enriched e
            ON c.user_id = e.user_id
           AND e.event_name = 'checklist_task_completed'
        GROUP BY c.user_id
    )
    SELECT
        COUNT(*)::bigint AS new_signups,
        COUNT(*) FILTER (WHERE COALESCE(did_deck, false))::bigint AS deck_created,
        COUNT(*) FILTER (WHERE COALESCE(did_saves, false))::bigint AS places_saved,
        COUNT(*) FILTER (WHERE COALESCE(did_mp, false))::bigint AS multiplayer_started,
        COUNT(*) FILTER (
            WHERE COALESCE(did_deck, false)
              AND COALESCE(did_saves, false)
              AND COALESCE(did_mp, false)
        )::bigint AS all_three,
        COUNT(*) FILTER (WHERE NOT COALESCE(did_deck, false))::bigint AS stuck_no_deck,
        COUNT(*) FILTER (
            WHERE COALESCE(did_deck, false) AND NOT COALESCE(did_saves, false)
        )::bigint AS stuck_at_saves,
        COUNT(*) FILTER (
            WHERE COALESCE(did_deck, false) AND COALESCE(did_saves, false) AND NOT COALESCE(did_mp, false)
        )::bigint AS stuck_at_mp
    FROM task_events
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Error loading weekly activation checklist: {str(e)}")
        return {}


@st.cache_data(ttl=300)
def load_weekly_new_signups_status(week_start):
    """Per-user onboarding & checklist status for users who signed up during the week."""
    engine = get_database_connection()
    query = f"""
    WITH cohort AS (
        SELECT user_id, email, username, full_name, onboarding_completed, created_at
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND DATE(created_at) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6
    ),
    task_events AS (
        SELECT
            user_id,
            BOOL_OR(task_name = 'deck_created') AS did_deck,
            BOOL_OR(task_name = 'places_saved') AS did_saves,
            BOOL_OR(task_name = 'multiplayer_started') AS did_mp
        FROM analytics_prod_silver.stg_app_events_enriched
        WHERE event_name = 'checklist_task_completed'
          AND user_id IN (SELECT user_id FROM cohort)
        GROUP BY user_id
    )
    SELECT
        COALESCE(NULLIF(c.full_name, ''), NULLIF(c.username, ''), c.email, 'Unknown') AS display_name,
        DATE(c.created_at) AS signup_date,
        COALESCE(c.onboarding_completed, false) AS onboarded,
        COALESCE(te.did_deck, false) AS deck_created,
        COALESCE(te.did_saves, false) AS places_saved,
        COALESCE(te.did_mp, false) AS multiplayer_started,
        (COALESCE(te.did_deck, false)
         AND COALESCE(te.did_saves, false)
         AND COALESCE(te.did_mp, false)) AS all_three
    FROM cohort c
    LEFT JOIN task_events te ON c.user_id = te.user_id
    ORDER BY all_three DESC, deck_created DESC, places_saved DESC, display_name
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading weekly new signups status: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_weekly_category_popularity(week_start):
    """Category-level likes/dislikes for swipes during the week."""
    engine = get_database_connection()
    query = f"""
    WITH swipes AS (
        SELECT e.card_id, e.event_type
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6
          AND e.event_type IN ('swipe_right', 'swipe_left')
          AND e.card_id IS NOT NULL
    ),
    joined AS (
        SELECT
            c.is_drinks, c.is_dining, c.is_entertainment,
            c.is_culture, c.is_adventure, c.is_health,
            s.event_type
        FROM swipes s
        INNER JOIN analytics_prod_silver.stg_cards c ON c.card_id = s.card_id
    ),
    unpivoted AS (
        SELECT 'Drinks' AS category, event_type FROM joined WHERE is_drinks
        UNION ALL SELECT 'Dining', event_type FROM joined WHERE is_dining
        UNION ALL SELECT 'Entertainment', event_type FROM joined WHERE is_entertainment
        UNION ALL SELECT 'Culture', event_type FROM joined WHERE is_culture
        UNION ALL SELECT 'Adventure', event_type FROM joined WHERE is_adventure
        UNION ALL SELECT 'Health', event_type FROM joined WHERE is_health
    )
    SELECT
        category,
        COUNT(*) FILTER (WHERE event_type = 'swipe_right')::bigint AS likes,
        COUNT(*) FILTER (WHERE event_type = 'swipe_left')::bigint AS dislikes,
        COUNT(*)::bigint AS total,
        CASE WHEN COUNT(*) > 0
             THEN COUNT(*) FILTER (WHERE event_type = 'swipe_right')::numeric / COUNT(*)
             ELSE NULL END AS like_pct
    FROM unpivoted
    GROUP BY category
    ORDER BY total DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading weekly category popularity: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_weekly_places_flagged(week_start, min_swipes=10):
    """Places with more dislikes than likes during the week."""
    engine = get_database_connection()
    query = f"""
    WITH swipes AS (
        SELECT e.card_id, pr.resolved_place_id, e.event_type
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        LEFT JOIN analytics_prod_silver.int_place_resolver pr
            ON e.card_id = pr.original_card_id
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6
          AND e.event_type IN ('swipe_right', 'swipe_left')
          AND e.card_id IS NOT NULL
    ),
    agg AS (
        SELECT
            resolved_place_id,
            COUNT(*) FILTER (WHERE event_type = 'swipe_right')::bigint AS likes,
            COUNT(*) FILTER (WHERE event_type = 'swipe_left')::bigint AS dislikes,
            COUNT(*)::bigint AS total_swipes
        FROM swipes
        WHERE resolved_place_id IS NOT NULL
        GROUP BY resolved_place_id
    )
    SELECT
        p.place_id AS id, p.name,
        split_part(p.formatted_address, ',', 1) AS area,
        a.likes, a.dislikes, a.total_swipes AS total,
        a.dislikes::numeric / NULLIF(a.total_swipes, 0) AS dislike_pct
    FROM agg a
    INNER JOIN analytics_prod_bronze.src_places p ON a.resolved_place_id = p.place_id
    WHERE a.total_swipes >= {int(min_swipes)}
      AND a.dislikes > a.likes
    ORDER BY dislike_pct DESC, a.total_swipes DESC
    LIMIT 20
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading weekly places flagged: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_weekly_top_liked_places(week_start, limit=10):
    """Top liked places during the week."""
    engine = get_database_connection()
    query = f"""
    WITH swipes AS (
        SELECT e.card_id, pr.resolved_place_id, e.event_type
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        LEFT JOIN analytics_prod_silver.int_place_resolver pr
            ON e.card_id = pr.original_card_id
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6
          AND e.event_type IN ('swipe_right', 'swipe_left')
          AND e.card_id IS NOT NULL
    ),
    agg AS (
        SELECT
            resolved_place_id,
            COUNT(*) FILTER (WHERE event_type = 'swipe_right')::bigint AS likes,
            COUNT(*) FILTER (WHERE event_type = 'swipe_left')::bigint AS dislikes
        FROM swipes
        WHERE resolved_place_id IS NOT NULL
        GROUP BY resolved_place_id
    )
    SELECT
        p.place_id AS id, p.name,
        split_part(p.formatted_address, ',', 1) AS area,
        a.likes, a.dislikes
    FROM agg a
    INNER JOIN analytics_prod_bronze.src_places p ON a.resolved_place_id = p.place_id
    WHERE a.likes > 0
    ORDER BY a.likes DESC, (a.likes::numeric / NULLIF(a.likes + a.dislikes, 0)) DESC
    LIMIT {int(limit)}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading weekly top liked places: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_weekly_user_activity(week_start):
    """Per-user activity during the week. is_new = signed up that week."""
    engine = get_database_connection()
    query = f"""
    WITH event_agg AS (
        SELECT
            e.user_id,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_right')::bigint AS likes,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_left')::bigint AS dislikes,
            COUNT(*) FILTER (WHERE e.event_type IN ('save', 'saved'))::bigint AS saves,
            COUNT(*) FILTER (WHERE e.event_type = 'query')::bigint AS prompts,
            COUNT(*)::bigint AS total_events
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND DATE(e.event_timestamp) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6
        GROUP BY e.user_id
    ),
    boards_agg AS (
        SELECT user_id, COUNT(*)::bigint AS boards_created
        FROM analytics_prod_bronze.src_boards
        WHERE DATE(created_at) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6
          AND (is_default = false OR is_default IS NULL)
        GROUP BY user_id
    )
    SELECT
        COALESCE(NULLIF(u.full_name, ''), NULLIF(u.username, ''), u.email, 'Unknown') AS display_name,
        ea.likes, ea.dislikes,
        CASE WHEN (ea.likes + ea.dislikes) > 0
             THEN ea.likes::numeric / (ea.likes + ea.dislikes)
             ELSE NULL END AS like_rate,
        ea.saves,
        COALESCE(ba.boards_created, 0) AS boards_created,
        ea.prompts, ea.total_events,
        (DATE(u.created_at) BETWEEN DATE '{week_start}' AND DATE '{week_start}' + 6) AS is_new
    FROM event_agg ea
    INNER JOIN analytics_prod_silver.stg_users u ON ea.user_id = u.user_id
    LEFT JOIN boards_agg ba ON ba.user_id = u.user_id
    WHERE u.is_test_user = 0
    ORDER BY ea.total_events DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading weekly user activity: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Monthly Report loaders
# ============================================================================
# Each mirrors a daily loader but aggregates over a calendar month.
# year and month are integers (e.g. 2026, 3 for March 2026).


@st.cache_data(ttl=300)
def load_monthly_topline_kpis(year, month):
    """Top-line KPIs aggregated over a calendar month. Returns dict with MAU instead of DAU."""
    engine = get_database_connection()
    query = f"""
    WITH signups AS (
        SELECT COUNT(*)::bigint AS new_signups
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND date_trunc('month', created_at) = '{year}-{month:02d}-01'::date
    ),
    event_tallies AS (
        SELECT
            COUNT(DISTINCT e.user_id)::bigint AS mau,
            COUNT(*) FILTER (WHERE e.event_type IN ('swipe_right', 'swipe_left'))::bigint AS total_swipes,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_right')::bigint AS total_right_swipes,
            COUNT(*) FILTER (WHERE e.event_type IN ('save', 'saved'))::bigint AS saves,
            COUNT(*) FILTER (WHERE e.event_type = 'query')::bigint AS prompts,
            COUNT(*)::bigint AS total_events
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND date_trunc('month', e.event_timestamp) = '{year}-{month:02d}-01'::date
    ),
    onboarding AS (
        SELECT COUNT(*)::bigint AS onboarding_completed
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND onboarding_completed = true
          AND date_trunc('month', created_at) = '{year}-{month:02d}-01'::date
    )
    SELECT
        s.new_signups,
        COALESCE(e.mau, 0) AS mau,
        COALESCE(e.total_swipes, 0) AS total_swipes,
        COALESCE(e.total_right_swipes, 0) AS total_right_swipes,
        CASE WHEN COALESCE(e.total_swipes, 0) > 0
             THEN e.total_right_swipes::numeric / e.total_swipes
             ELSE NULL END AS like_rate,
        COALESCE(e.saves, 0) AS saves,
        COALESCE(e.prompts, 0) AS prompts,
        COALESCE(o.onboarding_completed, 0) AS onboarding_completed,
        COALESCE(e.total_events, 0) AS total_events
    FROM signups s
    CROSS JOIN event_tallies e
    CROSS JOIN onboarding o
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Error loading monthly topline KPIs: {str(e)}")
        return {}


@st.cache_data(ttl=300)
def load_monthly_multimonth_trend(year, month, num_months=6):
    """Return num_months months ending on the target month with MAU, new_signups, etc."""
    engine = get_database_connection()
    query = f"""
    WITH months AS (
        SELECT generate_series(
            ('{year}-{month:02d}-01'::date - INTERVAL '{int(num_months) - 1} months')::date,
            '{year}-{month:02d}-01'::date,
            INTERVAL '1 month'
        )::date AS month_start
    ),
    signups AS (
        SELECT
            date_trunc('month', created_at)::date AS month_start,
            COUNT(*)::bigint AS new_signups
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND created_at >= ('{year}-{month:02d}-01'::date - INTERVAL '{int(num_months) - 1} months')
          AND date_trunc('month', created_at) <= '{year}-{month:02d}-01'::date
        GROUP BY 1
    ),
    events AS (
        SELECT
            date_trunc('month', e.event_timestamp)::date AS month_start,
            COUNT(DISTINCT e.user_id)::bigint AS mau,
            COUNT(*)::bigint AS total_events,
            COUNT(*) FILTER (WHERE e.event_type IN ('save', 'saved'))::bigint AS saves,
            COUNT(*) FILTER (WHERE e.event_type = 'query')::bigint AS prompts,
            COUNT(*) FILTER (WHERE e.event_type IN ('swipe_right', 'swipe_left'))::bigint AS total_swipes,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_right')::bigint AS right_swipes
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND e.event_timestamp >= ('{year}-{month:02d}-01'::date - INTERVAL '{int(num_months) - 1} months')
          AND date_trunc('month', e.event_timestamp) <= '{year}-{month:02d}-01'::date
        GROUP BY 1
    )
    SELECT
        m.month_start,
        COALESCE(s.new_signups, 0) AS new_signups,
        COALESCE(e.mau, 0) AS mau,
        COALESCE(e.total_events, 0) AS total_events,
        COALESCE(e.saves, 0) AS saves,
        COALESCE(e.prompts, 0) AS prompts,
        COALESCE(e.total_swipes, 0) AS total_swipes,
        CASE WHEN COALESCE(e.total_swipes, 0) > 0
             THEN e.right_swipes::numeric / e.total_swipes
             ELSE NULL END AS like_rate
    FROM months m
    LEFT JOIN signups s ON s.month_start = m.month_start
    LEFT JOIN events e ON e.month_start = m.month_start
    ORDER BY m.month_start
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading multi-month trend: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_monthly_activation_checklist(year, month):
    """Activation checklist funnel for users who signed up during the month."""
    engine = get_database_connection()
    query = f"""
    WITH cohort AS (
        SELECT user_id
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND date_trunc('month', created_at) = '{year}-{month:02d}-01'::date
    ),
    task_events AS (
        SELECT
            c.user_id,
            BOOL_OR(e.task_name = 'deck_created') AS did_deck,
            BOOL_OR(e.task_name = 'places_saved') AS did_saves,
            BOOL_OR(e.task_name = 'multiplayer_started') AS did_mp
        FROM cohort c
        LEFT JOIN analytics_prod_silver.stg_app_events_enriched e
            ON c.user_id = e.user_id
           AND e.event_name = 'checklist_task_completed'
        GROUP BY c.user_id
    )
    SELECT
        COUNT(*)::bigint AS new_signups,
        COUNT(*) FILTER (WHERE COALESCE(did_deck, false))::bigint AS deck_created,
        COUNT(*) FILTER (WHERE COALESCE(did_saves, false))::bigint AS places_saved,
        COUNT(*) FILTER (WHERE COALESCE(did_mp, false))::bigint AS multiplayer_started,
        COUNT(*) FILTER (
            WHERE COALESCE(did_deck, false)
              AND COALESCE(did_saves, false)
              AND COALESCE(did_mp, false)
        )::bigint AS all_three,
        COUNT(*) FILTER (WHERE NOT COALESCE(did_deck, false))::bigint AS stuck_no_deck,
        COUNT(*) FILTER (
            WHERE COALESCE(did_deck, false) AND NOT COALESCE(did_saves, false)
        )::bigint AS stuck_at_saves,
        COUNT(*) FILTER (
            WHERE COALESCE(did_deck, false) AND COALESCE(did_saves, false) AND NOT COALESCE(did_mp, false)
        )::bigint AS stuck_at_mp
    FROM task_events
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Error loading monthly activation checklist: {str(e)}")
        return {}


@st.cache_data(ttl=300)
def load_monthly_new_signups_status(year, month):
    """Per-user onboarding & checklist status for users who signed up during the month."""
    engine = get_database_connection()
    query = f"""
    WITH cohort AS (
        SELECT user_id, email, username, full_name, onboarding_completed, created_at
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
          AND date_trunc('month', created_at) = '{year}-{month:02d}-01'::date
    ),
    task_events AS (
        SELECT
            user_id,
            BOOL_OR(task_name = 'deck_created') AS did_deck,
            BOOL_OR(task_name = 'places_saved') AS did_saves,
            BOOL_OR(task_name = 'multiplayer_started') AS did_mp
        FROM analytics_prod_silver.stg_app_events_enriched
        WHERE event_name = 'checklist_task_completed'
          AND user_id IN (SELECT user_id FROM cohort)
        GROUP BY user_id
    )
    SELECT
        COALESCE(NULLIF(c.full_name, ''), NULLIF(c.username, ''), c.email, 'Unknown') AS display_name,
        DATE(c.created_at) AS signup_date,
        COALESCE(c.onboarding_completed, false) AS onboarded,
        COALESCE(te.did_deck, false) AS deck_created,
        COALESCE(te.did_saves, false) AS places_saved,
        COALESCE(te.did_mp, false) AS multiplayer_started,
        (COALESCE(te.did_deck, false)
         AND COALESCE(te.did_saves, false)
         AND COALESCE(te.did_mp, false)) AS all_three
    FROM cohort c
    LEFT JOIN task_events te ON c.user_id = te.user_id
    ORDER BY all_three DESC, deck_created DESC, places_saved DESC, display_name
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading monthly new signups status: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_monthly_category_popularity(year, month):
    """Category-level likes/dislikes for swipes during the month."""
    engine = get_database_connection()
    query = f"""
    WITH swipes AS (
        SELECT e.card_id, e.event_type
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND date_trunc('month', e.event_timestamp) = '{year}-{month:02d}-01'::date
          AND e.event_type IN ('swipe_right', 'swipe_left')
          AND e.card_id IS NOT NULL
    ),
    joined AS (
        SELECT
            c.is_drinks, c.is_dining, c.is_entertainment,
            c.is_culture, c.is_adventure, c.is_health,
            s.event_type
        FROM swipes s
        INNER JOIN analytics_prod_silver.stg_cards c ON c.card_id = s.card_id
    ),
    unpivoted AS (
        SELECT 'Drinks' AS category, event_type FROM joined WHERE is_drinks
        UNION ALL SELECT 'Dining', event_type FROM joined WHERE is_dining
        UNION ALL SELECT 'Entertainment', event_type FROM joined WHERE is_entertainment
        UNION ALL SELECT 'Culture', event_type FROM joined WHERE is_culture
        UNION ALL SELECT 'Adventure', event_type FROM joined WHERE is_adventure
        UNION ALL SELECT 'Health', event_type FROM joined WHERE is_health
    )
    SELECT
        category,
        COUNT(*) FILTER (WHERE event_type = 'swipe_right')::bigint AS likes,
        COUNT(*) FILTER (WHERE event_type = 'swipe_left')::bigint AS dislikes,
        COUNT(*)::bigint AS total,
        CASE WHEN COUNT(*) > 0
             THEN COUNT(*) FILTER (WHERE event_type = 'swipe_right')::numeric / COUNT(*)
             ELSE NULL END AS like_pct
    FROM unpivoted
    GROUP BY category
    ORDER BY total DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading monthly category popularity: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_monthly_places_flagged(year, month, min_swipes=20):
    """Places with more dislikes than likes during the month."""
    engine = get_database_connection()
    query = f"""
    WITH swipes AS (
        SELECT e.card_id, pr.resolved_place_id, e.event_type
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        LEFT JOIN analytics_prod_silver.int_place_resolver pr
            ON e.card_id = pr.original_card_id
        WHERE u.is_test_user = 0
          AND date_trunc('month', e.event_timestamp) = '{year}-{month:02d}-01'::date
          AND e.event_type IN ('swipe_right', 'swipe_left')
          AND e.card_id IS NOT NULL
    ),
    agg AS (
        SELECT
            resolved_place_id,
            COUNT(*) FILTER (WHERE event_type = 'swipe_right')::bigint AS likes,
            COUNT(*) FILTER (WHERE event_type = 'swipe_left')::bigint AS dislikes,
            COUNT(*)::bigint AS total_swipes
        FROM swipes
        WHERE resolved_place_id IS NOT NULL
        GROUP BY resolved_place_id
    )
    SELECT
        p.place_id AS id, p.name,
        split_part(p.formatted_address, ',', 1) AS area,
        a.likes, a.dislikes, a.total_swipes AS total,
        a.dislikes::numeric / NULLIF(a.total_swipes, 0) AS dislike_pct
    FROM agg a
    INNER JOIN analytics_prod_bronze.src_places p ON a.resolved_place_id = p.place_id
    WHERE a.total_swipes >= {int(min_swipes)}
      AND a.dislikes > a.likes
    ORDER BY dislike_pct DESC, a.total_swipes DESC
    LIMIT 20
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading monthly places flagged: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_monthly_top_liked_places(year, month, limit=10):
    """Top liked places during the month."""
    engine = get_database_connection()
    query = f"""
    WITH swipes AS (
        SELECT e.card_id, pr.resolved_place_id, e.event_type
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        LEFT JOIN analytics_prod_silver.int_place_resolver pr
            ON e.card_id = pr.original_card_id
        WHERE u.is_test_user = 0
          AND date_trunc('month', e.event_timestamp) = '{year}-{month:02d}-01'::date
          AND e.event_type IN ('swipe_right', 'swipe_left')
          AND e.card_id IS NOT NULL
    ),
    agg AS (
        SELECT
            resolved_place_id,
            COUNT(*) FILTER (WHERE event_type = 'swipe_right')::bigint AS likes,
            COUNT(*) FILTER (WHERE event_type = 'swipe_left')::bigint AS dislikes
        FROM swipes
        WHERE resolved_place_id IS NOT NULL
        GROUP BY resolved_place_id
    )
    SELECT
        p.place_id AS id, p.name,
        split_part(p.formatted_address, ',', 1) AS area,
        a.likes, a.dislikes
    FROM agg a
    INNER JOIN analytics_prod_bronze.src_places p ON a.resolved_place_id = p.place_id
    WHERE a.likes > 0
    ORDER BY a.likes DESC, (a.likes::numeric / NULLIF(a.likes + a.dislikes, 0)) DESC
    LIMIT {int(limit)}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading monthly top liked places: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_monthly_user_activity(year, month):
    """Per-user activity during the month. is_new = signed up that month."""
    engine = get_database_connection()
    query = f"""
    WITH event_agg AS (
        SELECT
            e.user_id,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_right')::bigint AS likes,
            COUNT(*) FILTER (WHERE e.event_type = 'swipe_left')::bigint AS dislikes,
            COUNT(*) FILTER (WHERE e.event_type IN ('save', 'saved'))::bigint AS saves,
            COUNT(*) FILTER (WHERE e.event_type = 'query')::bigint AS prompts,
            COUNT(*)::bigint AS total_events
        FROM analytics_prod_silver.stg_unified_events e
        INNER JOIN analytics_prod_silver.stg_users u USING (user_id)
        WHERE u.is_test_user = 0
          AND date_trunc('month', e.event_timestamp) = '{year}-{month:02d}-01'::date
        GROUP BY e.user_id
    ),
    boards_agg AS (
        SELECT user_id, COUNT(*)::bigint AS boards_created
        FROM analytics_prod_bronze.src_boards
        WHERE date_trunc('month', created_at) = '{year}-{month:02d}-01'::date
          AND (is_default = false OR is_default IS NULL)
        GROUP BY user_id
    )
    SELECT
        COALESCE(NULLIF(u.full_name, ''), NULLIF(u.username, ''), u.email, 'Unknown') AS display_name,
        ea.likes, ea.dislikes,
        CASE WHEN (ea.likes + ea.dislikes) > 0
             THEN ea.likes::numeric / (ea.likes + ea.dislikes)
             ELSE NULL END AS like_rate,
        ea.saves,
        COALESCE(ba.boards_created, 0) AS boards_created,
        ea.prompts, ea.total_events,
        (date_trunc('month', u.created_at) = '{year}-{month:02d}-01'::date) AS is_new
    FROM event_agg ea
    INNER JOIN analytics_prod_silver.stg_users u ON ea.user_id = u.user_id
    LEFT JOIN boards_agg ba ON ba.user_id = u.user_id
    WHERE u.is_test_user = 0
    ORDER BY ea.total_events DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading monthly user activity: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Track 2 loaders — new gold models from Phase D/E (PRs #50, #51)
# ============================================================================


@st.cache_data(ttl=300)
def load_surface_performance(start_date=None, end_date=None):
    """Load surface attribution data from fct_surface_performance.

    Aggregates rates over the filtered date range at the surface grain.
    """
    engine = get_database_connection()

    conditions = ["origin_surface IS NOT NULL"]
    if start_date:
        conditions.append(f"metric_date >= '{start_date}'")
    if end_date:
        conditions.append(f"metric_date <= '{end_date}'")
    where = "WHERE " + " AND ".join(conditions)

    query = f"""
    SELECT
        origin_surface,
        SUM(total_events)::bigint AS total_events,
        SUM(distinct_sessions)::bigint AS distinct_sessions,
        SUM(saves)::bigint AS saves,
        SUM(shares)::bigint AS shares,
        SUM(swipes)::bigint AS swipes,
        SUM(views)::bigint AS views,
        SUM(initiated_sessions)::bigint AS initiated_sessions,
        SUM(initiated_sessions_with_save)::bigint AS init_with_save,
        SUM(initiated_sessions_with_share)::bigint AS init_with_share,
        SUM(initiated_sessions_with_psr_broad)::bigint AS init_with_psr_broad,
        CASE WHEN SUM(initiated_sessions) > 0
             THEN SUM(initiated_sessions_with_save)::numeric / SUM(initiated_sessions)
             ELSE NULL END AS ssr_initiated,
        CASE WHEN SUM(initiated_sessions) > 0
             THEN SUM(initiated_sessions_with_share)::numeric / SUM(initiated_sessions)
             ELSE NULL END AS shr_initiated,
        CASE WHEN SUM(initiated_sessions) > 0
             THEN SUM(initiated_sessions_with_psr_broad)::numeric / SUM(initiated_sessions)
             ELSE NULL END AS psr_broad_initiated,
        CASE WHEN SUM(views) > 0
             THEN SUM(saves)::numeric / SUM(views)
             ELSE NULL END AS save_per_view_rate
    FROM analytics_prod_gold.fct_surface_performance
    {where}
    GROUP BY origin_surface
    ORDER BY total_events DESC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading surface performance: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_dextr_funnel(start_date=None, end_date=None, app_version=None):
    """Load Dextr query → results funnel from fct_dextr_funnel (telemetry era only)."""
    engine = get_database_connection()

    conditions = ["1=1"]
    if start_date:
        conditions.append(f"metric_date >= '{start_date}'")
    if end_date:
        conditions.append(f"metric_date <= '{end_date}'")
    if app_version:
        conditions.append(f"app_version = '{app_version}'")
    where = " AND ".join(conditions)

    query = f"""
    SELECT
        SUM(queries_submitted)::bigint AS queries_submitted,
        SUM(queries_with_results_view)::bigint AS queries_with_results_view,
        SUM(fast_path_queries)::bigint AS fast_path_queries,
        SUM(fast_path_queries_with_results)::bigint AS fast_path_queries_with_results,
        SUM(queries_leading_to_save)::bigint AS queries_leading_to_save,
        CASE WHEN SUM(queries_submitted) > 0
             THEN SUM(queries_with_results_view)::numeric / SUM(queries_submitted)
             ELSE NULL END AS results_view_rate,
        CASE WHEN SUM(queries_submitted) > 0
             THEN SUM(queries_leading_to_save)::numeric / SUM(queries_submitted)
             ELSE NULL END AS query_to_save_rate,
        ROUND(AVG(median_query_to_results_seconds)::numeric, 2) AS median_query_to_results_seconds,
        ROUND(AVG(p90_query_to_results_seconds)::numeric, 2) AS p90_query_to_results_seconds
    FROM analytics_prod_gold.fct_dextr_funnel
    WHERE {where}
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading Dextr funnel: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_first_session_experience():
    """Aggregate counts across the first-session experience funnel."""
    engine = get_database_connection()
    query = """
    SELECT
        COUNT(*)::bigint AS entered,
        COUNT(*) FILTER (WHERE saw_checklist)::bigint AS saw_checklist,
        COUNT(*) FILTER (WHERE COALESCE(tasks_completed_count, 0) > 0)::bigint AS completed_any_task,
        COUNT(*) FILTER (WHERE completed_checklist)::bigint AS completed_checklist,
        COUNT(*) FILTER (WHERE unlocked_spin)::bigint AS unlocked_spin,
        COUNT(*) FILTER (WHERE won_spin)::bigint AS won_spin,
        COUNT(*) FILTER (WHERE saw_notif_prompt)::bigint AS saw_notif_prompt,
        COUNT(*) FILTER (WHERE granted_notif)::bigint AS granted_notif,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY minutes_checklist_view_to_complete)
            FILTER (WHERE completed_checklist) AS median_minutes_to_complete
    FROM analytics_prod_gold.fct_first_session_experience
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading first-session experience: {str(e)}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Place Curation (page 13) — first write-capable loaders
# ---------------------------------------------------------------------------

def load_places_for_curation():
    """Load all places with media counts and engagement metrics for curation.

    NOT cached — must reflect live state immediately after deletes.
    """
    engine = get_database_connection()
    query = """
    SELECT
        p.id,
        p.name,
        p.description,
        COALESCE(p.area, perf.neighborhood, split_part(p.formatted_address, ',', 1)) AS neighborhood,
        p.categories,
        p.rating,
        p.user_ratings_total,
        COALESCE(p.media_count, 0) AS media_count,
        COALESCE(perf.total_impressions, 0) AS total_impressions,
        COALESCE(perf.total_left_swipes, 0) AS total_left_swipes,
        COALESCE(perf.total_saves, 0) AS total_saves,
        ROUND(
            perf.total_left_swipes::numeric
            / NULLIF(perf.total_impressions, 0) * 100, 1
        ) AS dislike_rate_pct,
        ROUND(perf.save_rate * 100, 1) AS save_rate_pct,
        p.source_type,
        p.is_featured
    FROM public.places p
    LEFT JOIN analytics_prod_gold.fct_place_performance perf
        ON perf.place_id = p.id
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading places for curation: {str(e)}")
        return pd.DataFrame()


def load_place_media(place_ids: list[int]) -> pd.DataFrame:
    """Load media URLs for given place IDs, ordered by display_order."""
    engine = get_database_connection()
    query = text("""
        SELECT place_id, media_url, display_order
        FROM public.place_media
        WHERE place_id = ANY(:place_ids) AND is_active = true
        ORDER BY place_id, display_order
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"place_ids": place_ids})
        return df
    except Exception as e:
        st.error(f"Error loading place media: {str(e)}")
        return pd.DataFrame()


def delete_places(place_ids: list[int]) -> int:
    """Delete places from the database. Returns affected row count.

    Also removes child rows in tables with NO ACTION foreign keys
    (dextr_candidate_pool, business_claims) before deleting places.
    """
    engine = get_database_connection()
    child_cleanup = text(
        "DELETE FROM public.dextr_candidate_pool WHERE place_id IN :ids"
    ).bindparams(bindparam("ids", expanding=True))
    claims_cleanup = text(
        "DELETE FROM public.business_claims WHERE place_id IN :ids"
    ).bindparams(bindparam("ids", expanding=True))
    delete_query = text(
        "DELETE FROM public.places WHERE id IN :ids"
    ).bindparams(bindparam("ids", expanding=True))
    try:
        with engine.begin() as conn:
            conn.execute(child_cleanup, {"ids": place_ids})
            conn.execute(claims_cleanup, {"ids": place_ids})
            result = conn.execute(delete_query, {"ids": place_ids})
        return result.rowcount
    except Exception as e:
        st.error(f"Error deleting places: {str(e)}")
        return 0
        st.error(f"Error loading monthly retention summary: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# CVP FUNNEL ANALYSIS DATA LOADERS
# ============================================================================

@st.cache_data(ttl=300)
def load_cvp_funnel_metrics(days=90):
    """
    Load CVP funnel conversion metrics

    Returns DataFrame with columns:
    - total_users: All non-test users in the time period
    - initiated: Users who prompted/searched/browsed featured
    - considered: Users who saved at least 1 card
    - validated: Users who shared OR created multiplayer
    - decided: Users whose shares were clicked OR multiplayer had 2+ participants
    """
    engine = get_database_connection()

    query = f"""
    WITH user_base AS (
        SELECT user_id, created_at
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
        AND created_at >= current_date - interval '{days} days'
    ),

    initiated AS (
        -- Users who prompted Dextr
        SELECT DISTINCT dq.user_id
        FROM analytics_prod_bronze.src_dextr_queries dq
        INNER JOIN user_base ub ON dq.user_id = ub.user_id
    ),

    considered AS (
        -- Users who saved at least 1 card
        SELECT DISTINCT cca.user_id
        FROM analytics_prod_bronze.src_core_card_actions cca
        INNER JOIN user_base ub ON cca.user_id = ub.user_id
        WHERE cca.action_type = 'saved'
    ),

    validated AS (
        -- Users who shared OR created multiplayer with 2+ participants
        SELECT DISTINCT user_id FROM (
            -- Shared a card
            SELECT cca.user_id
            FROM analytics_prod_bronze.src_core_card_actions cca
            INNER JOIN user_base ub ON cca.user_id = ub.user_id
            WHERE cca.action_type = 'share'

            UNION

            -- Created multiplayer session
            SELECT sm.creator_id as user_id
            FROM analytics_prod_silver.stg_multiplayer sm
            INNER JOIN user_base ub ON sm.creator_id = ub.user_id
        ) shared_or_multiplayer
    ),

    decided AS (
        -- Multiplayer sessions that had actual participation (2+ participants already filtered in stg_multiplayer)
        SELECT DISTINCT sm.creator_id as user_id
        FROM analytics_prod_silver.stg_multiplayer sm
        INNER JOIN user_base ub ON sm.creator_id = ub.user_id
        WHERE sm.total_participants >= 2
    )

    SELECT
        (SELECT COUNT(*) FROM user_base) as total_users,
        (SELECT COUNT(*) FROM initiated) as initiated,
        (SELECT COUNT(*) FROM considered) as considered,
        (SELECT COUNT(*) FROM validated) as validated,
        (SELECT COUNT(*) FROM decided) as decided
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading CVP funnel metrics: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_funnel_by_cohort(days=90):
    """
    Load funnel metrics broken down by signup week cohort
    """
    engine = get_database_connection()

    query = f"""
    WITH user_base AS (
        SELECT
            user_id,
            created_at,
            DATE_TRUNC('week', created_at) as cohort_week
        FROM analytics_prod_silver.stg_users
        WHERE is_test_user = 0
        AND created_at >= current_date - interval '{days} days'
    ),

    initiated AS (
        SELECT DISTINCT ub.cohort_week, dq.user_id
        FROM analytics_prod_bronze.src_dextr_queries dq
        INNER JOIN user_base ub ON dq.user_id = ub.user_id
    ),

    considered AS (
        SELECT DISTINCT ub.cohort_week, cca.user_id
        FROM analytics_prod_bronze.src_core_card_actions cca
        INNER JOIN user_base ub ON cca.user_id = ub.user_id
        WHERE cca.action_type = 'saved'
    ),

    validated AS (
        SELECT DISTINCT cohort_week, user_id FROM (
            SELECT ub.cohort_week, cca.user_id
            FROM analytics_prod_bronze.src_core_card_actions cca
            INNER JOIN user_base ub ON cca.user_id = ub.user_id
            WHERE cca.action_type = 'share'

            UNION

            SELECT ub.cohort_week, sm.creator_id as user_id
            FROM analytics_prod_silver.stg_multiplayer sm
            INNER JOIN user_base ub ON sm.creator_id = ub.user_id
        ) shared_or_multiplayer
    ),

    decided AS (
        SELECT DISTINCT ub.cohort_week, sm.creator_id as user_id
        FROM analytics_prod_silver.stg_multiplayer sm
        INNER JOIN user_base ub ON sm.creator_id = ub.user_id
        WHERE sm.total_participants >= 2
    )

    SELECT
        ub.cohort_week,
        COUNT(DISTINCT ub.user_id) as total_users,
        COUNT(DISTINCT i.user_id) as initiated,
        COUNT(DISTINCT c.user_id) as considered,
        COUNT(DISTINCT v.user_id) as validated,
        COUNT(DISTINCT d.user_id) as decided
    FROM user_base ub
    LEFT JOIN initiated i ON ub.cohort_week = i.cohort_week AND ub.user_id = i.user_id
    LEFT JOIN considered c ON ub.cohort_week = c.cohort_week AND ub.user_id = c.user_id
    LEFT JOIN validated v ON ub.cohort_week = v.cohort_week AND ub.user_id = v.user_id
    LEFT JOIN decided d ON ub.cohort_week = d.cohort_week AND ub.user_id = d.user_id
    GROUP BY ub.cohort_week
    ORDER BY ub.cohort_week DESC
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading funnel by cohort: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_prompt_to_save_analysis(days=90):
    """
    Detailed analysis of the Initiated → Considered conversion

    Returns metrics about:
    - Total prompts
    - Prompts with at least 1 right swipe
    - Prompts with at least 1 save
    - Avg cards shown per prompt
    - Avg cards swiped per prompt
    - Avg cards liked per prompt
    - Avg cards saved per prompt
    """
    engine = get_database_connection()

    query = f"""
    WITH prompt_sessions AS (
        SELECT
            dq.user_id,
            dq.query_id,
            dq.response_pack_id as pack_id,
            dq.query_timestamp
        FROM analytics_prod_bronze.src_dextr_queries dq
        INNER JOIN analytics_prod_silver.stg_users u ON dq.user_id = u.user_id
        WHERE u.is_test_user = 0
        AND dq.query_timestamp >= current_date - interval '{days} days'
    ),

    pack_engagement AS (
        SELECT
            ps.query_id,
            ps.user_id,
            ps.pack_id,
            COUNT(*) as cards_in_pack,
            COUNT(CASE WHEN dpc.shown_to_user THEN 1 END) as cards_shown,
            COUNT(CASE WHEN dpc.user_action IS NOT NULL THEN 1 END) as cards_swiped,
            COUNT(CASE WHEN dpc.user_action = 'right' THEN 1 END) as cards_liked,
            COUNT(CASE WHEN dpc.user_action = 'left' THEN 1 END) as cards_disliked
        FROM prompt_sessions ps
        LEFT JOIN analytics_prod_bronze.src_dextr_pack_cards dpc ON ps.pack_id = dpc.pack_id
        GROUP BY ps.query_id, ps.user_id, ps.pack_id
    ),

    saves_per_session AS (
        SELECT
            pe.query_id,
            pe.user_id,
            COUNT(DISTINCT cca.card_id) as cards_saved
        FROM pack_engagement pe
        LEFT JOIN analytics_prod_bronze.src_core_card_actions cca
            ON pe.user_id = cca.user_id
            AND cca.action_type = 'saved'
            AND cca.source = 'dextr'
            AND cca.source_id = pe.pack_id::text
        GROUP BY pe.query_id, pe.user_id
    )

    SELECT
        COUNT(*) as total_prompts,
        COUNT(CASE WHEN pe.cards_swiped > 0 THEN 1 END) as prompts_with_swipes,
        COUNT(CASE WHEN pe.cards_liked > 0 THEN 1 END) as prompts_with_likes,
        COUNT(CASE WHEN ss.cards_saved > 0 THEN 1 END) as prompts_with_saves,

        ROUND(AVG(pe.cards_in_pack), 1) as avg_cards_per_pack,
        ROUND(AVG(pe.cards_shown), 1) as avg_cards_shown,
        ROUND(AVG(pe.cards_swiped), 1) as avg_cards_swiped,
        ROUND(AVG(pe.cards_liked), 1) as avg_cards_liked,
        ROUND(AVG(COALESCE(ss.cards_saved, 0)), 2) as avg_cards_saved,

        -- Conversion rates
        ROUND(100.0 * COUNT(CASE WHEN pe.cards_swiped > 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as prompt_to_swipe_rate,
        ROUND(100.0 * COUNT(CASE WHEN pe.cards_liked > 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as prompt_to_like_rate,
        ROUND(100.0 * COUNT(CASE WHEN ss.cards_saved > 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as prompt_to_save_rate

    FROM pack_engagement pe
    LEFT JOIN saves_per_session ss ON pe.query_id = ss.query_id
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading prompt-to-save analysis: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_save_to_share_analysis(days=90):
    """
    Detailed analysis of Considered → Validated conversion
    """
    engine = get_database_connection()

    query = f"""
    WITH users_who_saved AS (
        SELECT DISTINCT
            cca.user_id,
            MIN(cca.action_timestamp) as first_save_timestamp
        FROM analytics_prod_bronze.src_core_card_actions cca
        INNER JOIN analytics_prod_silver.stg_users u ON cca.user_id = u.user_id
        WHERE u.is_test_user = 0
        AND cca.action_type = 'saved'
        AND cca.action_timestamp >= current_date - interval '{days} days'
        GROUP BY cca.user_id
    ),

    saves_per_user AS (
        SELECT
            cca.user_id,
            COUNT(DISTINCT cca.card_id) as total_cards_saved,
            COUNT(DISTINCT cca.board_id) as boards_used
        FROM analytics_prod_bronze.src_core_card_actions cca
        INNER JOIN users_who_saved uws ON cca.user_id = uws.user_id
        WHERE cca.action_type = 'saved'
        GROUP BY cca.user_id
    ),

    users_who_shared AS (
        SELECT DISTINCT
            cca.user_id,
            MIN(cca.action_timestamp) as first_share_timestamp
        FROM analytics_prod_bronze.src_core_card_actions cca
        INNER JOIN users_who_saved uws ON cca.user_id = uws.user_id
        WHERE cca.action_type = 'share'
        AND cca.action_timestamp >= uws.first_save_timestamp
        GROUP BY cca.user_id
    ),

    users_who_created_multiplayer AS (
        SELECT DISTINCT
            sm.creator_id as user_id,
            MIN(sm.created_at) as first_multiplayer_timestamp
        FROM analytics_prod_silver.stg_multiplayer sm
        INNER JOIN users_who_saved uws ON sm.creator_id = uws.user_id
        WHERE sm.created_at >= uws.first_save_timestamp
        GROUP BY sm.creator_id
    )

    SELECT
        COUNT(DISTINCT uws.user_id) as users_who_saved,
        COUNT(DISTINCT uwsh.user_id) as users_who_shared,
        COUNT(DISTINCT uwmp.user_id) as users_who_created_multiplayer,
        COUNT(DISTINCT COALESCE(uwsh.user_id, uwmp.user_id)) as users_who_validated,

        AVG(spu.total_cards_saved) as avg_cards_saved_per_user,
        AVG(spu.boards_used) as avg_boards_per_user,

        ROUND(100.0 * COUNT(DISTINCT uwsh.user_id) / NULLIF(COUNT(DISTINCT uws.user_id), 0), 1) as save_to_share_rate,
        ROUND(100.0 * COUNT(DISTINCT uwmp.user_id) / NULLIF(COUNT(DISTINCT uws.user_id), 0), 1) as save_to_multiplayer_rate,
        ROUND(100.0 * COUNT(DISTINCT COALESCE(uwsh.user_id, uwmp.user_id)) / NULLIF(COUNT(DISTINCT uws.user_id), 0), 1) as save_to_validate_rate

    FROM users_who_saved uws
    LEFT JOIN saves_per_user spu ON uws.user_id = spu.user_id
    LEFT JOIN users_who_shared uwsh ON uws.user_id = uwsh.user_id
    LEFT JOIN users_who_created_multiplayer uwmp ON uws.user_id = uwmp.user_id
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading save-to-share analysis: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_like_rate_by_position(days=90):
    """
    Analyze like rate by card position in pack (validates refinement hypothesis)
    """
    engine = get_database_connection()

    query = f"""
    SELECT
        dpc.card_order as position,
        COUNT(*) as total_cards,
        COUNT(CASE WHEN dpc.user_action = 'right' THEN 1 END) as liked,
        COUNT(CASE WHEN dpc.user_action = 'left' THEN 1 END) as disliked,
        ROUND(100.0 * COUNT(CASE WHEN dpc.user_action = 'right' THEN 1 END) /
            NULLIF(COUNT(CASE WHEN dpc.user_action IS NOT NULL THEN 1 END), 0), 1) as like_rate
    FROM analytics_prod_bronze.src_dextr_pack_cards dpc
    INNER JOIN analytics_prod_bronze.src_dextr_queries dq ON dpc.pack_id = dq.response_pack_id
    INNER JOIN analytics_prod_silver.stg_users u ON dq.user_id = u.user_id
    WHERE u.is_test_user = 0
    AND dq.query_timestamp >= current_date - interval '{days} days'
    AND dpc.card_order IS NOT NULL
    GROUP BY dpc.card_order
    ORDER BY dpc.card_order
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading like rate by position: {str(e)}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Spin Wheel Winners (page 14) — kanban board + mechanism metrics
#
# Source of truth for winners: public.spin_wheel_wins (read-only, owned by Dracon2).
# Fulfillment state lives in analytics_ops.spin_wheel_winner_outreach, keyed by the
# composite natural key (win_user_id, win_place_id, win_created_at). On every board
# read we upsert a placeholder outreach row for any win that doesn't have one, so
# new winners appear automatically without a separate sync job.
# ---------------------------------------------------------------------------

_OUTREACH_UPSERT_SQL = """
insert into analytics_ops.spin_wheel_winner_outreach
  (win_user_id, win_place_id, win_created_at, status)
select w.user_id, w.place_id, w.created_at, 'to_contact'
from public.spin_wheel_wins w
on conflict (win_user_id, win_place_id, win_created_at) do nothing
"""


@st.cache_data(ttl=60)
def load_spin_wheel_metrics(start_date, end_date):
    """Mechanism metrics for the spin-wheel winners page.

    Returns dict: total_spins, total_wins, win_rate, fulfillment_rate,
                  unique_winners, pending_contact, sent_count, redeemed_count.
    """
    engine = get_database_connection()
    query = text("""
        with spins as (
            select count(*)::bigint as n
            from public.spin_wheel_attempts a
            left join analytics_prod_silver.stg_users u on u.user_id = a.user_id
            where a.game_type = 'spin_wheel'
              and a.spun_at::date between :start_date and :end_date
              and coalesce(u.is_test_user, 0) = 0
        ),
        wins as (
            select count(*)::bigint as n,
                   count(distinct w.user_id)::bigint as unique_winners
            from public.spin_wheel_wins w
            left join analytics_prod_silver.stg_users u on u.user_id = w.user_id
            where w.created_at::date between :start_date and :end_date
              and coalesce(u.is_test_user, 0) = 0
        ),
        outreach as (
            select
                count(*) filter (where o.status = 'to_contact')::bigint  as pending_contact,
                count(*) filter (where o.status = 'sent')::bigint        as sent_count,
                count(*) filter (where o.status = 'redeemed')::bigint    as redeemed_count,
                count(*) filter (where o.status in ('sent','redeemed'))::bigint as fulfilled
            from analytics_ops.spin_wheel_winner_outreach o
            left join analytics_prod_silver.stg_users u on u.user_id = o.win_user_id
            where o.win_created_at::date between :start_date and :end_date
              and coalesce(u.is_test_user, 0) = 0
        )
        select
            spins.n                                                    as total_spins,
            wins.n                                                     as total_wins,
            wins.unique_winners                                        as unique_winners,
            case when spins.n > 0 then wins.n::numeric / spins.n end   as win_rate,
            case when wins.n > 0
                 then outreach.fulfilled::numeric / wins.n
            end                                                        as fulfillment_rate,
            outreach.pending_contact,
            outreach.sent_count,
            outreach.redeemed_count
        from spins, wins, outreach
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"start_date": start_date, "end_date": end_date})
        return df.iloc[0].to_dict() if not df.empty else {}
    except Exception as e:
        st.error(f"Error loading spin wheel metrics: {str(e)}")
        return {}


@st.cache_data(ttl=60)
def load_spin_wheel_daily_trend(start_date, end_date):
    """Daily spins vs wins in the selected range. Returns df[day, spins, wins]."""
    engine = get_database_connection()
    query = text("""
        with days as (
            select generate_series(cast(:start_date as date), cast(:end_date as date), interval '1 day')::date as day
        ),
        spins as (
            select a.spun_at::date as day, count(*)::bigint as spins
            from public.spin_wheel_attempts a
            left join analytics_prod_silver.stg_users u on u.user_id = a.user_id
            where a.game_type = 'spin_wheel'
              and a.spun_at::date between :start_date and :end_date
              and coalesce(u.is_test_user, 0) = 0
            group by 1
        ),
        wins as (
            select w.created_at::date as day, count(*)::bigint as wins
            from public.spin_wheel_wins w
            left join analytics_prod_silver.stg_users u on u.user_id = w.user_id
            where w.created_at::date between :start_date and :end_date
              and coalesce(u.is_test_user, 0) = 0
            group by 1
        )
        select d.day,
               coalesce(s.spins, 0) as spins,
               coalesce(w.wins, 0)  as wins
        from days d
        left join spins s on s.day = d.day
        left join wins  w on w.day = d.day
        order by d.day
    """)
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn, params={"start_date": start_date, "end_date": end_date})
    except Exception as e:
        st.error(f"Error loading spin wheel daily trend: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_spin_wheel_top_places(start_date, end_date, limit=10):
    """Top places by win count in the selected range."""
    engine = get_database_connection()
    query = text("""
        select w.place_name, count(*)::bigint as win_count
        from public.spin_wheel_wins w
        left join analytics_prod_silver.stg_users u on u.user_id = w.user_id
        where w.created_at::date between :start_date and :end_date
          and coalesce(u.is_test_user, 0) = 0
        group by w.place_name
        order by win_count desc
        limit :lim
    """)
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                query, conn,
                params={"start_date": start_date, "end_date": end_date, "lim": limit},
            )
    except Exception as e:
        st.error(f"Error loading top win places: {str(e)}")
        return pd.DataFrame()


def load_spin_wheel_winners_board(start_date, end_date, search_term=None, include_skipped=False):
    """Main kanban query.

    Auto-upserts a placeholder outreach row for every win before reading, so the
    board reflects the true universe of winners. Not cached — writes on this page
    must be visible on the next rerun.
    """
    engine = get_database_connection()
    select_sql = text("""
        select
            o.id                        as outreach_id,
            o.status,
            o.assigned_to,
            o.contacted_at,
            o.sent_at,
            o.sent_by,
            o.gift_card_code,
            o.gift_card_value,
            o.redeemed_at,
            o.notes,
            o.updated_at,
            w.user_id                   as user_id,
            w.place_id                  as place_id,
            w.place_name                as place_name,
            w.place_data                as place_data,
            w.created_at                as won_at,
            coalesce(u.username, u.full_name, u.email) as display_name,
            u.full_name                 as full_name,
            u.username                  as username,
            u.email                     as email,
            u.is_test_user              as is_test_user
        from public.spin_wheel_wins w
        join analytics_ops.spin_wheel_winner_outreach o
          on o.win_user_id    = w.user_id
         and o.win_place_id   = w.place_id
         and o.win_created_at = w.created_at
        left join analytics_prod_silver.stg_users u
          on u.user_id = w.user_id
        where w.created_at::date between :start_date and :end_date
          and coalesce(u.is_test_user, 0) = 0
          and (:include_skipped or o.status <> 'skipped')
          and (
                cast(:search_term as text) is null
                or lower(w.place_name)              like '%' || cast(:search_term as text) || '%'
                or lower(coalesce(u.username, ''))  like '%' || cast(:search_term as text) || '%'
                or lower(coalesce(u.full_name, '')) like '%' || cast(:search_term as text) || '%'
                or lower(coalesce(u.email, ''))     like '%' || cast(:search_term as text) || '%'
              )
        order by w.created_at desc
    """)
    search_lc = search_term.lower().strip() if search_term else None
    try:
        with engine.begin() as conn:
            conn.execute(text(_OUTREACH_UPSERT_SQL))
            df = pd.read_sql(
                select_sql, conn,
                params={
                    "start_date": start_date,
                    "end_date": end_date,
                    "search_term": search_lc,
                    "include_skipped": include_skipped,
                },
            )
        return df
    except Exception as e:
        st.error(f"Error loading spin wheel winners board: {str(e)}")
        return pd.DataFrame()


def load_spin_wheel_audit_log(limit=20):
    """Recent outreach updates, most recent first. MVP treats updated_at as audit."""
    engine = get_database_connection()
    query = text("""
        select
            o.updated_at,
            o.status,
            o.assigned_to,
            o.sent_by,
            w.place_name,
            coalesce(u.username, u.full_name, u.email) as display_name
        from analytics_ops.spin_wheel_winner_outreach o
        join public.spin_wheel_wins w
          on w.user_id    = o.win_user_id
         and w.place_id   = o.win_place_id
         and w.created_at = o.win_created_at
        left join analytics_prod_silver.stg_users u on u.user_id = o.win_user_id
        where coalesce(u.is_test_user, 0) = 0
        order by o.updated_at desc
        limit :lim
    """)
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn, params={"lim": limit})
    except Exception as e:
        st.error(f"Error loading spin wheel audit log: {str(e)}")
        return pd.DataFrame()


def update_winner_outreach_status(
    outreach_id,
    new_status,
    operator_email=None,
    gift_card_code=None,
    gift_card_value=None,
    notes=None,
):
    """Transition a winner's outreach row to a new status.

    Sets the matching timestamp for the destination state:
      contacted → contacted_at = now()
      sent      → sent_at = now(), sent_by = operator_email, plus gift card fields
      redeemed  → redeemed_at = now()
      skipped   → no timestamp set
      to_contact → clears contacted_at/sent_at/redeemed_at (for undo)
    updated_at is always bumped. Notes, if provided, overwrite the existing value.
    """
    valid_statuses = {"to_contact", "contacted", "sent", "redeemed", "skipped"}
    if new_status not in valid_statuses:
        st.error(f"Invalid status: {new_status}")
        return False

    set_clauses = ["status = :status", "updated_at = now()"]
    params = {"outreach_id": outreach_id, "status": new_status}

    if operator_email:
        set_clauses.append("assigned_to = coalesce(assigned_to, :operator_email)")
        params["operator_email"] = operator_email

    if new_status == "contacted":
        set_clauses.append("contacted_at = coalesce(contacted_at, now())")
    elif new_status == "sent":
        set_clauses.append("sent_at = coalesce(sent_at, now())")
        if operator_email:
            set_clauses.append("sent_by = coalesce(sent_by, :operator_email)")
        if gift_card_code is not None:
            set_clauses.append("gift_card_code = :gift_card_code")
            params["gift_card_code"] = gift_card_code
        if gift_card_value is not None:
            set_clauses.append("gift_card_value = :gift_card_value")
            params["gift_card_value"] = gift_card_value
    elif new_status == "redeemed":
        set_clauses.append("redeemed_at = coalesce(redeemed_at, now())")
    elif new_status == "to_contact":
        set_clauses.extend([
            "contacted_at = null",
            "sent_at = null",
            "redeemed_at = null",
        ])

    if notes is not None:
        set_clauses.append("notes = :notes")
        params["notes"] = notes

    query = text(
        f"update analytics_ops.spin_wheel_winner_outreach "
        f"set {', '.join(set_clauses)} "
        f"where id = :outreach_id"
    )
    engine = get_database_connection()
    try:
        with engine.begin() as conn:
            conn.execute(query, params)
        return True
    except Exception as e:
        st.error(f"Error updating winner outreach: {str(e)}")
        return False


def update_winner_outreach_notes(outreach_id, notes):
    """Notes-only write for the inline notes field on each card."""
    engine = get_database_connection()
    query = text("""
        update analytics_ops.spin_wheel_winner_outreach
        set notes = :notes, updated_at = now()
        where id = :outreach_id
    """)
    try:
        with engine.begin() as conn:
            conn.execute(query, {"outreach_id": outreach_id, "notes": notes})
        return True
    except Exception as e:
        st.error(f"Error updating winner notes: {str(e)}")
        return False
