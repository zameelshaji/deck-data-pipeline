"""Data loading functions for DECK Analytics Dashboard"""

import pandas as pd
import streamlit as st
from sqlalchemy import text
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
           total_active_users as value,
           wow_growth_percent as delta
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
    SELECT activity_date, total_active_users
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
