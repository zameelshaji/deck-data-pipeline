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
    FROM analytics_prod_gold.executive_summary
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
        FROM analytics_prod_gold.headline_metrics
        WHERE metric_date >= current_date - interval '{days} days'
        ORDER BY metric_date DESC
        """
    else:
        query = """
        SELECT *
        FROM analytics_prod_gold.headline_metrics
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
    FROM analytics_prod_gold.daily_active_users
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
    FROM analytics_prod_gold.weekly_active_users
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
    FROM analytics_prod_gold.monthly_active_users
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
    FROM analytics_prod_gold.user_acquisition_funnel
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
    FROM analytics_prod_gold.dextr_performance
    WHERE query_date >= current_date - interval '{days} days'
    ORDER BY query_date DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


@st.cache_data(ttl=300)
def load_supplier_performance():
    """Load supplier performance data"""
    engine = get_database_connection()

    query = """
    SELECT *
    FROM analytics_prod_gold.supplier_performance
    ORDER BY engagement_score DESC, total_impressions DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


@st.cache_data(ttl=300)
def load_content_performance():
    """Load content performance data"""
    engine = get_database_connection()

    query = """
    SELECT *
    FROM analytics_prod_gold.content_performance
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
    FROM analytics_prod_gold.monthly_active_users
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
    FROM analytics_prod_gold.weekly_active_users
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
    """Load total multiplayer sessions to date"""
    engine = get_database_connection()

    query = """
    SELECT COUNT(*) as total_multiplayer_sessions
    FROM analytics_prod_silver.stg_multiplayer 
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
    """Load referral metrics for Home page"""
    engine = get_database_connection()

    query = """
    SELECT
        COALESCE(COUNT(DISTINCT referred_user_id), 0) as total_referrals_made
    FROM analytics_prod_silver.stg_referral_relationships
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
    """
    Load monthly user cohort retention data

    Args:
        months: Number of months to look back for cohorts (default 12)

    Returns:
        DataFrame with monthly cohort retention rates
    """
    engine = get_database_connection()

    query = f"""
    SELECT
        cohort_month,
        cohort_size,
        months_since_signup,
        month_label,
        users_active,
        retention_rate
    FROM analytics_prod_gold.user_cohort_retention_monthly
    WHERE cohort_month >= current_date - interval '{months} months'
        AND months_since_signup <= 12  -- Show up to 12 months of retention
    ORDER BY cohort_month DESC, months_since_signup ASC
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading monthly cohort retention: {str(e)}")
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

    # Unique active planners needs a separate query against fct_session_outcomes
    # because distinct user counts can't be summed from daily aggregates
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

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
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
def load_monthly_retention_summary_metrics():
    """Load summary metrics for monthly retention performance"""
    engine = get_database_connection()

    query = """
    WITH recent_cohorts AS (
        -- Get cohorts from last 6 months with sufficient data
        SELECT
            cohort_month,
            cohort_size,
            months_since_signup,
            retention_rate
        FROM analytics_prod_gold.user_cohort_retention_monthly
        WHERE cohort_month >= current_date - interval '6 months'
            AND cohort_month < current_date - interval '1 month'  -- Exclude current month
            AND months_since_signup IN (1, 2, 3, 6)  -- Month 1, 2, 3, and 6 retention
    )

    SELECT
        -- Average retention rates
        AVG(CASE WHEN months_since_signup = 1 THEN retention_rate END) as avg_month1_retention,
        AVG(CASE WHEN months_since_signup = 2 THEN retention_rate END) as avg_month2_retention,
        AVG(CASE WHEN months_since_signup = 3 THEN retention_rate END) as avg_month3_retention,
        AVG(CASE WHEN months_since_signup = 6 THEN retention_rate END) as avg_month6_retention,

        -- Best performing cohort
        MAX(CASE WHEN months_since_signup = 1 THEN retention_rate END) as best_month1_retention,
        MAX(CASE WHEN months_since_signup = 3 THEN retention_rate END) as best_month3_retention,

        -- Cohort sizes
        AVG(cohort_size) as avg_cohort_size,
        SUM(CASE WHEN months_since_signup = 1 THEN cohort_size END) as total_users_analyzed,

        -- Retention trend (comparing early vs later cohorts)
        AVG(CASE
            WHEN months_since_signup = 1
                AND cohort_month >= current_date - interval '3 months'
            THEN retention_rate
        END) as recent_month1_retention,

        AVG(CASE
            WHEN months_since_signup = 1
                AND cohort_month < current_date - interval '3 months'
                AND cohort_month >= current_date - interval '6 months'
            THEN retention_rate
        END) as older_month1_retention

    FROM recent_cohorts
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading monthly retention summary: {str(e)}")
        return pd.DataFrame()
