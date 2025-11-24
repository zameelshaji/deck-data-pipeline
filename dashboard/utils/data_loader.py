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
        COALESCE(COUNT(DISTINCT referrer_used_id), 0) as total_referrals_given,
        COALESCE(COUNT(DISTINCT referred_user_id), 0) as total_referrals_claimed
    FROM analytics_prod_silver.stg_referral_relationships
    """

    try:
        with engine.begin() as conn:
            df = pd.read_sql(text(query), conn)
        return df
    except Exception as e:
        st.error(f"Error loading referral metrics: {str(e)}")
        return pd.DataFrame({'total_referrals_given': [0], 'total_referrals_claimed': [0]})
