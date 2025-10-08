import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# Page configuration
st.set_page_config(
    page_title="DECK Analytics Dashboard",
    page_icon="=�",
    layout="wide"
)

# Title
st.title("=� DECK Analytics Dashboard")

# Database connection
@st.cache_resource
def get_database_connection():
    """Create database connection using Supabase credentials from secrets"""
    db_host = st.secrets["supabase"]["host"]
    db_port = st.secrets["supabase"]["port"]
    db_name = st.secrets["supabase"]["database"]
    db_user = st.secrets["supabase"]["user"]
    db_password = st.secrets["supabase"]["password"]

    connection_string = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)
    return engine

# Load data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_headline_metrics():
    """Load headline metrics from the analytics_prod_gold schema"""
    engine = get_database_connection()

    query = """
    SELECT
        metric_date,
        total_users,
        total_experience_cards,
        total_prompts,
        total_swipes
    FROM analytics_prod_gold.headline_metrics
    ORDER BY metric_date DESC
    LIMIT 1
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df

# Main dashboard
try:
    # Load the latest metrics
    metrics_df = load_headline_metrics()

    if not metrics_df.empty:
        # Display metric date
        metric_date = metrics_df['metric_date'].iloc[0]
        st.caption(f"=� Metrics as of: {metric_date}")

        # Display headline metrics in columns
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="=e Total Users",
                value=f"{metrics_df['total_users'].iloc[0]:,}"
            )

        with col2:
            st.metric(
                label="<� Total Experience Cards",
                value=f"{metrics_df['total_experience_cards'].iloc[0]:,}"
            )

        with col3:
            st.metric(
                label="=� Total Prompts",
                value=f"{metrics_df['total_prompts'].iloc[0]:,}"
            )

        with col4:
            st.metric(
                label="=F Total Swipes",
                value=f"{metrics_df['total_swipes'].iloc[0]:,}"
            )
    else:
        st.warning("� No metrics data available")

except Exception as e:
    st.error(f"L Error loading data: {str(e)}")
    st.info("=� Make sure your Supabase connection is configured correctly in .streamlit/secrets.toml")

# Refresh button
if st.button("= Refresh Data"):
    st.cache_data.clear()
    st.rerun()
