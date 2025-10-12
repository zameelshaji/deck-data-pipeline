"""Database connection utilities for DECK Analytics Dashboard"""

import streamlit as st
from sqlalchemy import create_engine


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
