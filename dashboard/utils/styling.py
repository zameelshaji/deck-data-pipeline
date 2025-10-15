"""DECK Dashboard - Custom Styling and Branding"""

import streamlit as st


# DECK Brand Colors
BRAND_COLORS = {
    # Primary Brand Colors
    'primary': '#E91E8C',           # DECK Magenta/Pink
    'primary_dark': '#C7177A',      # Darker Pink (hover states)
    'primary_light': '#FF4FA3',     # Lighter Pink (accents)

    # Light Mode Colors
    'light_bg': '#FFFFFF',
    'light_secondary_bg': '#F5F5F5',
    'light_text': '#1A1A1A',
    'light_text_secondary': '#6B7280',

    # Dark Mode Colors
    'dark_bg': '#0E1117',
    'dark_secondary_bg': '#262730',
    'dark_text': '#FAFAFA',
    'dark_text_secondary': '#A3A3A3',

    # Status Colors
    'success': '#10B981',
    'warning': '#F59E0B',
    'error': '#EF4444',
    'info': '#3B82F6',
}


def apply_deck_branding():
    """Apply DECK custom CSS styling to the dashboard"""

    custom_css = f"""
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* Global Font */
    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }}

    /* Main Title Styling - BOLD PINK */
    h1 {{
        color: {BRAND_COLORS['primary']} !important;
        font-weight: 800 !important;
        letter-spacing: -0.02em;
        text-shadow: 2px 2px 4px rgba(233, 30, 140, 0.2);
        font-size: 3rem !important;
    }}

    /* Subheader Styling - BOLD PINK */
    h2, h3 {{
        color: {BRAND_COLORS['primary']} !important;
        font-weight: 700 !important;
        background: linear-gradient(90deg, {BRAND_COLORS['primary']}10 0%, transparent 100%);
        padding: 0.5rem 1rem;
        border-left: 5px solid {BRAND_COLORS['primary']};
        border-radius: 4px;
        margin: 1rem 0 !important;
    }}

    h4 {{
        color: {BRAND_COLORS['primary']} !important;
        font-weight: 600 !important;
        border-bottom: 2px solid {BRAND_COLORS['primary']};
        padding-bottom: 0.3rem;
    }}

    /* Metric Value Styling - BOLD PINK NUMBERS */
    [data-testid="stMetricValue"] {{
        font-size: 2.5rem !important;
        font-weight: 800 !important;
        color: {BRAND_COLORS['primary']} !important;
        text-shadow: 1px 1px 2px rgba(233, 30, 140, 0.15);
    }}

    /* Metric Label Styling */
    [data-testid="stMetricLabel"] {{
        font-weight: 700 !important;
        font-size: 1rem !important;
        color: {BRAND_COLORS['light_text']} !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}

    /* Metric Delta Positive */
    [data-testid="stMetricDelta"] svg {{
        fill: {BRAND_COLORS['success']} !important;
    }}

    [data-testid="stMetricDelta"] {{
        color: {BRAND_COLORS['success']} !important;
    }}

    /* Button Styling - VIBRANT PINK */
    .stButton > button {{
        background: linear-gradient(135deg, {BRAND_COLORS['primary']} 0%, {BRAND_COLORS['primary_dark']} 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.75rem 2rem !important;
        font-weight: 700 !important;
        font-size: 1.1rem !important;
        text-transform: uppercase;
        letter-spacing: 1px;
        transition: all 0.3s ease !important;
        box-shadow: 0 4px 15px rgba(233, 30, 140, 0.4) !important;
    }}

    .stButton > button:hover {{
        background: linear-gradient(135deg, {BRAND_COLORS['primary_dark']} 0%, {BRAND_COLORS['primary']} 100%) !important;
        transform: translateY(-3px) scale(1.02);
        box-shadow: 0 8px 25px rgba(233, 30, 140, 0.6) !important;
    }}

    .stButton > button:active {{
        transform: translateY(-1px) scale(0.98);
    }}

    /* Tab Styling - BOLD PINK TABS */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 12px;
        border-bottom: 3px solid {BRAND_COLORS['primary']} !important;
    }}

    .stTabs [data-baseweb="tab"] {{
        border-radius: 12px 12px 0 0;
        padding: 0.75rem 2rem;
        font-weight: 700;
        font-size: 1.05rem;
        border: 2px solid transparent;
        transition: all 0.3s ease;
    }}

    .stTabs [data-baseweb="tab"]:hover {{
        background-color: {BRAND_COLORS['primary']}15 !important;
        border-color: {BRAND_COLORS['primary']}50 !important;
    }}

    .stTabs [aria-selected="true"] {{
        background: linear-gradient(180deg, {BRAND_COLORS['primary']} 0%, {BRAND_COLORS['primary_dark']} 100%) !important;
        color: white !important;
        border-color: {BRAND_COLORS['primary']} !important;
        box-shadow: 0 -3px 10px rgba(233, 30, 140, 0.3) !important;
        transform: translateY(-2px);
    }}

    /* Divider Styling - BOLD PINK LINE */
    hr {{
        border: none !important;
        height: 4px !important;
        background: linear-gradient(90deg, {BRAND_COLORS['primary']} 0%, {BRAND_COLORS['primary_light']} 50%, {BRAND_COLORS['primary']} 100%) !important;
        border-radius: 2px;
        margin: 2rem 0 !important;
        box-shadow: 0 2px 8px rgba(233, 30, 140, 0.3);
    }}

    /* Sidebar Styling - PINK ACCENT SIDEBAR */
    [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, {BRAND_COLORS['primary']}08 0%, {BRAND_COLORS['light_secondary_bg']} 20%, {BRAND_COLORS['light_secondary_bg']} 80%, {BRAND_COLORS['primary']}08 100%) !important;
        border-right: 4px solid {BRAND_COLORS['primary']} !important;
    }}

    section[data-testid="stSidebar"] {{
        background: linear-gradient(180deg, {BRAND_COLORS['primary']}08 0%, {BRAND_COLORS['light_secondary_bg']} 20%, {BRAND_COLORS['light_secondary_bg']} 80%, {BRAND_COLORS['primary']}08 100%) !important;
    }}

    section[data-testid="stSidebar"] > div {{
        background-color: transparent !important;
    }}

    /* Sidebar Navigation - PINK HIGHLIGHTS */
    [data-testid="stSidebarNav"] li a {{
        border-radius: 12px;
        transition: all 0.3s ease;
        padding: 0.75rem 1rem !important;
        margin: 0.3rem 0;
        font-weight: 600;
        border-left: 4px solid transparent;
    }}

    [data-testid="stSidebarNav"] li a:hover {{
        background: linear-gradient(90deg, {BRAND_COLORS['primary']}25 0%, {BRAND_COLORS['primary']}10 100%) !important;
        border-left: 4px solid {BRAND_COLORS['primary']} !important;
        transform: translateX(5px);
        box-shadow: 0 2px 8px rgba(233, 30, 140, 0.2);
    }}

    /* Active Page in Sidebar - BOLD PINK */
    [data-testid="stSidebarNav"] li [aria-current="page"] {{
        background: linear-gradient(135deg, {BRAND_COLORS['primary']} 0%, {BRAND_COLORS['primary_dark']} 100%) !important;
        color: white !important;
        font-weight: 700;
        border-left: 4px solid {BRAND_COLORS['primary_light']} !important;
        box-shadow: 0 4px 12px rgba(233, 30, 140, 0.4) !important;
        transform: translateX(8px);
    }}

    /* Card Styling for Metrics - PINK BORDERS */
    [data-testid="metric-container"] {{
        background: linear-gradient(135deg, white 0%, {BRAND_COLORS['light_secondary_bg']} 100%);
        padding: 1.5rem;
        border-radius: 16px;
        border: 3px solid {BRAND_COLORS['primary']}30;
        border-left: 6px solid {BRAND_COLORS['primary']};
        transition: all 0.3s ease;
        box-shadow: 0 2px 8px rgba(233, 30, 140, 0.1);
    }}

    [data-testid="metric-container"]:hover {{
        border-color: {BRAND_COLORS['primary']};
        border-left-color: {BRAND_COLORS['primary_light']};
        box-shadow: 0 8px 24px rgba(233, 30, 140, 0.25);
        transform: translateY(-4px) scale(1.02);
        background: linear-gradient(135deg, {BRAND_COLORS['primary']}05 0%, white 100%);
    }}

    /* Chart Container Styling - PINK FRAME (minimal to not break charts) */
    .js-plotly-plot {{
        border-left: 5px solid {BRAND_COLORS['primary']};
        padding: 1rem;
    }}

    /* Links - BOLD PINK */
    a {{
        color: {BRAND_COLORS['primary']} !important;
        text-decoration: none !important;
        font-weight: 700;
        transition: all 0.2s ease;
        border-bottom: 2px solid transparent;
    }}

    a:hover {{
        color: {BRAND_COLORS['primary_dark']} !important;
        border-bottom: 2px solid {BRAND_COLORS['primary']} !important;
        text-shadow: 0 0 8px rgba(233, 30, 140, 0.3);
    }}

    /* Warning/Info Boxes */
    .stAlert {{
        border-radius: 8px;
        border-left: 4px solid {BRAND_COLORS['primary']};
    }}

    /* Success Message */
    .stSuccess {{
        background-color: {BRAND_COLORS['success']}15;
        border-left-color: {BRAND_COLORS['success']} !important;
    }}

    /* Warning Message */
    .stWarning {{
        background-color: {BRAND_COLORS['warning']}15;
        border-left-color: {BRAND_COLORS['warning']} !important;
    }}

    /* Error Message */
    .stError {{
        background-color: {BRAND_COLORS['error']}15;
        border-left-color: {BRAND_COLORS['error']} !important;
    }}

    /* Info Message */
    .stInfo {{
        background-color: {BRAND_COLORS['info']}15;
        border-left-color: {BRAND_COLORS['info']} !important;
    }}

    /* Footer Styling */
    .footer {{
        text-align: center;
        padding: 2rem 0;
        color: {BRAND_COLORS['light_text_secondary']};
        font-size: 0.9rem;
    }}

    /* Loading Spinner */
    .stSpinner > div {{
        border-top-color: {BRAND_COLORS['primary']} !important;
    }}

    /* Dataframe Styling */
    .dataframe {{
        border-radius: 8px !important;
    }}

    /* Code Blocks */
    code {{
        background-color: {BRAND_COLORS['light_secondary_bg']} !important;
        color: {BRAND_COLORS['primary']} !important;
        padding: 0.2rem 0.4rem;
        border-radius: 4px;
        font-weight: 500;
    }}

    /* Header Enhancement - PINK GRADIENT */
    header[data-testid="stHeader"] {{
        background: linear-gradient(135deg, {BRAND_COLORS['primary']}15 0%, {BRAND_COLORS['primary']}05 50%, transparent 100%);
        border-bottom: 3px solid {BRAND_COLORS['primary']}30;
        box-shadow: 0 2px 12px rgba(233, 30, 140, 0.1);
    }}

    /* Remove Streamlit Branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}

    /* Custom Footer */
    .custom-footer {{
        position: fixed;
        bottom: 0;
        left: 0;
        width: 100%;
        background: {BRAND_COLORS['light_secondary_bg']};
        padding: 0.5rem;
        text-align: center;
        font-size: 0.85rem;
        color: {BRAND_COLORS['light_text_secondary']};
        border-top: 2px solid {BRAND_COLORS['primary']};
        z-index: 999;
    }}

    /* Force Light Mode - Override Dark Mode with SUBTLE PINK ACCENTS */
    .stApp {{
        background: linear-gradient(180deg, {BRAND_COLORS['primary']}02 0%, {BRAND_COLORS['light_bg']} 5%, {BRAND_COLORS['light_bg']} 95%, {BRAND_COLORS['primary']}02 100%) !important;
    }}

    [data-testid="stAppViewContainer"] {{
        background-color: transparent !important;
    }}

    /* Force all text to light mode colors - but keep headings pink */
    p, span, label, [data-testid="stMarkdownContainer"] p {{
        color: {BRAND_COLORS['light_text']} !important;
    }}

    /* Ensure white backgrounds on all containers */
    .element-container, .stBlock, [data-testid="block-container"] {{
        background-color: transparent !important;
    }}

    /* Fix Selectbox (Dropdown) Styling */
    [data-baseweb="select"] {{
        background-color: {BRAND_COLORS['light_bg']} !important;
    }}

    [data-baseweb="popover"] {{
        background-color: {BRAND_COLORS['light_bg']} !important;
    }}

    [data-baseweb="menu"] {{
        background-color: {BRAND_COLORS['light_bg']} !important;
    }}

    [data-baseweb="menu"] li {{
        background-color: {BRAND_COLORS['light_bg']} !important;
        color: {BRAND_COLORS['light_text']} !important;
    }}

    [data-baseweb="menu"] li:hover {{
        background-color: {BRAND_COLORS['primary']}15 !important;
    }}

    /* Fix Input Fields */
    input, textarea, [data-baseweb="input"] {{
        background-color: {BRAND_COLORS['light_bg']} !important;
        color: {BRAND_COLORS['light_text']} !important;
    }}

    /* MINIMAL Plotly styling - only backgrounds, no trace interference */

    /* Fix chart container height */
    .stPlotlyChart {{
        width: 100% !important;
    }}

    /* Caption styling */
    .stCaption {{
        color: {BRAND_COLORS['light_text_secondary']} !important;
    }}

    /* Fix dataframe backgrounds */
    .stDataFrame {{
        background-color: {BRAND_COLORS['light_bg']} !important;
    }}

    [data-testid="stDataFrame"] {{
        background-color: {BRAND_COLORS['light_bg']} !important;
    }}

    /* Fix dataframe table headers - PINK */
    .stDataFrame thead tr th {{
        background-color: {BRAND_COLORS['primary']} !important;
        color: white !important;
        font-weight: 700 !important;
        border-bottom: 3px solid {BRAND_COLORS['primary_dark']} !important;
    }}

    /* Fix dataframe rows */
    .stDataFrame tbody tr {{
        background-color: white !important;
    }}

    .stDataFrame tbody tr:nth-child(even) {{
        background-color: {BRAND_COLORS['light_secondary_bg']} !important;
    }}

    .stDataFrame tbody tr:hover {{
        background-color: {BRAND_COLORS['primary']}10 !important;
    }}

    .stDataFrame tbody tr td {{
        color: {BRAND_COLORS['light_text']} !important;
        border-bottom: 1px solid {BRAND_COLORS['light_secondary_bg']} !important;
    }}

    /* Fix selectbox dropdown options */
    [data-baseweb="select"] > div {{
        background-color: {BRAND_COLORS['light_bg']} !important;
        color: {BRAND_COLORS['light_text']} !important;
        border-color: {BRAND_COLORS['primary']}40 !important;
    }}

    [data-baseweb="select"]:hover > div {{
        border-color: {BRAND_COLORS['primary']} !important;
    }}

    /* Fix all text inputs and selects */
    .stSelectbox > div > div {{
        background-color: {BRAND_COLORS['light_bg']} !important;
        color: {BRAND_COLORS['light_text']} !important;
    }}

    /* Fix download button */
    .stDownloadButton > button {{
        background: linear-gradient(135deg, {BRAND_COLORS['success']} 0%, #059669 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.75rem 2rem !important;
        font-weight: 700 !important;
        box-shadow: 0 4px 15px rgba(16, 185, 129, 0.4) !important;
    }}

    .stDownloadButton > button:hover {{
        background: linear-gradient(135deg, #059669 0%, {BRAND_COLORS['success']} 100%) !important;
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(16, 185, 129, 0.5) !important;
    }}
    </style>
    """

    st.markdown(custom_css, unsafe_allow_html=True)


def add_deck_header():
    """Add DECK branded header to the page"""
    st.markdown(f"""
        <div style="
            padding: 1rem 0;
            margin-bottom: 1rem;
            border-bottom: 3px solid {BRAND_COLORS['primary']};
        ">
            <h1 style="
                margin: 0;
                color: {BRAND_COLORS['primary']};
                font-size: 2.5rem;
                font-weight: 700;
            ">
                🎴 DECK Analytics
            </h1>
            <p style="
                margin: 0.5rem 0 0 0;
                color: {BRAND_COLORS['light_text_secondary']};
                font-size: 1rem;
            ">
                Insights & Performance Dashboard
            </p>
        </div>
    """, unsafe_allow_html=True)


def add_deck_footer():
    """Add DECK branded footer to the page"""
    st.markdown(f"""
        <div style="
            margin-top: 3rem;
            padding-top: 2rem;
            border-top: 2px solid {BRAND_COLORS['primary']}40;
            text-align: center;
            color: {BRAND_COLORS['light_text_secondary']};
        ">
            <p style="margin: 0.5rem 0; font-weight: 600; color: {BRAND_COLORS['primary']};">
                🎴 DECK Analytics Dashboard
            </p>
            <p style="margin: 0.5rem 0; font-size: 0.9rem;">
                Powered by Streamlit | &copy; 2025 DECK
            </p>
        </div>
    """, unsafe_allow_html=True)


def create_metric_card(label, value, delta=None, help_text=None):
    """Create a styled metric card with DECK branding"""
    delta_html = ""
    if delta is not None:
        delta_color = BRAND_COLORS['success'] if isinstance(delta, (int, float)) and delta >= 0 else BRAND_COLORS['error']
        delta_html = f"""
            <div style="
                color: {delta_color};
                font-size: 0.9rem;
                font-weight: 600;
                margin-top: 0.5rem;
            ">
                {delta}
            </div>
        """

    help_html = ""
    if help_text:
        help_html = f"""
            <div style="
                color: {BRAND_COLORS['light_text_secondary']};
                font-size: 0.85rem;
                margin-top: 0.5rem;
                font-style: italic;
            ">
                {help_text}
            </div>
        """

    return f"""
        <div style="
            background: {BRAND_COLORS['light_secondary_bg']};
            padding: 1.5rem;
            border-radius: 12px;
            border: 2px solid {BRAND_COLORS['primary']}20;
            transition: all 0.2s ease;
            height: 100%;
        ">
            <div style="
                color: {BRAND_COLORS['light_text']};
                font-size: 0.95rem;
                font-weight: 600;
                margin-bottom: 0.5rem;
            ">
                {label}
            </div>
            <div style="
                color: {BRAND_COLORS['primary']};
                font-size: 2rem;
                font-weight: 700;
            ">
                {value}
            </div>
            {delta_html}
            {help_html}
        </div>
    """
