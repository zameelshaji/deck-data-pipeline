"""DECK Dashboard - Minimal Notion-Inspired Styling"""

import streamlit as st


# Notion-Inspired Color Palette - Minimal and Clean
BRAND_COLORS = {
    # Primary Colors - Subtle and Neutral
    'primary': '#37352F',           # Notion's main text color
    'primary_hover': '#2F2F2F',     # Slightly darker for hover
    'accent': '#2383E2',            # Notion's blue accent (used sparingly)

    # Background Colors
    'bg_main': '#FFFFFF',           # Pure white main background
    'bg_secondary': '#F7F6F3',      # Notion's light gray background
    'bg_hover': '#EFEFEF',          # Hover state background

    # Text Colors
    'text_primary': '#37352F',      # Main text - warm dark gray
    'text_secondary': '#787774',    # Secondary text - medium gray
    'text_tertiary': '#9B9A97',     # Tertiary/placeholder text

    # Border Colors
    'border': '#E9E9E7',            # Light border
    'border_dark': '#DFDFDE',       # Slightly darker border

    # Status Colors - Muted
    'success': '#0F7B6C',           # Muted green
    'warning': '#D9730D',           # Muted orange
    'error': '#E03E3E',             # Muted red
    'info': '#2383E2',              # Notion blue
}


def apply_deck_branding():
    """Apply minimal Notion-inspired CSS styling to the dashboard"""

    custom_css = f"""
    <style>
    /* Import Google Fonts - Inter for clean typography */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* Global Font - Clean and Minimal */
    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    /* Main Title - Simple and Clean */
    h1 {{
        color: {BRAND_COLORS['text_primary']} !important;
        font-weight: 600 !important;
        font-size: 2rem !important;
        letter-spacing: -0.03em;
        margin-bottom: 0.5rem !important;
    }}

    /* Subheaders - Minimal Styling */
    h2, h3 {{
        color: {BRAND_COLORS['text_primary']} !important;
        font-weight: 600 !important;
        font-size: 1.25rem !important;
        padding: 0;
        border: none;
        margin: 1.5rem 0 1rem 0 !important;
    }}

    h4 {{
        color: {BRAND_COLORS['text_primary']} !important;
        font-weight: 500 !important;
        font-size: 1rem !important;
        border: none;
        padding: 0;
        margin: 1rem 0 0.5rem 0 !important;
    }}

    /* Metric Value - Clean Numbers */
    [data-testid="stMetricValue"] {{
        font-size: 1.75rem !important;
        font-weight: 600 !important;
        color: {BRAND_COLORS['text_primary']} !important;
        letter-spacing: -0.02em;
    }}

    /* Metric Label - Subtle */
    [data-testid="stMetricLabel"] {{
        font-weight: 400 !important;
        font-size: 0.875rem !important;
        color: {BRAND_COLORS['text_secondary']} !important;
        text-transform: none;
    }}

    /* Metric Delta */
    [data-testid="stMetricDelta"] svg {{
        fill: {BRAND_COLORS['success']} !important;
    }}

    [data-testid="stMetricDelta"] {{
        color: {BRAND_COLORS['success']} !important;
        font-size: 0.8rem !important;
        font-weight: 500 !important;
    }}

    /* Button - Minimal and Functional */
    .stButton > button {{
        background-color: {BRAND_COLORS['bg_main']} !important;
        color: {BRAND_COLORS['text_primary']} !important;
        border: 1px solid {BRAND_COLORS['border_dark']} !important;
        border-radius: 4px !important;
        padding: 0.4rem 1rem !important;
        font-weight: 500 !important;
        font-size: 0.875rem !important;
        transition: background-color 0.1s ease !important;
        box-shadow: none !important;
    }}

    .stButton > button:hover {{
        background-color: {BRAND_COLORS['bg_hover']} !important;
        transform: none;
        box-shadow: none !important;
    }}

    .stButton > button:active {{
        background-color: {BRAND_COLORS['bg_secondary']} !important;
    }}

    /* Tab Styling - Clean Underline Style */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 0;
        border-bottom: 1px solid {BRAND_COLORS['border']} !important;
        background-color: transparent !important;
    }}

    .stTabs [data-baseweb="tab"] {{
        border-radius: 0;
        padding: 0.75rem 1rem;
        font-weight: 400;
        font-size: 0.875rem;
        border: none;
        background-color: transparent !important;
        color: {BRAND_COLORS['text_secondary']} !important;
    }}

    .stTabs [data-baseweb="tab"]:hover {{
        background-color: transparent !important;
        color: {BRAND_COLORS['text_primary']} !important;
    }}

    .stTabs [aria-selected="true"] {{
        background-color: transparent !important;
        color: {BRAND_COLORS['text_primary']} !important;
        font-weight: 500 !important;
        border-bottom: 2px solid {BRAND_COLORS['text_primary']} !important;
        margin-bottom: -1px;
    }}

    /* Divider - Subtle */
    hr {{
        border: none !important;
        height: 1px !important;
        background-color: {BRAND_COLORS['border']} !important;
        margin: 1.5rem 0 !important;
    }}

    /* Sidebar - Clean Background */
    [data-testid="stSidebar"] {{
        background-color: {BRAND_COLORS['bg_secondary']} !important;
        border-right: 1px solid {BRAND_COLORS['border']} !important;
    }}

    section[data-testid="stSidebar"] {{
        background-color: {BRAND_COLORS['bg_secondary']} !important;
    }}

    /* Sidebar Navigation - Minimal */
    [data-testid="stSidebarNav"] li a {{
        border-radius: 4px;
        transition: background-color 0.1s ease;
        padding: 0.4rem 0.75rem !important;
        margin: 0.1rem 0;
        font-weight: 400;
        font-size: 0.875rem;
        color: {BRAND_COLORS['text_secondary']} !important;
    }}

    [data-testid="stSidebarNav"] li a:hover {{
        background-color: {BRAND_COLORS['bg_hover']} !important;
        color: {BRAND_COLORS['text_primary']} !important;
    }}

    /* Active Page in Sidebar - Subtle Highlight */
    [data-testid="stSidebarNav"] li [aria-current="page"] {{
        background-color: {BRAND_COLORS['bg_hover']} !important;
        color: {BRAND_COLORS['text_primary']} !important;
        font-weight: 500;
    }}

    /* Metric Container - Minimal Card */
    [data-testid="metric-container"] {{
        background-color: {BRAND_COLORS['bg_main']};
        padding: 1rem;
        border-radius: 4px;
        border: 1px solid {BRAND_COLORS['border']};
        transition: none;
        box-shadow: none;
    }}

    [data-testid="metric-container"]:hover {{
        border-color: {BRAND_COLORS['border_dark']};
        box-shadow: none;
        transform: none;
    }}

    /* Links - Subtle */
    a {{
        color: {BRAND_COLORS['accent']} !important;
        text-decoration: none !important;
        font-weight: 400;
    }}

    a:hover {{
        color: {BRAND_COLORS['accent']} !important;
        text-decoration: underline !important;
    }}

    /* Alert Boxes - Minimal */
    .stAlert {{
        border-radius: 4px;
        border-left: 3px solid {BRAND_COLORS['info']};
        background-color: {BRAND_COLORS['bg_secondary']} !important;
    }}

    /* Success Message */
    .stSuccess {{
        background-color: #F0FDF4 !important;
        border-left-color: {BRAND_COLORS['success']} !important;
    }}

    /* Warning Message */
    .stWarning {{
        background-color: #FFFBEB !important;
        border-left-color: {BRAND_COLORS['warning']} !important;
    }}

    /* Error Message */
    .stError {{
        background-color: #FEF2F2 !important;
        border-left-color: {BRAND_COLORS['error']} !important;
    }}

    /* Info Message */
    .stInfo {{
        background-color: #EFF6FF !important;
        border-left-color: {BRAND_COLORS['info']} !important;
    }}

    /* Loading Spinner */
    .stSpinner > div {{
        border-top-color: {BRAND_COLORS['text_secondary']} !important;
    }}

    /* Code Blocks */
    code {{
        background-color: {BRAND_COLORS['bg_secondary']} !important;
        color: {BRAND_COLORS['text_primary']} !important;
        padding: 0.125rem 0.25rem;
        border-radius: 3px;
        font-size: 0.875em;
        font-weight: 400;
    }}

    /* Remove Streamlit Branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}

    /* Main App Background */
    .stApp {{
        background-color: {BRAND_COLORS['bg_main']} !important;
    }}

    /* Selectbox Styling */
    [data-baseweb="select"] {{
        background-color: {BRAND_COLORS['bg_main']} !important;
    }}

    [data-baseweb="select"] > div {{
        background-color: {BRAND_COLORS['bg_main']} !important;
        color: {BRAND_COLORS['text_primary']} !important;
        border-color: {BRAND_COLORS['border']} !important;
        border-radius: 4px !important;
        font-size: 0.875rem !important;
    }}

    [data-baseweb="select"]:hover > div {{
        border-color: {BRAND_COLORS['border_dark']} !important;
    }}

    [data-baseweb="popover"] {{
        background-color: {BRAND_COLORS['bg_main']} !important;
        border: 1px solid {BRAND_COLORS['border']} !important;
        border-radius: 4px !important;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08) !important;
    }}

    [data-baseweb="menu"] {{
        background-color: {BRAND_COLORS['bg_main']} !important;
    }}

    [data-baseweb="menu"] li {{
        background-color: {BRAND_COLORS['bg_main']} !important;
        color: {BRAND_COLORS['text_primary']} !important;
        font-size: 0.875rem !important;
    }}

    [data-baseweb="menu"] li:hover {{
        background-color: {BRAND_COLORS['bg_secondary']} !important;
    }}

    /* Input Fields */
    input, textarea, [data-baseweb="input"] {{
        background-color: {BRAND_COLORS['bg_main']} !important;
        color: {BRAND_COLORS['text_primary']} !important;
        border-color: {BRAND_COLORS['border']} !important;
        border-radius: 4px !important;
        font-size: 0.875rem !important;
    }}

    /* Caption styling */
    .stCaption {{
        color: {BRAND_COLORS['text_tertiary']} !important;
        font-size: 0.75rem !important;
    }}

    /* Dataframe Styling - Clean Table */
    .stDataFrame {{
        background-color: {BRAND_COLORS['bg_main']} !important;
    }}

    [data-testid="stDataFrame"] {{
        background-color: {BRAND_COLORS['bg_main']} !important;
        border: 1px solid {BRAND_COLORS['border']} !important;
        border-radius: 4px !important;
    }}

    .stDataFrame thead tr th {{
        background-color: {BRAND_COLORS['bg_secondary']} !important;
        color: {BRAND_COLORS['text_primary']} !important;
        font-weight: 500 !important;
        font-size: 0.8125rem !important;
        border-bottom: 1px solid {BRAND_COLORS['border']} !important;
        padding: 0.5rem 0.75rem !important;
    }}

    .stDataFrame tbody tr {{
        background-color: {BRAND_COLORS['bg_main']} !important;
    }}

    .stDataFrame tbody tr:nth-child(even) {{
        background-color: {BRAND_COLORS['bg_main']} !important;
    }}

    .stDataFrame tbody tr:hover {{
        background-color: {BRAND_COLORS['bg_secondary']} !important;
    }}

    .stDataFrame tbody tr td {{
        color: {BRAND_COLORS['text_primary']} !important;
        border-bottom: 1px solid {BRAND_COLORS['border']} !important;
        font-size: 0.8125rem !important;
        padding: 0.5rem 0.75rem !important;
    }}

    /* Download Button - Minimal */
    .stDownloadButton > button {{
        background-color: {BRAND_COLORS['bg_main']} !important;
        color: {BRAND_COLORS['text_primary']} !important;
        border: 1px solid {BRAND_COLORS['border_dark']} !important;
        border-radius: 4px !important;
        padding: 0.4rem 1rem !important;
        font-weight: 500 !important;
        font-size: 0.875rem !important;
    }}

    .stDownloadButton > button:hover {{
        background-color: {BRAND_COLORS['bg_hover']} !important;
        transform: none;
    }}

    /* Expander - Clean */
    .streamlit-expanderHeader {{
        font-weight: 500 !important;
        font-size: 0.875rem !important;
        color: {BRAND_COLORS['text_primary']} !important;
        background-color: {BRAND_COLORS['bg_secondary']} !important;
        border-radius: 4px !important;
    }}

    /* Plotly Charts - Remove heavy styling */
    .js-plotly-plot {{
        border: none !important;
        padding: 0 !important;
    }}

    /* Column gap adjustments */
    [data-testid="column"] {{
        padding: 0 0.5rem !important;
    }}

    /* Remove extra padding */
    .block-container {{
        padding-top: 2rem !important;
        padding-bottom: 2rem !important;
    }}
    </style>
    """

    st.markdown(custom_css, unsafe_allow_html=True)


def add_deck_header():
    """Add minimal DECK header to the page"""
    st.markdown(f"""
        <div style="
            padding: 0 0 1rem 0;
            margin-bottom: 1.5rem;
            border-bottom: 1px solid {BRAND_COLORS['border']};
        ">
            <h1 style="
                margin: 0;
                color: {BRAND_COLORS['text_primary']};
                font-size: 1.5rem;
                font-weight: 600;
                letter-spacing: -0.02em;
            ">
                DECK Analytics
            </h1>
            <p style="
                margin: 0.25rem 0 0 0;
                color: {BRAND_COLORS['text_tertiary']};
                font-size: 0.875rem;
                font-weight: 400;
            ">
                Performance Dashboard
            </p>
        </div>
    """, unsafe_allow_html=True)


def add_deck_footer():
    """Add minimal DECK footer to the page"""
    st.markdown(f"""
        <div style="
            margin-top: 3rem;
            padding-top: 1.5rem;
            border-top: 1px solid {BRAND_COLORS['border']};
            text-align: center;
            color: {BRAND_COLORS['text_tertiary']};
        ">
            <p style="margin: 0; font-size: 0.75rem; font-weight: 400;">
                DECK Analytics &middot; {chr(169)} 2025
            </p>
        </div>
    """, unsafe_allow_html=True)


def create_metric_card(label, value, delta=None, help_text=None):
    """Create a minimal styled metric card"""
    delta_html = ""
    if delta is not None:
        delta_color = BRAND_COLORS['success'] if isinstance(delta, (int, float)) and delta >= 0 else BRAND_COLORS['error']
        delta_html = f"""
            <div style="
                color: {delta_color};
                font-size: 0.8rem;
                font-weight: 500;
                margin-top: 0.25rem;
            ">
                {delta}
            </div>
        """

    help_html = ""
    if help_text:
        help_html = f"""
            <div style="
                color: {BRAND_COLORS['text_tertiary']};
                font-size: 0.75rem;
                margin-top: 0.5rem;
            ">
                {help_text}
            </div>
        """

    return f"""
        <div style="
            background: {BRAND_COLORS['bg_main']};
            padding: 1rem;
            border-radius: 4px;
            border: 1px solid {BRAND_COLORS['border']};
            height: 100%;
        ">
            <div style="
                color: {BRAND_COLORS['text_secondary']};
                font-size: 0.8125rem;
                font-weight: 400;
                margin-bottom: 0.25rem;
            ">
                {label}
            </div>
            <div style="
                color: {BRAND_COLORS['text_primary']};
                font-size: 1.5rem;
                font-weight: 600;
                letter-spacing: -0.02em;
            ">
                {value}
            </div>
            {delta_html}
            {help_html}
        </div>
    """
