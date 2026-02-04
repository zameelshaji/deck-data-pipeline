"""DECK Analytics Dashboard - Monthly Summary Page"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.data_loader import (
    load_monthly_active_users,
    load_headline_metrics,
    load_user_acquisition_funnel
)
from utils.visualizations import (
    create_line_chart,
    create_bar_chart
)
from utils.styling import apply_deck_branding, add_deck_footer

# Page configuration
st.set_page_config(
    page_title="DECK Analytics - Monthly Summary",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("📊 Monthly Summary")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

try:
    # Load monthly data
    mau_data = load_monthly_active_users(months=12)
    headline_data = load_headline_metrics(days=365)  # Load a year of daily metrics
    funnel_data = load_user_acquisition_funnel(days=365)  # Load signup data

    if mau_data.empty:
        st.warning("⚠️ No monthly data available")
        st.stop()

    # Convert activity_month to datetime for proper filtering
    mau_data['activity_month'] = pd.to_datetime(mau_data['activity_month'])

    # Aggregate headline metrics by month
    if not headline_data.empty:
        headline_data['metric_date'] = pd.to_datetime(headline_data['metric_date'])
        headline_data['month'] = headline_data['metric_date'].dt.to_period('M')

        # For each month, get the max value (end of month) minus min value (start of month)
        monthly_headline_list = []
        for month in headline_data['month'].unique():
            month_data = headline_data[headline_data['month'] == month].sort_values('metric_date')
            if len(month_data) > 1:
                prompts_in_month = month_data['total_prompts'].iloc[-1] - month_data['total_prompts'].iloc[0]
                swipes_in_month = month_data['total_swipes'].iloc[-1] - month_data['total_swipes'].iloc[0]
            else:
                prompts_in_month = month_data['total_prompts'].iloc[0]
                swipes_in_month = month_data['total_swipes'].iloc[0]

            monthly_headline_list.append({
                'month': month,
                'total_prompts': prompts_in_month,
                'total_swipes': swipes_in_month
            })

        monthly_headline = pd.DataFrame(monthly_headline_list)
    else:
        monthly_headline = pd.DataFrame()

    # Aggregate signups by month
    if not funnel_data.empty:
        funnel_data['signup_date'] = pd.to_datetime(funnel_data['signup_date'])
        funnel_data['month'] = funnel_data['signup_date'].dt.to_period('M')
        monthly_signups = funnel_data.groupby('month').agg({
            'signups': 'sum'
        }).reset_index()
    else:
        monthly_signups = pd.DataFrame()

    # ============================================
    # MONTH FILTER
    # ============================================
    st.subheader("🔍 Filter by Month")

    # Create month options for selectbox
    available_months = mau_data['activity_month'].dt.to_period('M').unique()
    available_months = sorted(available_months, reverse=True)
    month_options = [period.to_timestamp() for period in available_months]

    # Format function for displaying months
    def format_month(date):
        return date.strftime('%B %Y')

    selected_month = st.selectbox(
        "Select Month",
        options=month_options,
        format_func=format_month,
        index=0
    )

    # Filter data for selected month
    selected_month_data = mau_data[
        mau_data['activity_month'].dt.to_period('M') == pd.Period(selected_month, freq='M')
    ]

    if selected_month_data.empty:
        st.warning(f"⚠️ No data available for {format_month(selected_month)}")
        st.stop()

    # Get the selected month's data (should be single row)
    month_metrics = selected_month_data.iloc[0]

    st.divider()

    # Get additional metrics for selected month
    selected_period = pd.Period(selected_month, freq='M')

    # Get signups for selected month
    month_signups = 0
    if not monthly_signups.empty and selected_period in monthly_signups['month'].values:
        month_signups = monthly_signups[monthly_signups['month'] == selected_period]['signups'].iloc[0]

    # Get prompts and swipes for selected month
    month_prompts = 0
    month_swipes = 0
    if not monthly_headline.empty and selected_period in monthly_headline['month'].values:
        headline_row = monthly_headline[monthly_headline['month'] == selected_period].iloc[0]
        month_prompts = headline_row['total_prompts']
        month_swipes = headline_row['total_swipes']

    # ============================================
    # SECTION A: KEY METRICS FOR SELECTED MONTH (Simplified)
    # ============================================
    # Build heading with MoM growth if available
    mom_growth = month_metrics['mom_growth_percent']
    if pd.notna(mom_growth):
        heading = f"📈 Key Metrics for {format_month(selected_month)} | MoM Growth: {mom_growth:.1f}%"
    else:
        heading = f"📈 Key Metrics for {format_month(selected_month)}"

    st.subheader(heading)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="MAU",
            value=f"{int(month_metrics['monthly_active_users']):,}",
            delta=f"{month_metrics['mom_growth_percent']:.1f}% MoM" if pd.notna(month_metrics['mom_growth_percent']) else None,
            help="Unique users who performed at least one action during this month"
        )

    with col2:
        st.metric(
            label="New Signups",
            value=f"{int(month_signups):,}",
            help="Number of new user registrations during this month"
        )

    with col3:
        st.metric(
            label="Total Prompts",
            value=f"{int(month_prompts):,}",
            help="Number of AI queries submitted during this month"
        )

    with col4:
        st.metric(
            label="Total Swipes",
            value=f"{int(month_swipes):,}",
            help="Number of card swipes performed during this month"
        )

    st.divider()

    # ============================================
    # SECTION B: MONTHLY TREND COMPARISON
    # ============================================
    st.subheader("📉 Monthly Trends (Last 12 Months)")

    # Highlight selected month in the chart
    mau_sorted = mau_data.sort_values('activity_month')

    # MAU Trend Chart
    mau_chart = create_line_chart(
        mau_sorted,
        x='activity_month',
        y='monthly_active_users',
        title="",
        y_label="Active Users"
    )
    st.plotly_chart(mau_chart, use_container_width=True, config={'displayModeBar': False})

    # Growth Rate Trend
    st.markdown("#### Month-over-Month MAU Growth Rate")
    growth_chart = create_bar_chart(
        mau_sorted,
        x='activity_month',
        y='mom_growth_percent',
        title="",
        orientation='v'
    )
    st.plotly_chart(growth_chart, use_container_width=True, config={'displayModeBar': False})

except Exception as e:
    st.error(f"❌ Error loading monthly summary data: {str(e)}")

    with st.expander("🔧 Troubleshooting"):
        st.markdown("""
        **Common Issues:**

        1. **Database Connection**
           - Verify `.streamlit/secrets.toml` contains correct credentials
           - Check database server accessibility

        2. **Data Availability**
           - Ensure `monthly_active_users` table exists in `analytics_prod_gold` schema
           - Verify analytics jobs have run and populated monthly data

        3. **Date Format**
           - Check that `activity_month` column contains valid date values
        """)

    st.exception(e)

# Footer
add_deck_footer()
