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
    create_bar_chart,
    create_multi_line_chart
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
    # Note: headline_data contains cumulative totals, so we need to calculate the difference
    # between the last day and first day of each month
    if not headline_data.empty:
        headline_data['metric_date'] = pd.to_datetime(headline_data['metric_date'])
        headline_data['month'] = headline_data['metric_date'].dt.to_period('M')

        # For each month, get the max value (end of month) minus min value (start of month)
        # This gives us the actual activity for that month
        monthly_headline_list = []
        for month in headline_data['month'].unique():
            month_data = headline_data[headline_data['month'] == month].sort_values('metric_date')
            if len(month_data) > 1:
                # Difference between last and first day of the month
                prompts_in_month = month_data['total_prompts'].iloc[-1] - month_data['total_prompts'].iloc[0]
                swipes_in_month = month_data['total_swipes'].iloc[-1] - month_data['total_swipes'].iloc[0]
            else:
                # Only one day of data, use that value (edge case)
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
    # SECTION A: KEY METRICS FOR SELECTED MONTH
    # ============================================
    st.subheader(f"📈 Key Metrics for {format_month(selected_month)}")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Monthly Active Users",
            value=f"{int(month_metrics['monthly_active_users']):,}",
            delta=f"{month_metrics['mom_growth_percent']:.1f}% MoM" if pd.notna(month_metrics['mom_growth_percent']) else None
        )

    with col2:
        st.metric(
            label="New Signups",
            value=f"{int(month_signups):,}"
        )

    with col3:
        st.metric(
            label="Total Prompts",
            value=f"{int(month_prompts):,}"
        )

    with col4:
        st.metric(
            label="Total Swipes",
            value=f"{int(month_swipes):,}"
        )

    # Second row of metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="3-Month Rolling Avg",
            value=f"{int(month_metrics['rolling_3month_avg']):,}" if pd.notna(month_metrics['rolling_3month_avg']) else "N/A"
        )

    with col2:
        st.metric(
            label="AI Adoption Rate",
            value=f"{month_metrics['ai_adoption_rate']:.1f}%" if pd.notna(month_metrics['ai_adoption_rate']) else "N/A"
        )

    with col3:
        st.metric(
            label="Avg Events/User",
            value=f"{month_metrics['avg_events_per_user']:.1f}" if pd.notna(month_metrics['avg_events_per_user']) else "N/A"
        )

    with col4:
        st.metric(
            label="MoM Growth",
            value=f"{month_metrics['mom_growth_percent']:.1f}%" if pd.notna(month_metrics['mom_growth_percent']) else "N/A"
        )

    st.divider()

    # ============================================
    # SECTION B: FEATURE ADOPTION FOR SELECTED MONTH
    # ============================================
    st.subheader(f"🎯 Feature Adoption - {format_month(selected_month)}")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        ai_users = month_metrics.get('ai_active_users', 0)
        st.metric(
            label="AI Active Users",
            value=f"{int(ai_users):,}" if pd.notna(ai_users) else "N/A"
        )

    with col2:
        conversion_rate = month_metrics.get('conversion_user_rate', 0)
        st.metric(
            label="Conversion User Rate",
            value=f"{conversion_rate:.1f}%" if pd.notna(conversion_rate) else "N/A"
        )

    with col3:
        multiplayer_rate = month_metrics.get('multiplayer_adoption_rate', 0)
        st.metric(
            label="Multiplayer Adoption",
            value=f"{multiplayer_rate:.1f}%" if pd.notna(multiplayer_rate) else "N/A"
        )

    with col4:
        featured_rate = month_metrics.get('featured_adoption_rate', 0)
        st.metric(
            label="Featured Adoption",
            value=f"{featured_rate:.1f}%" if pd.notna(featured_rate) else "N/A"
        )

    st.divider()

    # ============================================
    # SECTION C: MONTHLY TREND COMPARISON
    # ============================================
    st.subheader("📉 Monthly Trends (Last 12 Months)")

    # Highlight selected month in the chart
    mau_sorted = mau_data.sort_values('activity_month')

    # MAU Trend Chart
    mau_chart = create_line_chart(
        mau_sorted,
        x='activity_month',
        y='monthly_active_users',
        title="Monthly Active Users Trend",
        y_label="Active Users"
    )
    st.plotly_chart(mau_chart, use_container_width=True)

    # Growth Rate Trend
    st.markdown("#### Month-over-Month Growth Rate")
    growth_chart = create_bar_chart(
        mau_sorted,
        x='activity_month',
        y='mom_growth_percent',
        title="MoM Growth %",
        orientation='v'
    )
    st.plotly_chart(growth_chart, use_container_width=True)

    st.divider()

    # ============================================
    # SECTION D: DETAILED METRICS TABLE
    # ============================================
    st.subheader(f"📋 Detailed Metrics - {format_month(selected_month)}")

    # Create a summary table of all available metrics
    metrics_data = {
        'Metric': [
            'Monthly Active Users',
            'New Signups',
            'Total Prompts',
            'Total Swipes',
            'MoM Growth %',
            '3-Month Rolling Average',
            'AI Adoption Rate',
            'Avg Events/User',
            'AI Active Users',
            'Conversion User Rate',
            'Multiplayer Adoption Rate',
            'Featured Adoption Rate'
        ],
        'Value': [
            f"{int(month_metrics['monthly_active_users']):,}",
            f"{int(month_signups):,}",
            f"{int(month_prompts):,}",
            f"{int(month_swipes):,}",
            f"{month_metrics['mom_growth_percent']:.2f}%" if pd.notna(month_metrics['mom_growth_percent']) else "N/A",
            f"{int(month_metrics['rolling_3month_avg']):,}" if pd.notna(month_metrics['rolling_3month_avg']) else "N/A",
            f"{month_metrics['ai_adoption_rate']:.2f}%" if pd.notna(month_metrics['ai_adoption_rate']) else "N/A",
            f"{month_metrics['avg_events_per_user']:.2f}" if pd.notna(month_metrics['avg_events_per_user']) else "N/A",
            f"{int(month_metrics.get('ai_active_users', 0)):,}" if pd.notna(month_metrics.get('ai_active_users', 0)) else "N/A",
            f"{month_metrics.get('conversion_user_rate', 0):.2f}%" if pd.notna(month_metrics.get('conversion_user_rate', 0)) else "N/A",
            f"{month_metrics.get('multiplayer_adoption_rate', 0):.2f}%" if pd.notna(month_metrics.get('multiplayer_adoption_rate', 0)) else "N/A",
            f"{month_metrics.get('featured_adoption_rate', 0):.2f}%" if pd.notna(month_metrics.get('featured_adoption_rate', 0)) else "N/A"
        ]
    }

    metrics_df = pd.DataFrame(metrics_data)
    st.dataframe(metrics_df, use_container_width=True, hide_index=True)

    st.divider()

    # ============================================
    # SECTION E: MONTH COMPARISON
    # ============================================
    st.subheader("🔄 Compare with Previous Month")

    # Get previous month data if available
    current_idx = mau_sorted[mau_sorted['activity_month'] == selected_month].index
    if len(current_idx) > 0:
        current_pos = mau_sorted.index.get_loc(current_idx[0])
        if current_pos > 0:
            prev_month_metrics = mau_sorted.iloc[current_pos - 1]
            prev_month_date = prev_month_metrics['activity_month']

            col1, col2 = st.columns(2)

            with col1:
                st.markdown(f"**{format_month(selected_month)}**")
                st.metric("MAU", f"{int(month_metrics['monthly_active_users']):,}")
                st.metric("AI Adoption", f"{month_metrics['ai_adoption_rate']:.1f}%")
                st.metric("Avg Events/User", f"{month_metrics['avg_events_per_user']:.1f}")

            with col2:
                st.markdown(f"**{format_month(prev_month_date)}**")
                st.metric("MAU", f"{int(prev_month_metrics['monthly_active_users']):,}")
                st.metric("AI Adoption", f"{prev_month_metrics['ai_adoption_rate']:.1f}%")
                st.metric("Avg Events/User", f"{prev_month_metrics['avg_events_per_user']:.1f}")
        else:
            st.info("No previous month data available for comparison")
    else:
        st.info("Unable to find comparison data")

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
