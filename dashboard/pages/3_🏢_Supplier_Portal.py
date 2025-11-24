"""DECK Analytics Dashboard - Supplier Portal Page"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.data_loader import load_supplier_performance
from utils.styling import apply_deck_branding, add_deck_footer

# Page configuration
st.set_page_config(
    page_title="DECK Analytics - Supplier Portal",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply DECK branding
apply_deck_branding()

# Title
st.title("🏢 Supplier Performance Portal")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

try:
    # Load supplier performance data
    supplier_data = load_supplier_performance()

    if supplier_data.empty:
        st.warning("⚠️ No supplier performance data available")
        st.stop()

    # ============================================
    # SECTION A: SUPPLIER SELECTOR
    # ============================================
    st.subheader("🔍 Select Supplier")

    # Create supplier list with names
    supplier_options = ["All Suppliers"] + supplier_data['card_name'].dropna().unique().tolist()
    selected_supplier = st.selectbox(
        "Choose a supplier to view detailed metrics:",
        options=supplier_options,
        index=0
    )

    st.divider()

    # Filter data based on selection
    if selected_supplier != "All Suppliers":
        supplier_detail = supplier_data[supplier_data['card_name'] == selected_supplier].iloc[0]

        # ============================================
        # SECTION B: PERFORMANCE OVERVIEW CARDS (Simplified - 3 columns)
        # ============================================
        st.subheader(f"📊 Performance Overview: {selected_supplier}")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                label="Total Impressions",
                value=f"{int(supplier_detail['total_impressions']):,}",
                help="Number of times this supplier's card was viewed"
            )

        with col2:
            st.metric(
                label="Total Intent Actions",
                value=f"{int(supplier_detail['total_intent_actions']):,}",
                delta=f"{supplier_detail['impression_to_intent_rate']:.1f}%" if pd.notna(supplier_detail['impression_to_intent_rate']) else None,
                help="User engagement actions on this card (opens, swipes, saves, shares)"
            )

        with col3:
            st.metric(
                label="Total Conversions",
                value=f"{int(supplier_detail['total_conversions']):,}",
                delta=f"{supplier_detail['overall_conversion_rate']:.1f}%" if pd.notna(supplier_detail['overall_conversion_rate']) else None,
                help="Conversion actions taken on this card (website visits, bookings, directions, calls)"
            )

        st.divider()

        # ============================================
        # SECTION C: FUNNEL VISUALIZATION
        # ============================================
        st.subheader("🎯 Performance Funnel")

        # Create funnel chart
        funnel_stages = ['Impressions', 'Intent Actions', 'Conversions']
        funnel_values = [
            supplier_detail['total_impressions'],
            supplier_detail['total_intent_actions'],
            supplier_detail['total_conversions']
        ]

        funnel_fig = go.Figure(go.Funnel(
            y=funnel_stages,
            x=funnel_values,
            textinfo="value+percent initial",
            textposition="inside",
            textfont=dict(size=14, color='white', family='Inter'),
            marker=dict(
                color=['#FF4FA3', '#E91E8C', '#C7177A'],
                line=dict(width=3, color='white')
            )
        ))

        funnel_fig.update_layout(
            title="",
            showlegend=False,
            height=300,
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#FAFAFA',
            font={'color': '#1A1A1A', 'family': 'Inter, sans-serif'}
        )
        st.plotly_chart(funnel_fig, use_container_width=True)

        # Conversion rates
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                "Impression → Intent Rate",
                f"{supplier_detail['impression_to_intent_rate']:.1f}%" if pd.notna(supplier_detail['impression_to_intent_rate']) else "N/A"
            )
        with col2:
            st.metric(
                "Intent → Conversion Rate",
                f"{supplier_detail['intent_to_conversion_rate']:.1f}%" if pd.notna(supplier_detail['intent_to_conversion_rate']) else "N/A"
            )
        with col3:
            st.metric(
                "Overall Conversion Rate",
                f"{supplier_detail['overall_conversion_rate']:.1f}%" if pd.notna(supplier_detail['overall_conversion_rate']) else "N/A"
            )

        st.divider()

        # ============================================
        # SECTION D: RECENT PERFORMANCE TRENDS
        # ============================================
        st.subheader("📅 Recent Performance")

        tab_7d, tab_30d, tab_90d = st.tabs(["Last 7 Days", "Last 30 Days", "Last 90 Days"])

        with tab_7d:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Viewers",
                    f"{int(supplier_detail['viewers_last_7d']):,}" if pd.notna(supplier_detail['viewers_last_7d']) else "N/A"
                )
            with col2:
                st.metric(
                    "Intent Actions",
                    f"{int(supplier_detail['intent_actions_last_7d']):,}" if pd.notna(supplier_detail['intent_actions_last_7d']) else "N/A"
                )
            with col3:
                st.metric(
                    "Conversions",
                    f"{int(supplier_detail['conversions_last_7d']):,}" if pd.notna(supplier_detail['conversions_last_7d']) else "N/A"
                )

        with tab_30d:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Viewers",
                    f"{int(supplier_detail['viewers_last_30d']):,}" if pd.notna(supplier_detail['viewers_last_30d']) else "N/A"
                )
            with col2:
                st.metric(
                    "Intent Actions",
                    f"{int(supplier_detail['intent_actions_last_30d']):,}" if pd.notna(supplier_detail['intent_actions_last_30d']) else "N/A"
                )
            with col3:
                st.metric(
                    "Conversions",
                    f"{int(supplier_detail['conversions_last_30d']):,}" if pd.notna(supplier_detail['conversions_last_30d']) else "N/A"
                )

        with tab_90d:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Viewers",
                    f"{int(supplier_detail['viewers_last_90d']):,}" if pd.notna(supplier_detail['viewers_last_90d']) else "N/A"
                )
            with col2:
                st.metric(
                    "Intent Actions",
                    f"{int(supplier_detail['intent_actions_last_90d']):,}" if pd.notna(supplier_detail['intent_actions_last_90d']) else "N/A"
                )
            with col3:
                st.metric(
                    "Conversions",
                    f"{int(supplier_detail['conversions_last_90d']):,}" if pd.notna(supplier_detail['conversions_last_90d']) else "N/A"
                )

        st.divider()

        # ============================================
        # SECTION E: DETAILED METRICS BREAKDOWN
        # ============================================
        st.subheader("📋 Detailed Metrics")

        # Intent Actions Detail
        with st.expander("🎯 Intent Actions Breakdown", expanded=True):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Card Opens", f"{int(supplier_detail['card_opens']):,}" if pd.notna(supplier_detail['card_opens']) else "N/A")
                st.metric("Positive Swipes", f"{int(supplier_detail['positive_swipes']):,}" if pd.notna(supplier_detail['positive_swipes']) else "N/A")

            with col2:
                st.metric("Negative Swipes", f"{int(supplier_detail['negative_swipes']):,}" if pd.notna(supplier_detail['negative_swipes']) else "N/A")
                st.metric("Saves", f"{int(supplier_detail['saves']):,}" if pd.notna(supplier_detail['saves']) else "N/A")

            with col3:
                st.metric("Shares", f"{int(supplier_detail['shares']):,}" if pd.notna(supplier_detail['shares']) else "N/A")
                st.metric("Click-Through Rate", f"{supplier_detail['click_through_rate']:.1f}%" if pd.notna(supplier_detail['click_through_rate']) else "N/A")

            # Intent rates
            st.markdown("#### Intent Quality Metrics")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Positive Swipe Rate", f"{supplier_detail['positive_swipe_rate']:.1f}%" if pd.notna(supplier_detail['positive_swipe_rate']) else "N/A")
            with col2:
                st.metric("Save Rate", f"{supplier_detail['save_rate']:.1f}%" if pd.notna(supplier_detail['save_rate']) else "N/A")
            with col3:
                st.metric("Share Rate", f"{supplier_detail['share_rate']:.1f}%" if pd.notna(supplier_detail['share_rate']) else "N/A")

        # Conversion Actions Detail
        with st.expander("💰 Conversion Actions Breakdown", expanded=True):
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Website Visits", f"{int(supplier_detail['website_visits']):,}" if pd.notna(supplier_detail['website_visits']) else "N/A")

            with col2:
                st.metric("Book Clicks", f"{int(supplier_detail['book_clicks']):,}" if pd.notna(supplier_detail['book_clicks']) else "N/A")

            with col3:
                st.metric("Direction Clicks", f"{int(supplier_detail['direction_clicks']):,}" if pd.notna(supplier_detail['direction_clicks']) else "N/A")

            with col4:
                st.metric("Phone Clicks", f"{int(supplier_detail['phone_clicks']):,}" if pd.notna(supplier_detail['phone_clicks']) else "N/A")

        # Performance Classification
        with st.expander("🏆 Performance Metrics", expanded=True):
            col1, col2 = st.columns(2)

            with col1:
                st.metric("Impression → Intent Rate", f"{supplier_detail['impression_to_intent_rate']:.1f}%" if pd.notna(supplier_detail['impression_to_intent_rate']) else "N/A")

            with col2:
                st.metric("User Conversion Rate", f"{supplier_detail['user_conversion_rate']:.1f}%" if pd.notna(supplier_detail['user_conversion_rate']) else "N/A")

    else:
        # ============================================
        # ALL SUPPLIERS LEADERBOARD (Simplified)
        # ============================================
        st.subheader("🏆 All Suppliers Leaderboard")

        # Display summary statistics (4 columns - simplified)
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Suppliers",
                f"{len(supplier_data):,}",
                help="Number of featured supplier partners tracked in the system"
            )

        with col2:
            total_impressions = supplier_data['total_impressions'].sum()
            st.metric(
                "Total Impressions",
                f"{int(total_impressions):,}",
                help="Total number of times supplier cards were viewed by users"
            )

        with col3:
            total_intent = supplier_data['total_intent_actions'].sum()
            st.metric(
                "Total Intent Signals",
                f"{int(total_intent):,}",
                help="Total user engagement actions (card opens, swipes, saves, shares) on supplier cards"
            )

        with col4:
            total_conversions = supplier_data['total_conversions'].sum()
            st.metric(
                "Total Conversions",
                f"{int(total_conversions):,}",
                help="Total conversion actions (website visits, booking clicks, directions, phone clicks) on supplier cards"
            )

        st.divider()

        # Leaderboard table (Simplified - removed CTR, Conv Rate, Engagement Score)
        st.markdown("#### 📊 Supplier Performance Table")
        st.caption("💡 Click column headers to sort. Hover over rows to highlight.")

        # Prepare display dataframe with only essential columns
        display_df = supplier_data[[
            'card_name',
            'total_impressions',
            'total_intent_actions',
            'total_conversions'
        ]].copy()

        # Rename columns for better display
        display_df.columns = [
            'Supplier',
            'Impressions',
            'Intent Actions',
            'Conversions'
        ]

        # Sort by impressions by default (descending)
        display_df = display_df.sort_values('Impressions', ascending=False, na_position='last')

        # Display table with better styling
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Supplier": st.column_config.TextColumn("Supplier", width="medium"),
                "Impressions": st.column_config.NumberColumn("Impressions", format="%d"),
                "Intent Actions": st.column_config.NumberColumn("Intent Actions", format="%d"),
                "Conversions": st.column_config.NumberColumn("Conversions", format="%d")
            }
        )

        # Download option with feedback
        csv = supplier_data.to_csv(index=False)
        download_clicked = st.download_button(
            label="📥 Download Full Data (CSV)",
            data=csv,
            file_name=f"supplier_performance_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            help="Download complete supplier performance data including all metrics"
        )

        if download_clicked:
            st.success("✅ Download started! Check your downloads folder.")

except Exception as e:
    st.error(f"❌ Error loading supplier data: {str(e)}")

    with st.expander("🔧 Troubleshooting Steps"):
        st.markdown("""
        **Common Issues & Solutions:**

        1. **Database Connection**
           - Verify your database credentials in `.streamlit/secrets.toml`
           - Check if supplier performance tables exist in your database

        2. **Data Quality**
           - Ensure supplier analytics are being tracked
           - Verify that card impressions and actions are being logged

        3. **Calculation Issues**
           - High CTR (near 100%) usually indicates low impression counts
           - Zero conversion rates may be expected for suppliers with few interactions
        """)

    st.exception(e)

# Footer
add_deck_footer()
