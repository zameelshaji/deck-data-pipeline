"""DECK Analytics Dashboard - Supplier Portal Page"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.data_loader import load_supplier_performance

# Page configuration
st.set_page_config(
    page_title="DECK Analytics - Supplier Portal",
    page_icon="🏢",
    layout="wide"
)

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
        # SECTION B: PERFORMANCE OVERVIEW CARDS
        # ============================================
        st.subheader(f"📊 Performance Overview: {selected_supplier}")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="Total Impressions",
                value=f"{int(supplier_detail['total_impressions']):,}",
                help=f"Unique viewers: {int(supplier_detail['unique_viewers']):,}"
            )

        with col2:
            st.metric(
                label="Total Intent Actions",
                value=f"{int(supplier_detail['total_intent_actions']):,}",
                delta=f"{supplier_detail['impression_to_intent_rate']:.1f}%" if pd.notna(supplier_detail['impression_to_intent_rate']) else None,
                help="Swipes, saves, shares"
            )

        with col3:
            st.metric(
                label="Total Conversions",
                value=f"{int(supplier_detail['total_conversions']):,}",
                delta=f"{supplier_detail['overall_conversion_rate']:.1f}%" if pd.notna(supplier_detail['overall_conversion_rate']) else None,
                help="Website visits, bookings, etc."
            )

        with col4:
            engagement_score = supplier_detail['engagement_score']
            st.metric(
                label="Engagement Score",
                value=f"{engagement_score:.1f}/10" if pd.notna(engagement_score) else "N/A",
                help="Weighted score based on all interactions"
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
            marker=dict(color=["lightblue", "lightgreen", "lightyellow"])
        ))

        funnel_fig.update_layout(
            title=None,
            showlegend=False,
            height=300
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
        with st.expander("🏆 Performance Classification", expanded=True):
            col1, col2, col3 = st.columns(3)

            with col1:
                performance_tier = supplier_detail['performance_tier']
                if performance_tier == "Top Performer":
                    st.success(f"🌟 {performance_tier}")
                elif performance_tier == "Strong Performer":
                    st.info(f"💪 {performance_tier}")
                elif performance_tier == "Average Performer":
                    st.warning(f"📊 {performance_tier}")
                else:
                    st.error(f"📉 {performance_tier}")

            with col2:
                st.metric("Engagement Score", f"{supplier_detail['engagement_score']:.1f}/10" if pd.notna(supplier_detail['engagement_score']) else "N/A")

            with col3:
                st.metric("User Conversion Rate", f"{supplier_detail['user_conversion_rate']:.1f}%" if pd.notna(supplier_detail['user_conversion_rate']) else "N/A")

    else:
        # ============================================
        # ALL SUPPLIERS LEADERBOARD
        # ============================================
        st.subheader("🏆 All Suppliers Leaderboard")

        # Display summary statistics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Suppliers", f"{len(supplier_data):,}")

        with col2:
            top_performers = supplier_data[supplier_data['performance_tier'] == 'Top Performer']
            st.metric("Top Performers", f"{len(top_performers):,}")

        with col3:
            total_impressions = supplier_data['total_impressions'].sum()
            st.metric("Total Impressions", f"{int(total_impressions):,}")

        with col4:
            total_conversions = supplier_data['total_conversions'].sum()
            st.metric("Total Conversions", f"{int(total_conversions):,}")

        st.divider()

        # Leaderboard table
        st.markdown("#### Supplier Performance Table")

        # Prepare display dataframe
        display_df = supplier_data[[
            'card_name',
            'total_impressions',
            'total_intent_actions',
            'total_conversions',
            'click_through_rate',
            'overall_conversion_rate',
            'engagement_score',
            'performance_tier'
        ]].copy()

        # Rename columns for better display
        display_df.columns = [
            'Supplier',
            'Impressions',
            'Intent Actions',
            'Conversions',
            'CTR %',
            'Conv Rate %',
            'Engagement Score',
            'Tier'
        ]

        # Format numbers
        display_df['Impressions'] = display_df['Impressions'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A")
        display_df['Intent Actions'] = display_df['Intent Actions'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A")
        display_df['Conversions'] = display_df['Conversions'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A")
        display_df['CTR %'] = display_df['CTR %'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
        display_df['Conv Rate %'] = display_df['Conv Rate %'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
        display_df['Engagement Score'] = display_df['Engagement Score'].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")

        # Display table
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )

        # Download option
        csv = supplier_data.to_csv(index=False)
        st.download_button(
            label="📥 Download Full Data (CSV)",
            data=csv,
            file_name="supplier_performance.csv",
            mime="text/csv"
        )

except Exception as e:
    st.error(f"❌ Error loading data: {str(e)}")
    st.info("💡 Make sure your database connection is configured correctly")
    st.exception(e)

# Footer
st.divider()
st.caption("📊 DECK Analytics Dashboard - Supplier Portal")
