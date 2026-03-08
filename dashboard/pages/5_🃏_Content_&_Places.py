"""DECK Content & Places Dashboard - Card/place/content performance analytics"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.data_loader import (
    load_distinct_categories,
    load_content_overview_kpis,
    load_top_places,
    load_bad_recommendations,
    load_category_performance,
    load_neighborhood_performance,
    load_price_level_performance,
    load_viral_content,
    load_scatter_data,
)

st.set_page_config(
    page_title="Content & Places | DECK Analytics",
    page_icon="\U0001F0CF",
    layout="wide"
)

apply_deck_branding()

st.title("\U0001F0CF Content & Places")
st.markdown("*Performance analytics for places, categories, and content recommendations*")

# --- Sidebar Filters ---
with st.sidebar:
    st.header("Filters")
    categories = load_distinct_categories()
    selected_categories = st.multiselect("Categories", categories, default=categories)

# =============================================================================
# A: Content Overview KPIs
# =============================================================================
st.subheader("Content Overview")

try:
    kpis = load_content_overview_kpis(selected_categories)

    if kpis.empty:
        st.info("No data available for the selected filters.")
    else:
        row = kpis.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric(
                "Unique Places Shown",
                f"{int(row.get('total_places', 0)):,}",
                help="Total number of unique places that have been shown to users"
            )
        with c2:
            avg_save = row.get('avg_save_rate')
            st.metric(
                "Overall Save Rate",
                f"{float(avg_save) * 100:.1f}%" if pd.notna(avg_save) else "N/A",
                help="Average save rate across all places (saves / impressions)"
            )
        with c3:
            avg_swipe = row.get('avg_right_swipe_rate')
            st.metric(
                "Overall Swipe Rate",
                f"{float(avg_swipe) * 100:.1f}%" if pd.notna(avg_swipe) else "N/A",
                help="Average right-swipe rate across all places"
            )
        with c4:
            avg_imp = row.get('avg_impressions')
            st.metric(
                "Avg Impressions per Place",
                f"{float(avg_imp):.1f}" if pd.notna(avg_imp) else "N/A",
                help="Average number of times each place has been shown"
            )
except Exception as e:
    st.error(f"Error loading content overview KPIs: {e}")

st.divider()

# =============================================================================
# B: Top Performing Places
# =============================================================================
st.subheader("Top Performing Places")

try:
    top_places_df = load_top_places(selected_categories, min_impressions=1, limit=20)

    if top_places_df.empty:
        st.info("No data available for the selected filters.")
    else:
        st.dataframe(
            top_places_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "place_name": st.column_config.TextColumn("Place Name"),
                "category": st.column_config.TextColumn("Category"),
                "neighborhood": st.column_config.TextColumn("Neighborhood"),
                "total_impressions": st.column_config.NumberColumn("Impressions", format="%d"),
                "save_rate_pct": st.column_config.NumberColumn("Save Rate %", format="%.1f%%"),
                "swipe_rate_pct": st.column_config.NumberColumn("Swipe Rate %", format="%.1f%%"),
                "total_saves": st.column_config.NumberColumn("Saves", format="%d"),
                "viral_score": st.column_config.NumberColumn("Viral Score", format="%.2f"),
                "rating": st.column_config.NumberColumn("Rating", format="%.1f"),
            }
        )
except Exception as e:
    st.error(f"Error loading top performing places: {e}")

st.divider()

# =============================================================================
# C: Worst Performing Places
# =============================================================================
try:
    with st.expander("Places with High Impressions, Low Saves"):
        bad_recs_df = load_bad_recommendations(selected_categories)

        if bad_recs_df.empty:
            st.info("No data available for the selected filters.")
        else:
            st.dataframe(
                bad_recs_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "place_name": st.column_config.TextColumn("Place Name"),
                    "category": st.column_config.TextColumn("Category"),
                    "neighborhood": st.column_config.TextColumn("Neighborhood"),
                    "total_impressions": st.column_config.NumberColumn("Impressions", format="%d"),
                    "save_rate_pct": st.column_config.NumberColumn("Save Rate %", format="%.1f%%"),
                    "total_saves": st.column_config.NumberColumn("Saves", format="%d"),
                    "total_left_swipes": st.column_config.NumberColumn("Left Swipes", format="%d"),
                }
            )
except Exception as e:
    st.error(f"Error loading worst performing places: {e}")

st.divider()

# =============================================================================
# D: Category Performance
# =============================================================================
st.subheader("Category Performance")

try:
    cat_df = load_category_performance()

    if cat_df.empty:
        st.info("No data available for the selected filters.")
    else:
        fig_cat = go.Figure()
        fig_cat.add_trace(go.Bar(
            x=cat_df["category"],
            y=cat_df["save_rate_pct"],
            name="Save Rate %",
            marker_color=BRAND_COLORS["accent"],
        ))
        fig_cat.add_trace(go.Bar(
            x=cat_df["category"],
            y=cat_df["swipe_rate_pct"],
            name="Swipe Rate %",
            marker_color=BRAND_COLORS["success"],
        ))
        fig_cat.update_layout(
            barmode="group",
            title="Save Rate vs Swipe Rate by Category",
            xaxis_title="Category",
            yaxis_title="Rate (%)",
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor="white",
            paper_bgcolor="white",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_cat, use_container_width=True)

        st.dataframe(
            cat_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "category": st.column_config.TextColumn("Category"),
                "place_count": st.column_config.NumberColumn("Places", format="%d"),
                "total_impressions": st.column_config.NumberColumn("Impressions", format="%d"),
                "total_saves": st.column_config.NumberColumn("Saves", format="%d"),
                "save_rate_pct": st.column_config.NumberColumn("Save Rate %", format="%.1f%%"),
                "swipe_rate_pct": st.column_config.NumberColumn("Swipe Rate %", format="%.1f%%"),
                "total_shares": st.column_config.NumberColumn("Shares", format="%d"),
            }
        )
except Exception as e:
    st.error(f"Error loading category performance: {e}")

st.divider()

# =============================================================================
# E: Neighborhood Performance
# =============================================================================
st.subheader("Neighborhood Performance")

try:
    nbr_df = load_neighborhood_performance()

    if nbr_df.empty:
        st.info("No data available for the selected filters.")
    else:
        fig_nbr = go.Figure()
        fig_nbr.add_trace(go.Bar(
            x=nbr_df["neighborhood"],
            y=nbr_df["save_rate_pct"],
            name="Save Rate %",
            marker_color=BRAND_COLORS["accent"],
        ))
        fig_nbr.add_trace(go.Bar(
            x=nbr_df["neighborhood"],
            y=nbr_df["swipe_rate_pct"],
            name="Swipe Rate %",
            marker_color=BRAND_COLORS["success"],
        ))
        fig_nbr.update_layout(
            barmode="group",
            title="Save Rate vs Swipe Rate by Neighborhood",
            xaxis_title="Neighborhood",
            yaxis_title="Rate (%)",
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor="white",
            paper_bgcolor="white",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_nbr, use_container_width=True)

        st.dataframe(
            nbr_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "neighborhood": st.column_config.TextColumn("Neighborhood"),
                "place_count": st.column_config.NumberColumn("Places", format="%d"),
                "total_impressions": st.column_config.NumberColumn("Impressions", format="%d"),
                "total_saves": st.column_config.NumberColumn("Saves", format="%d"),
                "save_rate_pct": st.column_config.NumberColumn("Save Rate %", format="%.1f%%"),
                "swipe_rate_pct": st.column_config.NumberColumn("Swipe Rate %", format="%.1f%%"),
                "total_shares": st.column_config.NumberColumn("Shares", format="%d"),
            }
        )
except Exception as e:
    st.error(f"Error loading neighborhood performance: {e}")

st.divider()

# =============================================================================
# F: Price Level Analysis
# =============================================================================
st.subheader("Price Level Analysis")

try:
    price_df = load_price_level_performance()

    if price_df.empty:
        st.info("No data available for the selected filters.")
    else:
        fig_price = go.Figure()
        fig_price.add_trace(go.Bar(
            x=price_df["price_level"],
            y=price_df["save_rate_pct"],
            marker_color=BRAND_COLORS["accent"],
            text=price_df["save_rate_pct"].apply(lambda v: f"{v:.1f}%"),
            textposition="auto",
        ))
        fig_price.update_layout(
            title="Save Rate by Price Level",
            xaxis_title="Price Level",
            yaxis_title="Save Rate (%)",
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(fig_price, use_container_width=True)
except Exception as e:
    st.error(f"Error loading price level analysis: {e}")

st.divider()

# =============================================================================
# G: Viral Content
# =============================================================================
st.subheader("Viral Content")

try:
    viral_df = load_viral_content()

    if viral_df.empty:
        st.info("No data available for the selected filters.")
    else:
        st.dataframe(
            viral_df.head(10),
            use_container_width=True,
            hide_index=True,
            column_config={
                "place_name": st.column_config.TextColumn("Place Name"),
                "category": st.column_config.TextColumn("Category"),
                "viral_score": st.column_config.NumberColumn("Viral Score", format="%.2f"),
                "total_saves": st.column_config.NumberColumn("Saves", format="%d"),
                "total_shares": st.column_config.NumberColumn("Shares", format="%d"),
                "save_rate_pct": st.column_config.NumberColumn("Save Rate %", format="%.1f%%"),
            }
        )
except Exception as e:
    st.error(f"Error loading viral content: {e}")

st.divider()

# =============================================================================
# H: Impressions vs Saves Scatter
# =============================================================================
st.subheader("Impressions vs Save Rate")

try:
    scatter_df = load_scatter_data(selected_categories)

    if scatter_df.empty:
        st.info("No data available for the selected filters.")
    else:
        fig_scatter = px.scatter(
            scatter_df,
            x="total_impressions",
            y="save_rate",
            color="category",
            size="total_saves",
            hover_data=["place_name", "neighborhood", "rating"],
            log_x=True,
            title="Impressions vs Save Rate (log scale)",
            labels={
                "total_impressions": "Total Impressions",
                "save_rate": "Save Rate",
                "category": "Category",
                "total_saves": "Total Saves",
            },
        )
        fig_scatter.update_layout(
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
except Exception as e:
    st.error(f"Error loading scatter data: {e}")

# --- Footer ---
add_deck_footer()
