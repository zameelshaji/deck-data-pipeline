"""DECK Analytics Dashboard - Conversion & Viral Loop Page"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.data_loader import (
    load_conversion_overview,
    load_conversion_context,
    load_conversion_by_category,
    load_viral_loop_summary,
    load_viral_loop_detail,
)

st.set_page_config(
    page_title="Conversion & Viral | DECK Analytics",
    page_icon="\U0001f504",
    layout="wide",
)

apply_deck_branding()

st.title("\U0001f504 Conversion & Viral Loop")
st.markdown("*Booking intent signals and social sharing effectiveness*")

st.info(
    "Conversion and viral loop tracking is early-stage. Data volume is limited "
    "(~118 conversion events, ~13 share links tracked). Treat these as directional "
    "signals, not statistical truth."
)

# ══════════════════════════════════════════════════════════════════════════════
# Section A: Conversion Signals Overview
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("\U0001f4b3 Conversion Signals Overview")

try:
    overview_df = load_conversion_overview()
    if overview_df.empty:
        st.info("No data available.")
    else:
        row = overview_df.iloc[0]
        total = int(row.get("total_conversions", 0))
        opened = int(row.get("opened_website", 0))
        booked = int(row.get("book_with_deck", 0))
        directions = int(row.get("click_directions", 0))

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric(
                label="Total Conversion Actions",
                value=f"{total:,}",
                help="Total number of booking-intent actions recorded across all users",
            )
        with c2:
            st.metric(
                label="Opened Website",
                value=f"{opened:,}",
                help="Number of times users tapped to open a place's website",
            )
        with c3:
            st.metric(
                label="Book with DECK",
                value=f"{booked:,}",
                help="Number of times users initiated a booking through DECK",
            )
        with c4:
            st.metric(
                label="Click Directions",
                value=f"{directions:,}",
                help="Number of times users tapped to get directions to a place",
            )
except Exception as e:
    st.error(f"Error loading conversion overview: {str(e)}")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# Section B: Conversion Context
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("\U0001f50d Conversion Context")

try:
    context_df = load_conversion_context()
    if context_df.empty:
        st.info("No data available.")
    else:
        row = context_df.iloc[0]
        pct_saved = float(row.get("pct_saved_first", 0) or 0)
        avg_minutes = float(row.get("avg_minutes_to_convert", 0) or 0)
        pct_prompt = float(row.get("pct_prompt_initiated", 0) or 0)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric(
                label="% Saved First",
                value=f"{pct_saved:.1f}%",
                help="Percentage of conversion actions where the user saved the place before converting",
            )
        with c2:
            st.metric(
                label="Avg Minutes to Convert",
                value=f"{avg_minutes:.1f}",
                help="Average time in minutes between saving a place and taking a conversion action",
            )
        with c3:
            st.metric(
                label="% Prompt-Initiated",
                value=f"{pct_prompt:.1f}%",
                help="Percentage of conversions that originated from an AI prompt session",
            )
except Exception as e:
    st.error(f"Error loading conversion context: {str(e)}")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# Section C: Conversion by Place Category
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("\U0001f4ca Conversion by Place Category")

try:
    cat_df = load_conversion_by_category()
    if cat_df.empty:
        st.info("No data available.")
    else:
        fig = go.Figure(
            go.Bar(
                x=cat_df["place_category"],
                y=cat_df["conversion_count"],
                marker_color="#E91E8C",
                text=cat_df["conversion_count"],
                textposition="outside",
            )
        )
        fig.update_layout(
            xaxis_title="Place Category",
            yaxis_title="Conversion Count",
            font=dict(family="Inter, system-ui, sans-serif", size=13),
            plot_bgcolor="white",
            paper_bgcolor="white",
            margin=dict(l=40, r=20, t=40, b=40),
            yaxis=dict(gridcolor=BRAND_COLORS["border"]),
        )
        st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Error loading conversion by category: {str(e)}")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# Section D: Viral Loop Metrics
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("\U0001f517 Viral Loop Metrics")

try:
    viral_df = load_viral_loop_summary()
    if viral_df.empty:
        st.info("No data available.")
    else:
        row = viral_df.iloc[0]
        total_shares = int(row.get("total_shares", 0) or 0)
        avg_viewers = float(row.get("avg_viewers", 0) or 0)
        avg_signup_rate = float(row.get("avg_signup_rate", 0) or 0)
        avg_k = float(row.get("avg_k_factor", 0) or 0)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric(
                label="Total Shares",
                value=f"{total_shares:,}",
                help="Total number of share links created by users",
            )
        with c2:
            st.metric(
                label="Avg Viewers per Share",
                value=f"{avg_viewers:.1f}",
                help="Average number of unique viewers per share link",
            )
        with c3:
            st.metric(
                label="Avg Signup Rate",
                value=f"{avg_signup_rate * 100:.1f}%",
                help="Average percentage of share link viewers who signed up",
            )
        with c4:
            st.metric(
                label="Avg K-Factor",
                value=f"{avg_k:.3f}",
                help="Average effective K-factor: how many new users each share generates",
            )
except Exception as e:
    st.error(f"Error loading viral loop metrics: {str(e)}")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# Section E: Share Effectiveness Table
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("\U0001f4cb Share Effectiveness Table")

try:
    detail_df = load_viral_loop_detail()
    if detail_df.empty:
        st.info("No data available.")
    else:
        st.dataframe(detail_df, use_container_width=True, hide_index=True)
except Exception as e:
    st.error(f"Error loading share effectiveness table: {str(e)}")

add_deck_footer()
