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
    load_organic_vs_referred_weekly,
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
# Section D2: Organic vs Referred Acquisition (weekly trend)
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("\U0001f331 Organic vs Referred Acquisition")
st.markdown(
    "*Share of new activated users arriving organically vs via a share link / "
    "referral code, plus D30 retention for each group. A rising organic share "
    "is one signal of a working network effect.*"
)

try:
    mix_df = load_organic_vs_referred_weekly()
    if mix_df.empty:
        st.info("No organic/referral data available.")
    else:
        mix_df = mix_df.copy()
        mix_df["activation_week"] = pd.to_datetime(mix_df["activation_week"])
        mix_df["organic_count"] = mix_df["organic_count"].fillna(0).astype(int)
        mix_df["referral_count"] = mix_df["referral_count"].fillna(0).astype(int)
        mix_df["total_count"] = mix_df["organic_count"] + mix_df["referral_count"]
        mix_df["pct_organic"] = (
            mix_df["organic_count"] / mix_df["total_count"].where(mix_df["total_count"] > 0)
        ) * 100

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**% of activated users arriving organically**")
            plot_df = mix_df.dropna(subset=["pct_organic"])
            if plot_df.empty:
                st.info("Not enough data to render.")
            else:
                fig_pct = go.Figure(
                    go.Scatter(
                        x=plot_df["activation_week"],
                        y=plot_df["pct_organic"],
                        mode="lines+markers",
                        line=dict(color=BRAND_COLORS.get("primary", "#E91E8C"), width=3),
                        marker=dict(size=6),
                        hovertemplate="Week: %{x|%Y-%m-%d}<br>Organic: %{y:.1f}%<extra></extra>",
                    )
                )
                fig_pct.update_layout(
                    xaxis_title="Activation week",
                    yaxis_title="% organic",
                    yaxis=dict(range=[0, 100], ticksuffix="%", gridcolor=BRAND_COLORS["border"]),
                    font=dict(family="Inter, system-ui, sans-serif", size=13),
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    margin=dict(l=40, r=20, t=20, b=40),
                    height=360,
                )
                st.plotly_chart(fig_pct, use_container_width=True)

        with col2:
            st.markdown("**D30 retention: organic vs referred cohorts**")
            ret_df = mix_df.copy()
            ret_df["organic_retention_d30_pct"] = ret_df["organic_retention_d30"] * 100
            ret_df["referral_retention_d30_pct"] = ret_df["referral_retention_d30"] * 100

            fig_ret = go.Figure()
            fig_ret.add_trace(
                go.Scatter(
                    x=ret_df["activation_week"],
                    y=ret_df["organic_retention_d30_pct"],
                    mode="lines+markers",
                    name="Organic",
                    line=dict(color=BRAND_COLORS.get("primary", "#E91E8C"), width=3),
                    marker=dict(size=6),
                    connectgaps=False,
                    hovertemplate="Week: %{x|%Y-%m-%d}<br>Organic D30: %{y:.1f}%<extra></extra>",
                )
            )
            fig_ret.add_trace(
                go.Scatter(
                    x=ret_df["activation_week"],
                    y=ret_df["referral_retention_d30_pct"],
                    mode="lines+markers",
                    name="Referred",
                    line=dict(color=BRAND_COLORS.get("accent", "#6366F1"), width=3, dash="dot"),
                    marker=dict(size=6),
                    connectgaps=False,
                    hovertemplate="Week: %{x|%Y-%m-%d}<br>Referred D30: %{y:.1f}%<extra></extra>",
                )
            )
            fig_ret.update_layout(
                xaxis_title="Activation week",
                yaxis_title="D30 retention",
                yaxis=dict(range=[0, 100], ticksuffix="%", gridcolor=BRAND_COLORS["border"]),
                font=dict(family="Inter, system-ui, sans-serif", size=13),
                plot_bgcolor="white",
                paper_bgcolor="white",
                margin=dict(l=40, r=20, t=20, b=40),
                height=360,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_ret, use_container_width=True)

        st.caption(
            "Cohorts with zero users in a group render as gaps in the retention "
            "line rather than 0%. Early weeks with tiny referral counts will be "
            "noisy."
        )
except Exception as e:
    st.error(f"Error loading organic-vs-referred trend: {str(e)}")

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
