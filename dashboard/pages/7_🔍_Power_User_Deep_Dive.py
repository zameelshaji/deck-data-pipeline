"""DECK Analytics Dashboard - Power User Deep Dive Page"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sys
import json
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.db_connection import get_database_connection
from utils.styling import apply_deck_branding, add_deck_footer

# ── Brand colours (match existing dashboard) ──────────────────────────────────
PINK = "#E91E8C"
PINK_DARK = "#C7177A"
GREEN = "#10B981"
ORANGE = "#F59E0B"
RED = "#EF4444"
BLUE = "#3B82F6"
GREY = "#6B7280"
LIGHT_BG = "#F9FAFB"

# Page configuration
st.set_page_config(
    page_title="DECK Analytics - Power User Deep Dive",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_deck_branding()

# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

from sqlalchemy import text


@st.cache_data(ttl=300)
def load_power_users():
    """Load all users with >5 sessions (power users)."""
    engine = get_database_connection()
    query = """
    SELECT
        user_id,
        email,
        username,
        full_name,
        signup_date,
        is_activated,
        activation_date,
        activation_week,
        activation_trigger,
        days_since_signup,
        last_activity_date,
        days_since_last_activity,
        total_sessions,
        total_prompts,
        total_swipes,
        total_saves,
        total_shares,
        total_boards_created,
        total_multiplayer_sessions,
        total_conversions,
        total_referrals_given,
        user_archetype,
        is_planner,
        is_passenger,
        churn_risk,
        is_churned,
        retained_d7,
        retained_d30,
        retained_d60,
        referral_source,
        avg_saves_per_session,
        avg_session_duration_seconds,
        prompt_to_save_rate,
        swipe_to_save_rate,
        sessions_with_save_pct,
        sessions_with_share_pct,
        days_active,
        likes_adventure,
        likes_dining,
        likes_drinks,
        likes_culture,
        likes_entertainment,
        likes_health
    FROM analytics_prod_gold.fct_user_segments
    WHERE total_sessions > 5
    ORDER BY total_sessions DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    return df


@st.cache_data(ttl=300)
def load_user_engagement_trajectory(user_id: str):
    """Week-by-week engagement for a specific user."""
    engine = get_database_connection()
    query = """
    SELECT *
    FROM analytics_prod_gold.fct_user_engagement_trajectory
    WHERE user_id = :uid
    ORDER BY activity_week
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"uid": user_id})
    return df


@st.cache_data(ttl=300)
def load_user_sessions(user_id: str):
    """Session-level outcomes for a specific user."""
    engine = get_database_connection()
    query = """
    SELECT
        session_id,
        session_date,
        started_at,
        ended_at,
        session_duration_seconds,
        initiation_surface,
        device_type,
        has_save,
        has_share,
        has_post_share_interaction,
        save_count,
        share_count,
        meets_psr_broad,
        meets_psr_strict,
        is_no_value_session,
        time_to_first_save_seconds,
        time_to_first_share_seconds,
        intent_strength,
        is_prompt_session,
        is_genuine_planning_attempt
    FROM analytics_prod_gold.fct_session_outcomes
    WHERE user_id = :uid
    ORDER BY started_at DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"uid": user_id})
    return df


@st.cache_data(ttl=300)
def load_user_prompts(user_id: str):
    """Prompt-level analysis for a specific user."""
    engine = get_database_connection()
    query = """
    SELECT
        query_id,
        query_text,
        query_date,
        query_timestamp,
        location,
        session_id,
        prompt_sequence_in_session,
        is_refinement,
        cards_generated,
        cards_shown,
        cards_liked,
        cards_disliked,
        cards_saved,
        like_rate,
        save_rate,
        zero_save_prompt,
        led_to_save,
        led_to_share,
        prompt_intent,
        prompt_specificity,
        performance_category,
        processing_time_secs
    FROM analytics_prod_gold.fct_prompt_analysis
    WHERE user_id = :uid
    ORDER BY query_timestamp DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"uid": user_id})
    return df


@st.cache_data(ttl=300)
def load_session_explorer(session_id: str):
    """Detailed session drill-down with JSONB event data."""
    engine = get_database_connection()
    query = """
    SELECT
        session_id,
        session_date,
        started_at,
        ended_at,
        session_duration_seconds,
        initiation_surface,
        save_count,
        share_count,
        prompts_detail,
        cards_generated_detail,
        cards_liked_list,
        cards_disliked_list,
        saves_detail,
        shares_detail,
        event_timeline,
        total_events,
        explorer_prompt_count,
        total_cards_generated,
        cards_liked,
        cards_disliked,
        total_share_viewers
    FROM analytics_prod_gold.fct_session_explorer
    WHERE session_id = :sid
    LIMIT 1
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"sid": session_id})
    return df


@st.cache_data(ttl=300)
def load_user_conversions(user_id: str):
    """Conversion events for a specific user."""
    engine = get_database_connection()
    query = """
    SELECT
        card_id,
        place_name,
        action_type,
        action_timestamp,
        was_saved_first,
        time_from_save_to_conversion_minutes,
        session_id,
        was_prompt_initiated,
        prompt_text
    FROM analytics_prod_gold.fct_conversions
    WHERE user_id = :uid
    ORDER BY action_timestamp DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"uid": user_id})
    return df


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════


def fmt_duration(seconds):
    """Format seconds into a human-readable string."""
    if pd.isna(seconds) or seconds is None:
        return "—"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


def archetype_badge(archetype):
    """Return emoji + label for user archetype."""
    mapping = {
        "power_planner": "Power Planner",
        "planner": "Planner",
        "saver": "Saver",
        "browser": "Browser",
        "one_and_done": "One & Done",
    }
    return mapping.get(archetype, archetype or "Unknown")


def churn_badge(risk):
    """Return styled churn risk label."""
    mapping = {
        "low": "Low",
        "medium": "Medium",
        "high": "High",
    }
    return mapping.get(risk, risk or "—")


def pct_fmt(val):
    """Format a decimal/percentage value."""
    if pd.isna(val) or val is None:
        return "—"
    return f"{val:.1f}%"


def parse_jsonb(val):
    """Safely parse a JSONB/json column value."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return []
    return val


# ══════════════════════════════════════════════════════════════════════════════
# PAGE CONTENT
# ══════════════════════════════════════════════════════════════════════════════

st.title("Power User Deep Dive")
st.caption("Explore the activity of users who have returned more than 5 times.")

# Refresh
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

try:
    # ── Load power users ──────────────────────────────────────────────────
    power_users = load_power_users()

    if power_users.empty:
        st.warning("No power users found (users with >5 sessions).")
        st.stop()

    st.info(
        f"**{len(power_users)} power users** identified (>5 sessions).",
        icon="⚡",
    )

    # ── User selector ─────────────────────────────────────────────────────
    # Build a display label combining username + email for easy searching
    power_users["display_label"] = power_users.apply(
        lambda r: f"{r['username'] or '(no username)'} — {r['email'] or '(no email)'}",
        axis=1,
    )

    col_filter1, col_filter2 = st.columns([3, 1])
    with col_filter1:
        selected_label = st.selectbox(
            "Select a user (search by username or email)",
            options=power_users["display_label"].tolist(),
            index=0,
            help="Type to search by username or email address.",
        )
    with col_filter2:
        st.metric("Total Power Users", len(power_users))

    # Resolve selected user
    selected_idx = power_users.index[
        power_users["display_label"] == selected_label
    ][0]
    user = power_users.loc[selected_idx]
    uid = str(user["user_id"])

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 1 — USER PROFILE CARD
    # ══════════════════════════════════════════════════════════════════════

    st.subheader("User Profile")

    pc1, pc2, pc3, pc4 = st.columns(4)

    with pc1:
        st.markdown(f"**Username:** {user['username'] or '—'}")
        st.markdown(f"**Email:** {user['email'] or '—'}")
        st.markdown(f"**Full Name:** {user['full_name'] or '—'}")

    with pc2:
        st.markdown(f"**Archetype:** {archetype_badge(user['user_archetype'])}")
        st.markdown(
            f"**Segment:** {'Planner' if user['is_planner'] else 'Passenger'}"
        )
        st.markdown(f"**Churn Risk:** {churn_badge(user['churn_risk'])}")

    with pc3:
        st.markdown(f"**Signed Up:** {user['signup_date']}")
        st.markdown(f"**Activated:** {user['activation_date'] or '—'}")
        st.markdown(f"**Trigger:** {user['activation_trigger'] or '—'}")

    with pc4:
        st.markdown(f"**Last Active:** {user['last_activity_date'] or '—'}")
        st.markdown(
            f"**Days Since Activity:** {int(user['days_since_last_activity']) if pd.notna(user['days_since_last_activity']) else '—'}"
        )
        st.markdown(f"**Referral Source:** {user['referral_source'] or '—'}")

    # Interest tags
    interests = []
    for col, label in [
        ("likes_adventure", "Adventure"),
        ("likes_dining", "Dining"),
        ("likes_drinks", "Drinks"),
        ("likes_culture", "Culture"),
        ("likes_entertainment", "Entertainment"),
        ("likes_health", "Health"),
    ]:
        if user.get(col):
            interests.append(label)

    if interests:
        st.markdown("**Interests:** " + ", ".join(interests))

    # Social context preferences
    social_prefs = []
    for col, label in [
        ("prefers_solo", "Solo"),
        ("prefers_friends", "Friends"),
        ("prefers_family", "Family"),
        ("prefers_date", "Date"),
    ]:
        if user.get(col):
            social_prefs.append(label)

    if social_prefs:
        st.markdown("**Social Preferences:** " + ", ".join(social_prefs))

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 2 — LIFETIME METRICS
    # ══════════════════════════════════════════════════════════════════════

    st.subheader("Lifetime Metrics")

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Sessions", int(user["total_sessions"]))
    m2.metric("Prompts", int(user["total_prompts"]))
    m3.metric("Swipes", int(user["total_swipes"]))
    m4.metric("Saves", int(user["total_saves"]))
    m5.metric("Shares", int(user["total_shares"]))
    m6.metric("Conversions", int(user["total_conversions"]))

    m7, m8, m9, m10, m11, m12 = st.columns(6)
    m7.metric("Days Active", int(user["days_active"]) if pd.notna(user["days_active"]) else "—")
    m8.metric(
        "Avg Saves/Session",
        f"{user['avg_saves_per_session']:.1f}"
        if pd.notna(user["avg_saves_per_session"])
        else "—",
    )
    m9.metric(
        "Avg Session Duration",
        fmt_duration(user["avg_session_duration_seconds"]),
    )
    m10.metric(
        "Swipe-Save Rate",
        pct_fmt(user["swipe_to_save_rate"] * 100)
        if pd.notna(user["swipe_to_save_rate"])
        else "—",
    )
    m11.metric("Boards Created", int(user["total_boards_created"]))
    m12.metric("Multiplayer Sessions", int(user["total_multiplayer_sessions"]))

    # Retention flags
    ret_cols = st.columns(4)
    ret_cols[0].metric(
        "Retained D7",
        "Yes" if user.get("retained_d7") else "No",
    )
    ret_cols[1].metric(
        "Retained D30",
        "Yes" if user.get("retained_d30") else "No",
    )
    ret_cols[2].metric(
        "Retained D60",
        "Yes" if user.get("retained_d60") else "No",
    )
    ret_cols[3].metric(
        "Churned?",
        "Yes" if user.get("is_churned") else "No",
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 3 — ENGAGEMENT TRAJECTORY (weekly chart)
    # ══════════════════════════════════════════════════════════════════════

    st.subheader("Engagement Trajectory")

    trajectory = load_user_engagement_trajectory(uid)

    if trajectory.empty:
        st.info("No engagement trajectory data available for this user.")
    else:
        trajectory["activity_week"] = pd.to_datetime(trajectory["activity_week"])

        traj_metric = st.radio(
            "Metric",
            ["sessions_count", "prompts_count", "saves_count", "shares_count", "days_active_in_week"],
            horizontal=True,
            format_func=lambda x: x.replace("_", " ").title(),
        )

        fig_traj = go.Figure()
        fig_traj.add_trace(
            go.Scatter(
                x=trajectory["activity_week"],
                y=trajectory[traj_metric],
                mode="lines+markers",
                line=dict(color=PINK, width=2),
                marker=dict(size=6, color=PINK),
                name=traj_metric.replace("_", " ").title(),
                hovertemplate="%{x|%d %b %Y}<br>%{y}<extra></extra>",
            )
        )
        fig_traj.update_layout(
            height=320,
            margin=dict(l=20, r=20, t=30, b=20),
            xaxis_title="Week",
            yaxis_title=traj_metric.replace("_", " ").title(),
            plot_bgcolor="white",
            hovermode="x unified",
        )
        fig_traj.update_xaxes(showgrid=True, gridcolor="#F0F0F0")
        fig_traj.update_yaxes(showgrid=True, gridcolor="#F0F0F0")
        st.plotly_chart(fig_traj, use_container_width=True)

        # Cumulative metrics
        if "cumulative_saves" in trajectory.columns:
            cum_cols = st.columns(3)
            latest = trajectory.iloc[-1]
            cum_cols[0].metric(
                "Cumulative Saves",
                int(latest["cumulative_saves"])
                if pd.notna(latest.get("cumulative_saves"))
                else "—",
            )
            cum_cols[1].metric(
                "Cumulative Shares",
                int(latest["cumulative_shares"])
                if pd.notna(latest.get("cumulative_shares"))
                else "—",
            )
            cum_cols[2].metric(
                "Cumulative Sessions",
                int(latest["cumulative_sessions"])
                if pd.notna(latest.get("cumulative_sessions"))
                else "—",
            )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 4 — SESSION HISTORY
    # ══════════════════════════════════════════════════════════════════════

    st.subheader("Session History")

    sessions = load_user_sessions(uid)

    if sessions.empty:
        st.info("No session data found.")
    else:
        st.caption(f"{len(sessions)} sessions recorded.")

        # Summary row
        sc1, sc2, sc3, sc4 = st.columns(4)
        sc1.metric(
            "Sessions w/ Save",
            f"{sessions['has_save'].sum()} ({100 * sessions['has_save'].mean():.0f}%)",
        )
        sc2.metric(
            "Sessions w/ Share",
            f"{sessions['has_share'].sum()} ({100 * sessions['has_share'].mean():.0f}%)",
        )
        sc3.metric(
            "PSR Broad Sessions",
            f"{sessions['meets_psr_broad'].sum()} ({100 * sessions['meets_psr_broad'].mean():.0f}%)",
        )
        sc4.metric(
            "Zero-Value Sessions",
            f"{sessions['is_no_value_session'].sum()} ({100 * sessions['is_no_value_session'].mean():.0f}%)",
        )

        # Session table
        display_sessions = sessions.copy()
        display_sessions["duration"] = display_sessions[
            "session_duration_seconds"
        ].apply(fmt_duration)
        display_sessions["started"] = pd.to_datetime(
            display_sessions["started_at"]
        ).dt.strftime("%Y-%m-%d %H:%M")

        session_table = display_sessions[
            [
                "session_id",
                "started",
                "duration",
                "initiation_surface",
                "save_count",
                "share_count",
                "has_save",
                "has_share",
                "meets_psr_broad",
                "is_no_value_session",
                "intent_strength",
            ]
        ].rename(
            columns={
                "session_id": "Session ID",
                "started": "Started",
                "duration": "Duration",
                "initiation_surface": "Surface",
                "save_count": "Saves",
                "share_count": "Shares",
                "has_save": "Save?",
                "has_share": "Share?",
                "meets_psr_broad": "PSR?",
                "is_no_value_session": "Zero-Value?",
                "intent_strength": "Intent",
            }
        )

        st.dataframe(session_table, use_container_width=True, hide_index=True)

        # ── Session drill-down ────────────────────────────────────────────
        st.markdown("#### Session Drill-Down")
        st.caption("Select a session above to see its full event timeline.")

        session_options = display_sessions.apply(
            lambda r: f"{r['started']} — {r['save_count']} saves, {r['share_count']} shares ({fmt_duration(r['session_duration_seconds'])})",
            axis=1,
        ).tolist()

        selected_session_label = st.selectbox(
            "Choose session", session_options, index=0
        )
        selected_session_idx = session_options.index(selected_session_label)
        selected_session_id = str(
            display_sessions.iloc[selected_session_idx]["session_id"]
        )

        explorer = load_session_explorer(selected_session_id)

        if not explorer.empty:
            exp = explorer.iloc[0]

            # Session overview
            ec1, ec2, ec3, ec4, ec5 = st.columns(5)
            ec1.metric("Prompts", int(exp.get("explorer_prompt_count", 0)))
            ec2.metric("Cards Generated", int(exp.get("total_cards_generated", 0)))
            ec3.metric("Liked", int(exp.get("cards_liked", 0)))
            ec4.metric("Disliked", int(exp.get("cards_disliked", 0)))
            ec5.metric("Share Viewers", int(exp.get("total_share_viewers", 0)))

            # Prompts
            prompts_data = parse_jsonb(exp.get("prompts_detail"))
            if prompts_data:
                with st.expander(f"Prompts ({len(prompts_data)})", expanded=True):
                    for i, p in enumerate(prompts_data, 1):
                        query_text = p.get("query_text", "—")
                        pack_name = p.get("pack_name", "—")
                        location = p.get("location", "")
                        ts = p.get("timestamp", "")
                        total_cards = p.get("total_cards_in_pack", "—")
                        loc_str = f" | {location}" if location else ""
                        st.markdown(
                            f"**{i}.** \"{query_text}\"{loc_str}  \n"
                            f"*Pack:* {pack_name} | *Cards:* {total_cards} | *Time:* {ts}"
                        )

            # Saves
            saves_data = parse_jsonb(exp.get("saves_detail"))
            if saves_data:
                with st.expander(f"Saves ({len(saves_data)})"):
                    for s in saves_data:
                        card_name = s.get("card_name", "Unknown")
                        category = s.get("category", "")
                        board = s.get("board_name", "default")
                        saved_at = s.get("saved_at", "")
                        st.markdown(
                            f"- **{card_name}** ({category}) -> Board: *{board}* at {saved_at}"
                        )

            # Shares
            shares_data = parse_jsonb(exp.get("shares_detail"))
            if shares_data:
                with st.expander(f"Shares ({len(shares_data)})"):
                    for s in shares_data:
                        share_type = s.get("share_type", "—")
                        channel = s.get("share_channel", "—")
                        board = s.get("board_name", "—")
                        viewers = s.get("unique_viewers", 0)
                        interactions = s.get("total_interactions", 0)
                        st.markdown(
                            f"- **{share_type}** via {channel} — Board: *{board}*  \n"
                            f"  {viewers} viewers | {interactions} interactions"
                        )

            # Event timeline
            timeline_data = parse_jsonb(exp.get("event_timeline"))
            if timeline_data:
                with st.expander(
                    f"Event Timeline ({len(timeline_data)} events)"
                ):
                    for ev in timeline_data:
                        event = ev.get("event", "—")
                        ts = ev.get("timestamp", "")
                        card = ev.get("card_name", "")
                        card_str = f" — {card}" if card else ""
                        st.markdown(f"- `{ts}` **{event}**{card_str}")
        else:
            st.info("No explorer data available for this session.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 5 — PROMPT ANALYSIS
    # ══════════════════════════════════════════════════════════════════════

    st.subheader("Dextr Prompt Analysis")

    prompts = load_user_prompts(uid)

    if prompts.empty:
        st.info("No prompt data found for this user.")
    else:
        st.caption(f"{len(prompts)} prompts recorded.")

        # Summary
        pc1, pc2, pc3, pc4 = st.columns(4)
        pc1.metric("Total Prompts", len(prompts))
        pc2.metric(
            "Avg Save Rate",
            pct_fmt(prompts["save_rate"].mean() * 100)
            if prompts["save_rate"].notna().any()
            else "—",
        )
        pc3.metric(
            "Zero-Save Prompts",
            f"{prompts['zero_save_prompt'].sum()} ({100 * prompts['zero_save_prompt'].mean():.0f}%)",
        )
        pc4.metric(
            "Refinement Prompts",
            f"{prompts['is_refinement'].sum()} ({100 * prompts['is_refinement'].mean():.0f}%)",
        )

        # Intent distribution
        if prompts["prompt_intent"].notna().any():
            intent_counts = prompts["prompt_intent"].value_counts().reset_index()
            intent_counts.columns = ["Intent", "Count"]
            fig_intent = px.bar(
                intent_counts,
                x="Intent",
                y="Count",
                color_discrete_sequence=[PINK],
            )
            fig_intent.update_layout(
                height=280,
                margin=dict(l=20, r=20, t=30, b=20),
                plot_bgcolor="white",
                title="Prompt Intent Distribution",
            )
            st.plotly_chart(fig_intent, use_container_width=True)

        # Prompt table
        prompt_table = prompts[
            [
                "query_text",
                "query_date",
                "prompt_intent",
                "prompt_specificity",
                "cards_generated",
                "cards_saved",
                "save_rate",
                "led_to_save",
                "led_to_share",
                "is_refinement",
                "performance_category",
            ]
        ].copy()

        prompt_table["save_rate"] = prompt_table["save_rate"].apply(
            lambda x: f"{x:.1%}" if pd.notna(x) else "—"
        )
        prompt_table = prompt_table.rename(
            columns={
                "query_text": "Prompt",
                "query_date": "Date",
                "prompt_intent": "Intent",
                "prompt_specificity": "Specificity",
                "cards_generated": "Cards",
                "cards_saved": "Saved",
                "save_rate": "Save Rate",
                "led_to_save": "-> Save?",
                "led_to_share": "-> Share?",
                "is_refinement": "Refinement?",
                "performance_category": "Speed",
            }
        )

        st.dataframe(prompt_table, use_container_width=True, hide_index=True)

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 6 — CONVERSIONS
    # ══════════════════════════════════════════════════════════════════════

    st.subheader("Conversions")

    conversions = load_user_conversions(uid)

    if conversions.empty:
        st.info("No conversion events found for this user.")
    else:
        st.caption(f"{len(conversions)} conversion events recorded.")

        conv_table = conversions.copy()
        conv_table["action_timestamp"] = pd.to_datetime(
            conv_table["action_timestamp"]
        ).dt.strftime("%Y-%m-%d %H:%M")
        conv_table["time_from_save_to_conversion_minutes"] = conv_table[
            "time_from_save_to_conversion_minutes"
        ].apply(lambda x: f"{x:.0f} min" if pd.notna(x) else "—")

        conv_display = conv_table[
            [
                "place_name",
                "action_type",
                "action_timestamp",
                "was_saved_first",
                "time_from_save_to_conversion_minutes",
                "was_prompt_initiated",
                "prompt_text",
            ]
        ].rename(
            columns={
                "place_name": "Place",
                "action_type": "Action",
                "action_timestamp": "When",
                "was_saved_first": "Saved First?",
                "time_from_save_to_conversion_minutes": "Save->Conv Time",
                "was_prompt_initiated": "Prompt-Initiated?",
                "prompt_text": "Prompt",
            }
        )

        st.dataframe(conv_display, use_container_width=True, hide_index=True)

    # ── Footer ────────────────────────────────────────────────────────────
    add_deck_footer()

except Exception as e:
    st.error(
        f"Error loading data. Please check your database connection.\n\n**Details:** {e}"
    )
    st.caption(
        "If this persists, try refreshing or check that all gold layer tables have been built."
    )
