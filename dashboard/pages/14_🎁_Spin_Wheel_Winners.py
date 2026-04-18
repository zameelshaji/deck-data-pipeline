"""DECK Spin Wheel Winners — sorted list for winner outreach + gift card fulfillment."""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date, datetime, timedelta, timezone

from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.data_loader import (
    load_spin_wheel_metrics,
    load_spin_wheel_daily_trend,
    load_spin_wheel_top_places,
    load_spin_wheel_winners_board,
    load_spin_wheel_audit_log,
    update_winner_outreach_status,
    update_winner_outreach_notes,
)

st.set_page_config(
    page_title="Spin Wheel Winners | DECK Analytics",
    page_icon="\U0001f381",
    layout="wide",
)

apply_deck_branding()

STATUS_ORDER = {"to_contact": 0, "contacted": 1, "sent": 2, "redeemed": 3, "skipped": 4}
STATUS_LABELS = {
    "to_contact": "To Contact",
    "contacted":  "Contacted",
    "sent":       "Gift Card Sent",
    "redeemed":   "Redeemed",
    "skipped":    "Skipped",
}
# For each status, the list of (target_status, label) the user can transition to.
ACTIONS = {
    "to_contact": [("contacted", "Mark contacted"), ("skipped", "Skip")],
    "contacted":  [("sent", "Mark gift card sent"), ("to_contact", "Back to To Contact"), ("skipped", "Skip")],
    "sent":       [("redeemed", "Mark redeemed"), ("skipped", "Skip")],
    "redeemed":   [("sent", "Reopen (back to Sent)")],
    "skipped":    [("to_contact", "Restore to To Contact")],
}

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "operator_email" not in st.session_state:
    st.session_state.operator_email = "isaac@swipeondeck.com"
if "sending_form_for" not in st.session_state:
    st.session_state.sending_form_for = None  # outreach_id of card showing the send form

# ---------------------------------------------------------------------------
# Title + operator selector
# ---------------------------------------------------------------------------
st.title("\U0001f381 Spin Wheel Winners")
st.caption("Sorted list of winners with per-row actions for outreach and gift card fulfillment.")

op_col1, op_col2 = st.columns([3, 2])
with op_col1:
    st.session_state.operator_email = st.text_input(
        "Your email (stamped on every action)",
        value=st.session_state.operator_email,
        placeholder="zac@deck.app",
    )
with op_col2:
    st.caption("")
    st.caption("Your email is recorded on `sent_by` and `assigned_to` when you move a card.")

operator_email = st.session_state.operator_email.strip() or None

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Filters")
    date_range = st.date_input(
        "Date Range (by win date)",
        value=(date.today() - timedelta(days=90), date.today()),
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = str(date_range[0]), str(date_range[1])
    else:
        start_date = str(date.today() - timedelta(days=90))
        end_date = str(date.today())

    search_term = st.text_input("Search", placeholder="username / email / place name")
    show_skipped = st.toggle("Include skipped", value=False)

# ---------------------------------------------------------------------------
# Section 1 — Mechanism metrics
# ---------------------------------------------------------------------------
st.subheader("Mechanism")
metrics = load_spin_wheel_metrics(start_date, end_date)

m1, m2, m3, m4 = st.columns(4)
total_spins = int(metrics.get("total_spins") or 0)
total_wins  = int(metrics.get("total_wins") or 0)
win_rate    = metrics.get("win_rate")
fulfillment = metrics.get("fulfillment_rate")

with m1:
    st.metric("Total spins", f"{total_spins:,}")
with m2:
    st.metric("Total wins", f"{total_wins:,}")
with m3:
    st.metric("Win rate", f"{float(win_rate) * 100:.1f}%" if win_rate is not None else "—")
with m4:
    st.metric(
        "Fulfillment rate",
        f"{float(fulfillment) * 100:.1f}%" if fulfillment is not None else "—",
        help="Wins moved to 'Gift Card Sent' or 'Redeemed' ÷ total wins in range.",
    )

chart_col_a, chart_col_b = st.columns([3, 2])
with chart_col_a:
    trend_df = load_spin_wheel_daily_trend(start_date, end_date)
    if not trend_df.empty:
        long_df = trend_df.melt(id_vars="day", value_vars=["spins", "wins"],
                                var_name="type", value_name="count")
        fig = px.line(
            long_df, x="day", y="count", color="type",
            color_discrete_map={"spins": BRAND_COLORS["text_secondary"], "wins": BRAND_COLORS["accent"]},
            title="Spins vs. wins per day",
        )
        fig.update_layout(height=300, margin=dict(l=10, r=10, t=40, b=10), legend_title_text="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("No activity in range.")

with chart_col_b:
    top_df = load_spin_wheel_top_places(start_date, end_date, limit=10)
    if not top_df.empty:
        top_df = top_df.sort_values("win_count")
        fig = px.bar(
            top_df, x="win_count", y="place_name", orientation="h",
            color_discrete_sequence=[BRAND_COLORS["accent"]],
            title="Top 10 places by wins",
        )
        fig.update_layout(height=300, margin=dict(l=10, r=10, t=40, b=10),
                          xaxis_title="", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("No wins in range.")

st.divider()

# ---------------------------------------------------------------------------
# Section 2 — Sorted winners list
# ---------------------------------------------------------------------------
st.subheader("Outreach")
board = load_spin_wheel_winners_board(start_date, end_date, search_term, include_skipped=show_skipped)

if board.empty:
    st.info("No winners in this range.")
else:
    board = board.copy()
    board["_order"] = board["status"].map(STATUS_ORDER).fillna(99)
    board = board.sort_values(["_order", "won_at"], ascending=[True, False])

    visible_statuses = [s for s in STATUS_ORDER if s != "skipped" or show_skipped]
    status_counts = " · ".join(
        f"**{STATUS_LABELS[s]}**: {int((board['status'] == s).sum())}"
        for s in visible_statuses
    )
    st.caption(status_counts)

    def _days_ago(ts):
        if pd.isna(ts):
            return "—"
        now = datetime.now(timezone.utc)
        if getattr(ts, "tzinfo", None) is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta_days = (now - ts).days
        if delta_days < 1:
            return "today"
        if delta_days == 1:
            return "1 day ago"
        return f"{delta_days} days ago"

    for _, row in board.iterrows():
        outreach_id = row["outreach_id"]
        status = row["status"]

        with st.container(border=True):
            info_col, meta_col, action_col = st.columns([3, 2, 3])

            with info_col:
                full_name = (row.get("full_name") or "").strip()
                username  = (row.get("username") or "").strip()
                email     = (row.get("email") or "").strip()

                header = full_name or username or email or "(unknown user)"
                st.markdown(f"**{header}**")

                sub_parts = []
                if username and username != header:
                    sub_parts.append(f"@{username}")
                if email and email != header:
                    sub_parts.append(email)
                if sub_parts:
                    st.caption(" · ".join(sub_parts))

                st.caption(f"\U0001f4cd {row['place_name']}")

            with meta_col:
                st.markdown(f"`{STATUS_LABELS[status]}`")
                st.caption(f"Won {_days_ago(row['won_at'])}")
                if status == "sent" and row.get("gift_card_code"):
                    amt = row.get("gift_card_value")
                    amt_str = f" · £{float(amt):.2f}" if amt is not None and pd.notna(amt) else ""
                    st.caption(f"\U0001f3ab `{row['gift_card_code']}`{amt_str}")

            with action_col:
                actions = ACTIONS.get(status, [])

                if st.session_state.sending_form_for == outreach_id:
                    with st.form(f"sendform_{outreach_id}", clear_on_submit=True):
                        gc_code = st.text_input("Gift card code", key=f"gc_code_{outreach_id}")
                        gc_value = st.number_input(
                            "Value (£)", min_value=0.0, step=5.0, key=f"gc_val_{outreach_id}"
                        )
                        submit, cancel = st.columns(2)
                        with submit:
                            submitted = st.form_submit_button("Confirm sent", use_container_width=True)
                        with cancel:
                            cancelled = st.form_submit_button("Cancel", use_container_width=True)
                        if submitted:
                            if not operator_email:
                                st.warning("Enter your email above first.")
                            elif not gc_code.strip():
                                st.warning("Gift card code is required.")
                            elif update_winner_outreach_status(
                                outreach_id, "sent", operator_email,
                                gift_card_code=gc_code.strip(),
                                gift_card_value=float(gc_value) if gc_value else None,
                            ):
                                st.session_state.sending_form_for = None
                                st.toast("Gift card sent")
                                st.rerun()
                        elif cancelled:
                            st.session_state.sending_form_for = None
                            st.rerun()
                elif actions:
                    placeholder = "\u2014 Pick action \u2014"
                    label_to_target = {label: target for target, label in actions}
                    choice = st.selectbox(
                        "Action",
                        options=[placeholder] + list(label_to_target.keys()),
                        key=f"action_{outreach_id}",
                        label_visibility="collapsed",
                    )
                    apply_disabled = choice == placeholder
                    if st.button("Apply", key=f"apply_{outreach_id}",
                                 use_container_width=True, disabled=apply_disabled):
                        target_status = label_to_target[choice]
                        if target_status == "sent":
                            st.session_state.sending_form_for = outreach_id
                            st.rerun()
                        elif not operator_email:
                            st.warning("Enter your email above first.")
                        elif update_winner_outreach_status(outreach_id, target_status, operator_email):
                            st.rerun()

            with st.expander("\U0001f4dd Notes"):
                new_notes = st.text_area(
                    "Notes",
                    value=row.get("notes") or "",
                    key=f"notes_{outreach_id}",
                    label_visibility="collapsed",
                    height=80,
                )
                if st.button("Save notes", key=f"save_notes_{outreach_id}"):
                    if update_winner_outreach_notes(outreach_id, new_notes):
                        st.toast("Notes saved")
                        st.rerun()

st.divider()

# ---------------------------------------------------------------------------
# Section 3 — Audit strip
# ---------------------------------------------------------------------------
st.subheader("Recent activity")
audit = load_spin_wheel_audit_log(limit=20)
if audit.empty:
    st.caption("No activity yet.")
else:
    audit_display = audit.rename(columns={
        "updated_at":      "When",
        "full_name":       "Name",
        "username":        "Username",
        "email":           "Email",
        "place_name":      "Place",
        "status":          "Status",
        "assigned_to":     "Assigned to",
        "sent_by":         "Sent by",
        "gift_card_code":  "Gift card code",
        "gift_card_value": "Gift card £",
    })
    ordered_cols = [c for c in [
        "When", "Name", "Username", "Email", "Place", "Status",
        "Gift card code", "Gift card £", "Assigned to", "Sent by",
    ] if c in audit_display.columns]
    audit_display = audit_display[ordered_cols]
    st.dataframe(audit_display, hide_index=True, use_container_width=True)

add_deck_footer()
