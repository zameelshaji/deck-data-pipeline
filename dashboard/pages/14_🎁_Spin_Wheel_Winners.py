"""DECK Spin Wheel Winners — kanban for winner outreach + gift card fulfillment."""

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

STATUS_COLUMNS = [
    ("to_contact", "To Contact"),
    ("contacted",  "Contacted"),
    ("sent",       "Gift Card Sent"),
    ("redeemed",   "Redeemed"),
]

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "operator_email" not in st.session_state:
    st.session_state.operator_email = ""
if "sending_form_for" not in st.session_state:
    st.session_state.sending_form_for = None  # outreach_id of card showing the send form

# ---------------------------------------------------------------------------
# Title + operator selector
# ---------------------------------------------------------------------------
st.title("\U0001f381 Spin Wheel Winners")
st.caption("Kanban board for winner outreach and gift card fulfillment.")

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
# Section 2 — Kanban
# ---------------------------------------------------------------------------
st.subheader("Outreach")
board = load_spin_wheel_winners_board(start_date, end_date, search_term, include_skipped=show_skipped)

if board.empty:
    st.info("No winners in this range.")
else:
    by_status = {status: board[board["status"] == status] for status, _ in STATUS_COLUMNS}
    skipped_df = board[board["status"] == "skipped"]

    status_counts = " · ".join(
        f"**{label}**: {len(by_status[status])}" for status, label in STATUS_COLUMNS
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

    def _render_card(row):
        outreach_id = row["outreach_id"]
        status = row["status"]
        with st.container(border=True):
            display_name = row["display_name"] or "(unknown user)"
            st.markdown(f"**{display_name}**")
            if row.get("email"):
                st.caption(row["email"])
            st.caption(f"\U0001f4cd {row['place_name']}")
            st.caption(f"Won {_days_ago(row['won_at'])}")

            notes_preview = row.get("notes") or ""
            if notes_preview:
                preview = notes_preview[:80] + ("…" if len(notes_preview) > 80 else "")
                st.caption(f"\U0001f4dd {preview}")

            if status == "sent" and row.get("gift_card_code"):
                amt = row.get("gift_card_value")
                amt_str = f" · £{float(amt):.2f}" if amt is not None and pd.notna(amt) else ""
                st.caption(f"\U0001f3ab `{row['gift_card_code']}`{amt_str}")

            # ----- Action buttons per status -----
            if status == "to_contact":
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Mark contacted", key=f"contact_{outreach_id}", use_container_width=True):
                        if not operator_email:
                            st.warning("Enter your email above first.")
                        elif update_winner_outreach_status(outreach_id, "contacted", operator_email):
                            st.toast("Marked contacted")
                            st.rerun()
                with c2:
                    if st.button("Skip", key=f"skip_{outreach_id}", use_container_width=True):
                        if update_winner_outreach_status(outreach_id, "skipped", operator_email):
                            st.rerun()

            elif status == "contacted":
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
                else:
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        if st.button("Mark sent", key=f"send_{outreach_id}", use_container_width=True):
                            st.session_state.sending_form_for = outreach_id
                            st.rerun()
                    with c2:
                        if st.button("Undo", key=f"undo_{outreach_id}", use_container_width=True):
                            if update_winner_outreach_status(outreach_id, "to_contact", operator_email):
                                st.rerun()
                    with c3:
                        if st.button("Skip", key=f"skipc_{outreach_id}", use_container_width=True):
                            if update_winner_outreach_status(outreach_id, "skipped", operator_email):
                                st.rerun()

            elif status == "sent":
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Mark redeemed", key=f"redeem_{outreach_id}", use_container_width=True):
                        if update_winner_outreach_status(outreach_id, "redeemed", operator_email):
                            st.rerun()
                with c2:
                    if st.button("Skip", key=f"skips_{outreach_id}", use_container_width=True):
                        if update_winner_outreach_status(outreach_id, "skipped", operator_email):
                            st.rerun()

            elif status == "redeemed":
                if st.button("Reopen", key=f"reopen_{outreach_id}", use_container_width=True):
                    if update_winner_outreach_status(outreach_id, "sent", operator_email):
                        st.rerun()

            elif status == "skipped":
                if st.button("Restore", key=f"restore_{outreach_id}", use_container_width=True):
                    if update_winner_outreach_status(outreach_id, "to_contact", operator_email):
                        st.rerun()

            # Notes expander
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

    cols = st.columns(4)
    for (status, label), col in zip(STATUS_COLUMNS, cols):
        with col:
            count = len(by_status[status])
            st.markdown(f"#### {label} ({count})")
            if count == 0:
                st.caption("—")
            for _, row in by_status[status].iterrows():
                _render_card(row)

    if not skipped_df.empty:
        with st.expander(f"Skipped ({len(skipped_df)})", expanded=False):
            for _, row in skipped_df.iterrows():
                _render_card(row)

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
        "updated_at":   "When",
        "display_name": "Winner",
        "place_name":   "Place",
        "status":       "Status",
        "assigned_to":  "Assigned to",
        "sent_by":      "Sent by",
    })
    st.dataframe(audit_display, hide_index=True, use_container_width=True)

add_deck_footer()
