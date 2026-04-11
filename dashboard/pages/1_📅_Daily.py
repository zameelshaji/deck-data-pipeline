"""DECK Daily Report — replicates the CEO's SwipeOnDeck Daily Report as an interactive page.

Every section is keyed off a single date picker (defaults to yesterday). Change
the date to time-travel: the all-time topline, 7-day trend, activation funnel,
category popularity, and per-user activity all re-roll to reflect that date.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import date, timedelta
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.data_loader import (
    load_daily_topline_kpis,
    load_daily_alltime_topline,
    load_daily_7day_trend,
    load_daily_activation_checklist,
    load_daily_new_signups_status,
    load_daily_category_popularity,
    load_daily_places_flagged,
    load_daily_top_liked_places,
    load_daily_weekly_intensity,
    load_daily_user_activity,
)

st.set_page_config(
    page_title="Daily Report | DECK Analytics",
    page_icon="📅",
    layout="wide"
)

apply_deck_branding()

st.title("Daily Report")
st.caption("The CEO's daily snapshot — pick any date to time-travel the numbers.")

# ----------------------------------------------------------------------------
# Date picker — default to yesterday
# ----------------------------------------------------------------------------
with st.sidebar:
    st.header("Filters")
    report_date = st.date_input(
        "Report Date",
        value=date.today() - timedelta(days=1),
        max_value=date.today(),
        help="All metrics below filter to this single date. Defaults to yesterday.",
    )
    min_swipe_threshold = st.number_input(
        "Places-to-remove min swipes",
        min_value=1, max_value=20, value=4, step=1,
        help="A place is flagged only if it had at least this many swipes on the report date."
    )

report_date_str = str(report_date)
st.markdown(f"### {report_date.strftime('%A, %B %d, %Y')}")

# ============================================================================
# Section A: Top-line KPI tiles (matches CEO report page 1)
# ============================================================================
kpi = load_daily_topline_kpis(report_date_str)

if not kpi:
    st.warning("No data for the selected date. Try a different date.")
    st.stop()

def _fmt_int(v):
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return "—"

def _fmt_pct(v):
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v) * 100:.1f}%"

# Row 1: the 4 hero tiles from the CEO report
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("New Signups", _fmt_int(kpi.get('new_signups', 0)))
with c2:
    st.metric("DAU", _fmt_int(kpi.get('dau', 0)))
with c3:
    st.metric("Total Swipes", _fmt_int(kpi.get('total_swipes', 0)))
with c4:
    st.metric("Like Rate", _fmt_pct(kpi.get('like_rate')),
              help=f"{_fmt_int(kpi.get('total_right_swipes', 0))} right of {_fmt_int(kpi.get('total_swipes', 0))} total swipes")

# Row 2: the 4 secondary tiles
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Saves", _fmt_int(kpi.get('saves', 0)))
with c2:
    st.metric("Prompts", _fmt_int(kpi.get('prompts', 0)))
with c3:
    st.metric("Onboarding", _fmt_int(kpi.get('onboarding_completed', 0)),
              help="Users who completed onboarding on this date (signed up AND finished onboarding)")
with c4:
    st.metric("Total Events", _fmt_int(kpi.get('total_events', 0)))

st.divider()

# ============================================================================
# Section B: All-time topline (cumulative as of report_date)
# ============================================================================
st.subheader("All-time Topline")
st.caption(f"Cumulative through {report_date.strftime('%b %d, %Y')}")

alltime = load_daily_alltime_topline(report_date_str)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Total Signups", _fmt_int(alltime.get('total_signups', 0)))
with c2:
    st.metric("Activations", _fmt_int(alltime.get('total_activations', 0)),
              help="Users who have prompted, saved, or shared at least once")
with c3:
    st.metric("Prompters", _fmt_int(alltime.get('total_prompters', 0)),
              help="Distinct users who have submitted a Dextr query")
with c4:
    st.metric("Total Prompts", _fmt_int(alltime.get('total_prompts', 0)))

st.divider()

# ============================================================================
# Section C: 7-Day Trend
# ============================================================================
st.subheader("7-Day Trend")
st.caption(f"Rolling 7 days ending {report_date.strftime('%b %d')}")

trend_df = load_daily_7day_trend(report_date_str)

if not trend_df.empty:
    metric_choice = st.radio(
        "Metric",
        options=["DAU", "New Signups", "Total Events", "Saves", "Prompts", "Like Rate"],
        horizontal=True,
        label_visibility="collapsed",
    )
    metric_col = {
        "DAU": "dau",
        "New Signups": "new_signups",
        "Total Events": "total_events",
        "Saves": "saves",
        "Prompts": "prompts",
        "Like Rate": "like_rate",
    }[metric_choice]

    display_df = trend_df.copy()
    display_df['day'] = pd.to_datetime(display_df['day'])
    y_vals = display_df[metric_col].astype(float)
    if metric_col == 'like_rate':
        y_vals = y_vals * 100
        y_label = "Like Rate (%)"
        text_vals = [f"{v:.1f}%" if pd.notna(v) else "—" for v in y_vals]
    else:
        y_label = metric_choice
        text_vals = [f"{int(v):,}" if pd.notna(v) else "—" for v in y_vals]

    fig = go.Figure(go.Bar(
        x=display_df['day'],
        y=y_vals,
        text=text_vals,
        textposition='outside',
        marker_color=['#E91E8C' if d.date() == report_date else BRAND_COLORS['info']
                      for d in display_df['day']],
    ))
    fig.update_layout(
        yaxis_title=y_label,
        font=dict(family="Inter, system-ui, sans-serif", size=13),
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=40, r=20, t=20, b=40),
        xaxis=dict(showgrid=False, tickformat='%a %b %d'),
        yaxis=dict(showgrid=True, gridcolor=BRAND_COLORS['border']),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No trend data available.")

st.divider()

# ============================================================================
# Section D: Activation Checklist Funnel — New Signups
# ============================================================================
st.subheader("Activation Checklist Funnel — New Signups")

checklist = load_daily_activation_checklist(report_date_str)
new_signups_n = int(checklist.get('new_signups', 0) or 0)
deck_n = int(checklist.get('deck_created', 0) or 0)
saves_n = int(checklist.get('places_saved', 0) or 0)
mp_n = int(checklist.get('multiplayer_started', 0) or 0)
all_three_n = int(checklist.get('all_three', 0) or 0)
stuck_no_deck = int(checklist.get('stuck_no_deck', 0) or 0)
stuck_at_saves = int(checklist.get('stuck_at_saves', 0) or 0)
stuck_at_mp = int(checklist.get('stuck_at_mp', 0) or 0)

if new_signups_n == 0:
    st.info("No new signups on this date — nothing to funnel.")
else:
    st.caption(
        f"**{all_three_n} / {new_signups_n}** new users completed all 3 steps and earned the spin wheel"
    )

    def _pct(n):
        return (n / new_signups_n) if new_signups_n else 0

    funnel_df = pd.DataFrame({
        "Step": [
            "1. Deck created",
            "2. 3+ places saved",
            "3. Multiplayer started",
            "All 3 complete (spin wheel unlocked)",
        ],
        "Count": [deck_n, saves_n, mp_n, all_three_n],
        "% of new users": [_pct(deck_n), _pct(saves_n), _pct(mp_n), _pct(all_three_n)],
    })
    st.dataframe(
        funnel_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Step": st.column_config.TextColumn("Step"),
            "Count": st.column_config.NumberColumn("Count", format="%d"),
            "% of new users": st.column_config.NumberColumn("% of new users", format="%.0f%%"),
        },
    )

    st.caption(
        f"**Drop-off:** {stuck_no_deck} never created a deck · "
        f"{stuck_at_saves} stuck at saves step · "
        f"{stuck_at_mp} stuck at multiplayer step"
    )

st.divider()

# ============================================================================
# Section E: New Signups — Onboarding & Checklist Status table
# ============================================================================
st.subheader("New Signups — Onboarding & Checklist Status")

signups_df = load_daily_new_signups_status(report_date_str)

if signups_df.empty:
    st.info("No new signups on this date.")
else:
    onboarded_n = int(signups_df['onboarded'].sum())
    total_n = len(signups_df)
    pct = (onboarded_n / total_n * 100) if total_n else 0
    st.caption(f"{onboarded_n} of {total_n} completed onboarding ({pct:.0f}%)")

    # Render the checkmark table
    display = signups_df.copy()
    def _check(v):
        return "✓" if bool(v) else "—"
    for col in ['onboarded', 'deck_created', 'places_saved', 'multiplayer_started', 'all_three']:
        display[col] = display[col].apply(_check)

    display = display.rename(columns={
        'display_name': 'Name',
        'onboarded': 'Onboarded',
        'deck_created': 'Deck',
        'places_saved': 'Saves',
        'multiplayer_started': 'MP',
        'all_three': 'All 3',
    })
    st.dataframe(display, use_container_width=True, hide_index=True)

st.divider()

# ============================================================================
# Section F: Category Popularity
# ============================================================================
st.subheader("Category Popularity")

cat_df = load_daily_category_popularity(report_date_str)

if cat_df.empty:
    st.info("No category swipes recorded on this date.")
else:
    display = cat_df.copy()
    display['Like %'] = (display['like_pct'].astype(float) * 100).round(0).astype(int).astype(str) + '%'
    display = display[['category', 'likes', 'dislikes', 'total', 'Like %']]
    display.columns = ['Category', 'Likes', 'Dislikes', 'Total', 'Like %']
    st.dataframe(display, use_container_width=True, hide_index=True)

st.divider()

# ============================================================================
# Section G: Places Flagged for Removal
# ============================================================================
st.subheader("Places Flagged for Removal (Candidates to Review)")
st.caption(
    f"Places with more dislikes than likes on this date, minimum {int(min_swipe_threshold)} swipes. "
    "Review before removing from the database."
)

flagged_df = load_daily_places_flagged(report_date_str, min_swipes=int(min_swipe_threshold))

if flagged_df.empty:
    st.info(f"No places met the flagging criteria on {report_date.strftime('%b %d')}.")
else:
    display = flagged_df.copy()
    display['Dislike %'] = (display['dislike_pct'].astype(float) * 100).round(0).astype(int).astype(str) + '%'
    display = display[['id', 'name', 'area', 'likes', 'dislikes', 'total', 'Dislike %']]
    display.columns = ['ID', 'Name', 'Area', 'Likes', 'Dislikes', 'Total', 'Dislike %']
    st.dataframe(display, use_container_width=True, hide_index=True)

st.divider()

# ============================================================================
# Section H: Top Liked Places
# ============================================================================
st.subheader("Top Liked Places")

top_df = load_daily_top_liked_places(report_date_str, limit=10)

if top_df.empty:
    st.info("No likes recorded on this date.")
else:
    display = top_df.copy()
    display.columns = ['ID', 'Name', 'Area', 'Likes', 'Dislikes']
    st.dataframe(display, use_container_width=True, hide_index=True)

st.divider()

# ============================================================================
# Section I: Weekly Engagement Intensity — Per Active Activated User
# ============================================================================
st.subheader("Weekly Engagement Intensity — Per Active Activated User")
st.caption(
    "Average actions per week per returning activated user. "
    "Saves and shares are only instrumented from the week of 2026-01-26 onward."
)

intensity_df = load_daily_weekly_intensity(report_date_str, weeks=12)

if intensity_df.empty:
    st.info("No weekly intensity data available.")
else:
    idf = intensity_df.copy()
    idf['week_start'] = pd.to_datetime(idf['week_start'])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=idf['week_start'], y=idf['avg_swipes'].astype(float),
        name='Avg Swipes', line=dict(color=BRAND_COLORS['info'], width=2),
        mode='lines+markers',
    ))
    fig.add_trace(go.Scatter(
        x=idf['week_start'], y=idf['avg_saves'].astype(float),
        name='Avg Saves', line=dict(color='#E91E8C', width=2),
        mode='lines+markers',
    ))
    fig.add_trace(go.Scatter(
        x=idf['week_start'], y=idf['avg_shares'].astype(float),
        name='Avg Shares', line=dict(color=BRAND_COLORS['success'], width=2),
        mode='lines+markers',
    ))
    fig.update_layout(
        yaxis_title="Avg per active activated user",
        xaxis_title="Week starting",
        font=dict(family="Inter, system-ui, sans-serif", size=13),
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=40, r=20, t=20, b=40),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor=BRAND_COLORS['border']),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Table view
    with st.expander("Weekly values table"):
        table = idf.copy()
        table.columns = ['Week Start', 'Active Activated Users', 'Avg Swipes', 'Avg Saves', 'Avg Shares']
        st.dataframe(table, use_container_width=True, hide_index=True)

st.divider()

# ============================================================================
# Section J: User Activity — Report Date
# ============================================================================
st.subheader(f"User Activity — {report_date.strftime('%b %d')}")

user_df = load_daily_user_activity(report_date_str)

if user_df.empty:
    st.info("No user activity on this date.")
else:
    display = user_df.copy()
    display['Like %'] = display['like_rate'].apply(
        lambda v: f"{float(v) * 100:.0f}%" if pd.notna(v) else "—"
    )
    display['New?'] = display['is_new'].apply(lambda v: "✓" if bool(v) else "")
    display = display[[
        'display_name', 'likes', 'dislikes', 'Like %',
        'saves', 'boards_created', 'prompts', 'total_events', 'New?'
    ]]
    display.columns = ['Name', 'Likes', 'Dis.', 'Like %', 'Saves', 'Boards', 'Prompts', 'Total', 'New?']
    st.dataframe(display, use_container_width=True, hide_index=True)

    # Download button
    csv = user_df.to_csv(index=False)
    st.download_button(
        label="Download user activity CSV",
        data=csv,
        file_name=f"deck_user_activity_{report_date_str}.csv",
        mime="text/csv",
    )

add_deck_footer()
