"""DECK Daily Report — replicates the CEO's SwipeOnDeck Daily Report as an interactive page.

Every section is keyed off a single date picker (defaults to yesterday). Change
the date to time-travel: the 7-day trend, activation funnel, category popularity,
and per-user activity all re-roll to reflect that date.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import tempfile
import os
from datetime import date, timedelta
from fpdf import FPDF
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.data_loader import (
    load_daily_topline_kpis,
    load_daily_7day_trend,
    load_daily_activation_checklist,
    load_daily_new_signups_status,
    load_daily_category_popularity,
    load_daily_places_flagged,
    load_daily_top_liked_places,
    load_daily_weekly_intensity,
    load_daily_user_activity,
    load_latest_eqt_memo,
)

st.set_page_config(
    page_title="Daily Report | DECK Analytics",
    page_icon="📅",
    layout="wide"
)

apply_deck_branding()

st.title("Daily Report")

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

# Reserve a slot for the PDF download button (filled at the bottom after all data loads)
pdf_slot = st.empty()

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
# Section A2: EQT Insight Memo
# ============================================================================
st.subheader("EQT Insight Memo")
memo = load_latest_eqt_memo('daily', report_date_str)
if memo:
    ts = pd.to_datetime(memo['generated_at'])
    st.caption(
        f"Generated {ts.strftime('%b %d %Y %H:%M UTC')} · "
        f"{memo.get('model_version') or 'unknown model'}"
    )
    st.markdown(memo['memo_markdown'])
else:
    st.info(
        "No EQT memo for this date yet. Scheduled run: every day at 02:00 UTC "
        "(after the 01:00 UTC dbt rebuild)."
    )

st.divider()

# ============================================================================
# Section B: 7-Day Trend
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
# Section C: Activation Checklist Funnel — New Signups
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
# Section D: New Signups — Onboarding & Checklist Status table
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
        return "Y" if bool(v) else "-"
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
# Section E: Category Popularity
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
# Section F: Places Flagged for Removal
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
# Section G: Top Liked Places
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
# Section H: Weekly Engagement Intensity — Per Active Activated User
# ============================================================================
st.subheader("Weekly Engagement Intensity — Per Active Activated User")
st.caption(
    "Average actions per week per returning activated user. "
    "Saves and shares are only instrumented from the week of 2026-01-26 onward."
)

intensity_df = load_daily_weekly_intensity(report_date_str, weeks=12)

# Build the intensity chart (used both for display and PDF)
intensity_fig = None
if not intensity_df.empty:
    idf = intensity_df.copy()
    idf['week_start'] = pd.to_datetime(idf['week_start'])

    intensity_fig = go.Figure()
    intensity_fig.add_trace(go.Scatter(
        x=idf['week_start'], y=idf['avg_swipes'].astype(float),
        name='Avg Swipes', line=dict(color=BRAND_COLORS['info'], width=2),
        mode='lines+markers',
    ))
    intensity_fig.add_trace(go.Scatter(
        x=idf['week_start'], y=idf['avg_saves'].astype(float),
        name='Avg Saves', line=dict(color='#E91E8C', width=2),
        mode='lines+markers',
    ))
    intensity_fig.add_trace(go.Scatter(
        x=idf['week_start'], y=idf['avg_shares'].astype(float),
        name='Avg Shares', line=dict(color=BRAND_COLORS['success'], width=2),
        mode='lines+markers',
    ))
    intensity_fig.update_layout(
        yaxis_title="Avg per active activated user",
        xaxis_title="Week starting",
        font=dict(family="Inter, system-ui, sans-serif", size=13),
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=40, r=20, t=20, b=40),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor=BRAND_COLORS['border']),
    )
    st.plotly_chart(intensity_fig, use_container_width=True)

    # Table view
    with st.expander("Weekly values table"):
        table = idf.copy()
        table.columns = ['Week Start', 'Active Activated Users', 'Avg Swipes', 'Avg Saves', 'Avg Shares']
        st.dataframe(table, use_container_width=True, hide_index=True)
else:
    st.info("No weekly intensity data available.")

st.divider()

# ============================================================================
# Section I: User Activity — Report Date
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
    display['New?'] = display['is_new'].apply(lambda v: "Y" if bool(v) else "")
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


# ============================================================================
# PDF Generation
# ============================================================================

def _pdf_safe(s):
    """Replace Unicode characters unsupported by Helvetica with ASCII equivalents."""
    return (str(s)
            .replace('\u2014', '-')   # em dash
            .replace('\u2013', '-')   # en dash
            .replace('\u2713', 'Y')   # checkmark
            .replace('\u2717', 'N')   # ballot X
            .replace('\u2022', '*')   # bullet
            .encode('latin-1', errors='replace').decode('latin-1'))


def _fig_to_png_bytes(fig, width=700, height=350):
    """Render a Plotly figure to PNG bytes using kaleido."""
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        fig.write_image(tmp.name, width=width, height=height, scale=2)
        tmp.seek(0)
    with open(tmp.name, 'rb') as f:
        data = f.read()
    os.unlink(tmp.name)
    return data


def _pdf_section_heading(pdf, title):
    """Render a section heading with a subtle underline."""
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, _pdf_safe(title), new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(200, 200, 200)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(3)


def _pdf_table(pdf, headers, rows, col_widths=None):
    """Render a table. col_widths can be a list of mm widths matching headers."""
    if col_widths is None:
        col_widths = [pdf.epw / len(headers)] * len(headers)

    # Header row
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(240, 240, 240)
    for i, h in enumerate(headers):
        pdf.cell(col_widths[i], 6, _pdf_safe(h), border=1, align="C", fill=True)
    pdf.ln()

    # Data rows
    pdf.set_font("Helvetica", "", 8)
    for row in rows:
        for i, val in enumerate(row):
            pdf.cell(col_widths[i], 5.5, _pdf_safe(val), border=1, align="C")
        pdf.ln()


def _generate_daily_pdf():
    """Build a PDF mirroring the Daily Report page content."""
    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # --- Title ---
    pdf.set_font("Helvetica", "B", 20)
    pdf.cell(0, 10, "SwipeOnDeck Daily Report", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "", 12)
    pdf.cell(0, 7, report_date.strftime('%A, %B %d, %Y'), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(6)

    # --- KPI tiles (2 rows x 4) ---
    kpi_items_row1 = [
        ("New Signups", _fmt_int(kpi.get('new_signups', 0))),
        ("DAU", _fmt_int(kpi.get('dau', 0))),
        ("Total Swipes", _fmt_int(kpi.get('total_swipes', 0))),
        ("Like Rate", _fmt_pct(kpi.get('like_rate'))),
    ]
    kpi_items_row2 = [
        ("Saves", _fmt_int(kpi.get('saves', 0))),
        ("Prompts", _fmt_int(kpi.get('prompts', 0))),
        ("Onboarding", _fmt_int(kpi.get('onboarding_completed', 0))),
        ("Total Events", _fmt_int(kpi.get('total_events', 0))),
    ]
    tile_w = pdf.epw / 4
    for row_items in [kpi_items_row1, kpi_items_row2]:
        # Values
        pdf.set_font("Helvetica", "B", 18)
        for label, val in row_items:
            pdf.cell(tile_w, 10, _pdf_safe(val), align="C")
        pdf.ln()
        # Labels
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(120, 120, 120)
        for label, val in row_items:
            pdf.cell(tile_w, 5, _pdf_safe(label), align="C")
        pdf.ln(8)
        pdf.set_text_color(0, 0, 0)
    pdf.ln(4)

    # --- 7-Day Trend charts (all 6 metrics) ---
    if not trend_df.empty:
        _pdf_section_heading(pdf, "7-Day Trend")
        trend_display = trend_df.copy()
        trend_display['day'] = pd.to_datetime(trend_display['day'])

        trend_metrics = [
            ("DAU", "dau", False),
            ("New Signups", "new_signups", False),
            ("Total Events", "total_events", False),
            ("Saves", "saves", False),
            ("Prompts", "prompts", False),
            ("Like Rate", "like_rate", True),
        ]
        for label, col, is_pct in trend_metrics:
            y_vals = trend_display[col].astype(float)
            if is_pct:
                y_vals = y_vals * 100
                text_vals = [f"{v:.1f}%" if pd.notna(v) else "-" for v in y_vals]
            else:
                text_vals = [str(int(v)) if pd.notna(v) else "-" for v in y_vals]
            pdf_trend_fig = go.Figure(go.Bar(
                x=trend_display['day'], y=y_vals,
                text=text_vals, textposition='outside',
                marker_color=[
                    '#E91E8C' if d.date() == report_date else '#2383E2'
                    for d in trend_display['day']
                ],
            ))
            pdf_trend_fig.update_layout(
                yaxis_title=f"{label} (%)" if is_pct else label,
                font=dict(family="Helvetica", size=12),
                plot_bgcolor='white', paper_bgcolor='white',
                margin=dict(l=40, r=20, t=10, b=40),
                xaxis=dict(showgrid=False, tickformat='%a %b %d'),
                yaxis=dict(showgrid=True, gridcolor='#E5E5E5'),
                showlegend=False,
            )
            png = _fig_to_png_bytes(pdf_trend_fig, width=700, height=220)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                tmp.write(png)
                tmp_path = tmp.name
            pdf.image(tmp_path, w=pdf.epw)
            os.unlink(tmp_path)
            pdf.ln(2)

    # --- Activation Checklist Funnel ---
    if new_signups_n > 0:
        _pdf_section_heading(pdf, "Activation Checklist Funnel — New Signups")
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 5,
                 f"{all_three_n} / {new_signups_n} new users completed all 3 steps and earned the spin wheel",
                 new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        funnel_headers = ["Step", "Count", "% of new users"]
        funnel_rows = [
            ["1. Deck created", str(deck_n), f"{_pct(deck_n) * 100:.0f}%"],
            ["2. 3+ places saved", str(saves_n), f"{_pct(saves_n) * 100:.0f}%"],
            ["3. Multiplayer started", str(mp_n), f"{_pct(mp_n) * 100:.0f}%"],
            ["All 3 complete", str(all_three_n), f"{_pct(all_three_n) * 100:.0f}%"],
        ]
        _pdf_table(pdf, funnel_headers, funnel_rows, col_widths=[pdf.epw * 0.5, pdf.epw * 0.25, pdf.epw * 0.25])
        pdf.ln(2)
        pdf.set_font("Helvetica", "I", 8)
        pdf.cell(0, 4,
                 f"Drop-off: {stuck_no_deck} never created a deck | "
                 f"{stuck_at_saves} stuck at saves | {stuck_at_mp} stuck at multiplayer",
                 new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)

    # --- New Signups Onboarding Status ---
    if not signups_df.empty:
        _pdf_section_heading(pdf, "New Signups — Onboarding & Checklist Status")
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 5,
                 f"{onboarded_n} of {total_n} completed onboarding ({pct:.0f}%)",
                 new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        signup_headers = ["Name", "Onboarded", "Deck", "Saves", "MP", "All 3"]
        signup_rows = []
        for _, r in signups_df.iterrows():
            ck = lambda v: "Y" if bool(v) else "-"
            name = str(r['display_name'])[:25]
            signup_rows.append([
                name, ck(r['onboarded']), ck(r['deck_created']),
                ck(r['places_saved']), ck(r['multiplayer_started']), ck(r['all_three']),
            ])
        cw = [pdf.epw * 0.35] + [pdf.epw * 0.13] * 5
        _pdf_table(pdf, signup_headers, signup_rows, col_widths=cw)
        pdf.ln(4)

    # --- Category Popularity ---
    if not cat_df.empty:
        _pdf_section_heading(pdf, "Category Popularity")
        cat_headers = ["Category", "Likes", "Dislikes", "Total", "Like %"]
        cat_rows = []
        for _, r in cat_df.iterrows():
            like_pct_val = float(r['like_pct']) * 100 if pd.notna(r['like_pct']) else 0
            cat_rows.append([
                str(r['category']), str(int(r['likes'])), str(int(r['dislikes'])),
                str(int(r['total'])), f"{like_pct_val:.0f}%",
            ])
        _pdf_table(pdf, cat_headers, cat_rows)
        pdf.ln(4)

    # --- Places Flagged for Removal ---
    if not flagged_df.empty:
        _pdf_section_heading(pdf, "Places Flagged for Removal")
        flag_headers = ["ID", "Name", "Area", "Likes", "Dislikes", "Total", "Dislike %"]
        flag_rows = []
        for _, r in flagged_df.iterrows():
            dp = float(r['dislike_pct']) * 100 if pd.notna(r['dislike_pct']) else 0
            flag_rows.append([
                str(r['id']), str(r['name'])[:28], str(r['area'])[:20],
                str(int(r['likes'])), str(int(r['dislikes'])), str(int(r['total'])),
                f"{dp:.0f}%",
            ])
        cw = [pdf.epw * 0.08, pdf.epw * 0.25, pdf.epw * 0.2,
              pdf.epw * 0.1, pdf.epw * 0.12, pdf.epw * 0.1, pdf.epw * 0.15]
        _pdf_table(pdf, flag_headers, flag_rows, col_widths=cw)
        pdf.ln(4)

    # --- Top Liked Places ---
    if not top_df.empty:
        _pdf_section_heading(pdf, "Top Liked Places")
        top_headers = ["ID", "Name", "Area", "Likes", "Dislikes"]
        top_rows = []
        for _, r in top_df.iterrows():
            top_rows.append([
                str(r['id']), str(r['name'])[:30], str(r['area'])[:22],
                str(int(r['likes'])), str(int(r['dislikes'])),
            ])
        cw = [pdf.epw * 0.1, pdf.epw * 0.3, pdf.epw * 0.3, pdf.epw * 0.15, pdf.epw * 0.15]
        _pdf_table(pdf, top_headers, top_rows, col_widths=cw)
        pdf.ln(4)

    # --- Weekly Engagement Intensity chart ---
    if intensity_fig is not None:
        pdf.add_page()
        _pdf_section_heading(pdf, "Weekly Engagement Intensity — Per Active Activated User")
        png = _fig_to_png_bytes(intensity_fig, width=700, height=350)
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp.write(png)
            tmp_path = tmp.name
        pdf.image(tmp_path, w=pdf.epw)
        os.unlink(tmp_path)
        pdf.ln(4)

    # --- User Activity table ---
    if not user_df.empty:
        _pdf_section_heading(pdf, f"User Activity — {report_date.strftime('%b %d')}")
        ua_headers = ["Name", "Likes", "Dis.", "Like %", "Saves", "Boards", "Prompts", "Total", "New?"]
        ua_rows = []
        for _, r in user_df.iterrows():
            lr = f"{float(r['like_rate']) * 100:.0f}%" if pd.notna(r.get('like_rate')) else "-"
            new_flag = "Y" if bool(r.get('is_new')) else ""
            name = str(r.get('display_name', ''))[:22]
            ua_rows.append([
                name, str(int(r['likes'])), str(int(r['dislikes'])),
                lr, str(int(r['saves'])), str(int(r.get('boards_created', 0))),
                str(int(r['prompts'])), str(int(r['total_events'])), new_flag,
            ])
        cw_unit = pdf.epw / 9
        cw = [cw_unit * 2] + [cw_unit * 7 / 8] * 8
        _pdf_table(pdf, ua_headers, ua_rows, col_widths=cw)

    return bytes(pdf.output())


# Generate PDF and fill the slot near the top
try:
    pdf_bytes = _generate_daily_pdf()
    pdf_slot.download_button(
        label="Download as PDF",
        data=pdf_bytes,
        file_name=f"SwipeOnDeck_Daily_Report_{report_date_str}.pdf",
        mime="application/pdf",
    )
except Exception as e:
    pdf_slot.warning(f"PDF generation failed: {str(e)}")

add_deck_footer()
