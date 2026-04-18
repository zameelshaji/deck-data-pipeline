"""DECK Monthly Report — aggregates all Daily Report sections over a calendar month.

Sidebar lets you pick from the last 12 months. The 6-month trend, activation
funnel, category popularity, and per-user activity all re-roll for the
selected month.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import tempfile
import os
import calendar
from datetime import date
from fpdf import FPDF
from utils.styling import apply_deck_branding, add_deck_footer, BRAND_COLORS
from utils.data_loader import (
    load_monthly_topline_kpis,
    load_monthly_multimonth_trend,
    load_monthly_activation_checklist,
    load_monthly_new_signups_status,
    load_monthly_category_popularity,
    load_monthly_places_flagged,
    load_monthly_top_liked_places,
    load_daily_weekly_intensity,
    load_monthly_user_activity,
    load_latest_eqt_memo,
)

st.set_page_config(
    page_title="Monthly Report | DECK Analytics",
    page_icon="📅",
    layout="wide"
)

apply_deck_branding()

st.title("Monthly Report")

# ----------------------------------------------------------------------------
# Month picker — default to last completed calendar month
# ----------------------------------------------------------------------------
today = date.today()
# Last completed month
if today.month == 1:
    default_year, default_month = today.year - 1, 12
else:
    default_year, default_month = today.year, today.month - 1

month_options = []
for i in range(12):
    m = default_month - i
    y = default_year
    while m < 1:
        m += 12
        y -= 1
    month_options.append((y, m, f"{calendar.month_name[m]} {y}"))

with st.sidebar:
    st.header("Filters")
    selected_idx = st.selectbox(
        "Report Month",
        options=range(len(month_options)),
        format_func=lambda i: month_options[i][2],
        help="All metrics below aggregate over this calendar month.",
    )
    sel_year, sel_month, sel_label = month_options[selected_idx]
    min_swipe_threshold = st.number_input(
        "Places-to-remove min swipes",
        min_value=1, max_value=100, value=20, step=1,
        help="A place is flagged only if it had at least this many swipes during the month."
    )

st.markdown(f"### {sel_label}")

# For the engagement intensity section, pass the last day of the month
last_day = calendar.monthrange(sel_year, sel_month)[1]
month_end_str = f"{sel_year}-{sel_month:02d}-{last_day:02d}"

pdf_slot = st.empty()

# ============================================================================
# Section A: Top-line KPI tiles
# ============================================================================
kpi = load_monthly_topline_kpis(sel_year, sel_month)

if not kpi:
    st.warning("No data for the selected month. Try a different month.")
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

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("New Signups", _fmt_int(kpi.get('new_signups', 0)))
with c2:
    st.metric("MAU", _fmt_int(kpi.get('mau', 0)))
with c3:
    st.metric("Total Swipes", _fmt_int(kpi.get('total_swipes', 0)))
with c4:
    st.metric("Like Rate", _fmt_pct(kpi.get('like_rate')),
              help=f"{_fmt_int(kpi.get('total_right_swipes', 0))} right of {_fmt_int(kpi.get('total_swipes', 0))} total swipes")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Saves", _fmt_int(kpi.get('saves', 0)))
with c2:
    st.metric("Prompts", _fmt_int(kpi.get('prompts', 0)))
with c3:
    st.metric("Onboarding", _fmt_int(kpi.get('onboarding_completed', 0)),
              help="Users who completed onboarding this month (signed up AND finished onboarding)")
with c4:
    st.metric("Total Events", _fmt_int(kpi.get('total_events', 0)))

st.divider()

# ============================================================================
# Section A2: EQT Insight Memo
# ============================================================================
st.subheader("EQT Insight Memo")
month_period_key = f"{sel_year}-{sel_month:02d}"
memo = load_latest_eqt_memo('monthly', month_period_key)
if memo:
    ts = pd.to_datetime(memo['generated_at'])
    st.caption(
        f"Generated {ts.strftime('%b %d %Y %H:%M UTC')} · "
        f"{memo.get('model_version') or 'unknown model'}"
    )
    st.markdown(memo['memo_markdown'])
else:
    st.info(
        "No EQT memo for this month yet. Scheduled run: 1st of each month at "
        "02:00 UTC (after the 01:00 UTC dbt rebuild) on the prior calendar month."
    )

st.divider()

# ============================================================================
# Section B: 6-Month Trend
# ============================================================================
st.subheader("6-Month Trend")
st.caption(f"Rolling 6 months ending {sel_label}")

trend_df = load_monthly_multimonth_trend(sel_year, sel_month, num_months=6)

if not trend_df.empty:
    metric_choice = st.radio(
        "Metric",
        options=["MAU", "New Signups", "Total Events", "Saves", "Prompts", "Like Rate"],
        horizontal=True,
        label_visibility="collapsed",
    )
    metric_col = {
        "MAU": "mau",
        "New Signups": "new_signups",
        "Total Events": "total_events",
        "Saves": "saves",
        "Prompts": "prompts",
        "Like Rate": "like_rate",
    }[metric_choice]

    display_df = trend_df.copy()
    display_df['month_start'] = pd.to_datetime(display_df['month_start'])
    y_vals = display_df[metric_col].astype(float)
    if metric_col == 'like_rate':
        y_vals = y_vals * 100
        y_label = "Like Rate (%)"
        text_vals = [f"{v:.1f}%" if pd.notna(v) else "—" for v in y_vals]
    else:
        y_label = metric_choice
        text_vals = [f"{int(v):,}" if pd.notna(v) else "—" for v in y_vals]

    target_month_start = date(sel_year, sel_month, 1)
    fig = go.Figure(go.Bar(
        x=display_df['month_start'],
        y=y_vals,
        text=text_vals,
        textposition='outside',
        marker_color=['#E91E8C' if d.date() == target_month_start else BRAND_COLORS['info']
                      for d in display_df['month_start']],
    ))
    fig.update_layout(
        yaxis_title=y_label,
        font=dict(family="Inter, system-ui, sans-serif", size=13),
        plot_bgcolor='white', paper_bgcolor='white',
        margin=dict(l=40, r=20, t=20, b=40),
        xaxis=dict(showgrid=False, tickformat='%b %Y'),
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

checklist = load_monthly_activation_checklist(sel_year, sel_month)
new_signups_n = int(checklist.get('new_signups', 0) or 0)
deck_n = int(checklist.get('deck_created', 0) or 0)
saves_n = int(checklist.get('places_saved', 0) or 0)
mp_n = int(checklist.get('multiplayer_started', 0) or 0)
all_three_n = int(checklist.get('all_three', 0) or 0)
stuck_no_deck = int(checklist.get('stuck_no_deck', 0) or 0)
stuck_at_saves = int(checklist.get('stuck_at_saves', 0) or 0)
stuck_at_mp = int(checklist.get('stuck_at_mp', 0) or 0)

if new_signups_n == 0:
    st.info("No new signups this month — nothing to funnel.")
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

signups_df = load_monthly_new_signups_status(sel_year, sel_month)

if signups_df.empty:
    st.info("No new signups this month.")
else:
    onboarded_n = int(signups_df['onboarded'].sum())
    total_n = len(signups_df)
    pct = (onboarded_n / total_n * 100) if total_n else 0
    st.caption(f"{onboarded_n} of {total_n} completed onboarding ({pct:.0f}%)")

    display = signups_df.copy()
    def _check(v):
        return "Y" if bool(v) else "-"
    for col in ['onboarded', 'deck_created', 'places_saved', 'multiplayer_started', 'all_three']:
        display[col] = display[col].apply(_check)

    display = display.rename(columns={
        'display_name': 'Name',
        'signup_date': 'Signed Up',
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

cat_df = load_monthly_category_popularity(sel_year, sel_month)

if cat_df.empty:
    st.info("No category swipes recorded this month.")
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
    f"Places with more dislikes than likes this month, minimum {int(min_swipe_threshold)} swipes. "
    "Review before removing from the database."
)

flagged_df = load_monthly_places_flagged(sel_year, sel_month, min_swipes=int(min_swipe_threshold))

if flagged_df.empty:
    st.info(f"No places met the flagging criteria for {sel_label}.")
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

top_df = load_monthly_top_liked_places(sel_year, sel_month, limit=10)

if top_df.empty:
    st.info("No likes recorded this month.")
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

intensity_df = load_daily_weekly_intensity(month_end_str, weeks=12)

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

    with st.expander("Weekly values table"):
        table = idf.copy()
        table.columns = ['Week Start', 'Active Activated Users', 'Avg Swipes', 'Avg Saves', 'Avg Shares']
        st.dataframe(table, use_container_width=True, hide_index=True)
else:
    st.info("No weekly intensity data available.")

st.divider()

# ============================================================================
# Section I: User Activity — Month
# ============================================================================
st.subheader(f"User Activity — {sel_label}")

user_df = load_monthly_user_activity(sel_year, sel_month)

if user_df.empty:
    st.info("No user activity this month.")
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

    csv = user_df.to_csv(index=False)
    st.download_button(
        label="Download user activity CSV",
        data=csv,
        file_name=f"deck_monthly_user_activity_{sel_year}-{sel_month:02d}.csv",
        mime="text/csv",
    )


# ============================================================================
# PDF Generation
# ============================================================================

def _pdf_safe(s):
    """Replace Unicode characters unsupported by Helvetica with ASCII equivalents."""
    return (str(s)
            .replace('\u2014', '-')
            .replace('\u2013', '-')
            .replace('\u2713', 'Y')
            .replace('\u2717', 'N')
            .replace('\u2022', '*')
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
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(240, 240, 240)
    for i, h in enumerate(headers):
        pdf.cell(col_widths[i], 6, _pdf_safe(h), border=1, align="C", fill=True)
    pdf.ln()
    pdf.set_font("Helvetica", "", 8)
    for row in rows:
        for i, val in enumerate(row):
            pdf.cell(col_widths[i], 5.5, _pdf_safe(val), border=1, align="C")
        pdf.ln()


def _generate_monthly_pdf():
    """Build a PDF mirroring the Monthly Report page content."""
    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # --- Title ---
    pdf.set_font("Helvetica", "B", 20)
    pdf.cell(0, 10, "SwipeOnDeck Monthly Report", new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "", 12)
    pdf.cell(0, 7, _pdf_safe(sel_label), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(6)

    # --- KPI tiles (2 rows x 4) ---
    kpi_items_row1 = [
        ("New Signups", _fmt_int(kpi.get('new_signups', 0))),
        ("MAU", _fmt_int(kpi.get('mau', 0))),
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
        pdf.set_font("Helvetica", "B", 18)
        for label, val in row_items:
            pdf.cell(tile_w, 10, _pdf_safe(val), align="C")
        pdf.ln()
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(120, 120, 120)
        for label, val in row_items:
            pdf.cell(tile_w, 5, _pdf_safe(label), align="C")
        pdf.ln(8)
        pdf.set_text_color(0, 0, 0)
    pdf.ln(4)

    # --- 6-Month Trend charts (all 6 metrics) ---
    if not trend_df.empty:
        _pdf_section_heading(pdf, "6-Month Trend")
        trend_display = trend_df.copy()
        trend_display['month_start'] = pd.to_datetime(trend_display['month_start'])

        target_month_start = date(sel_year, sel_month, 1)
        trend_metrics = [
            ("MAU", "mau", False),
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
            pdf_fig = go.Figure(go.Bar(
                x=trend_display['month_start'], y=y_vals,
                text=text_vals, textposition='outside',
                marker_color=[
                    '#E91E8C' if d.date() == target_month_start else '#2383E2'
                    for d in trend_display['month_start']
                ],
            ))
            pdf_fig.update_layout(
                yaxis_title=f"{label} (%)" if is_pct else label,
                font=dict(family="Helvetica", size=12),
                plot_bgcolor='white', paper_bgcolor='white',
                margin=dict(l=40, r=20, t=10, b=40),
                xaxis=dict(showgrid=False, tickformat='%b %Y'),
                yaxis=dict(showgrid=True, gridcolor='#E5E5E5'),
                showlegend=False,
            )
            png = _fig_to_png_bytes(pdf_fig, width=700, height=220)
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
        signup_headers = ["Name", "Signed Up", "Onboarded", "Deck", "Saves", "MP", "All 3"]
        signup_rows = []
        for _, r in signups_df.iterrows():
            ck = lambda v: "Y" if bool(v) else "-"
            name = str(r['display_name'])[:22]
            signup_rows.append([
                name, str(r.get('signup_date', '')), ck(r['onboarded']), ck(r['deck_created']),
                ck(r['places_saved']), ck(r['multiplayer_started']), ck(r['all_three']),
            ])
        cw = [pdf.epw * 0.25, pdf.epw * 0.15] + [pdf.epw * 0.12] * 5
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
        _pdf_section_heading(pdf, f"User Activity — {sel_label}")
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
    pdf_bytes = _generate_monthly_pdf()
    pdf_slot.download_button(
        label="Download as PDF",
        data=pdf_bytes,
        file_name=f"SwipeOnDeck_Monthly_Report_{sel_year}-{sel_month:02d}.pdf",
        mime="application/pdf",
    )
except Exception as e:
    pdf_slot.warning(f"PDF generation failed: {str(e)}")

add_deck_footer()
