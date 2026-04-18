# Page ↔ EQT Lens Map

Which dashboard query / gold model supplies evidence for each EQT lens, on
each of the three report pages. Line numbers are into
`deck-data-pipeline/dashboard/utils/data_loader.py` (valid at skill creation;
re-grep if they drift).

## Daily page (`pages/1_📅_Daily.py`)

| EQT Lens | Evidence source | data_loader fn | Notes |
|----------|----------------|----------------|-------|
| Need / PMF | Like rate (topline) + category popularity | `load_daily_topline_kpis` (L2440), `load_daily_category_popularity` (L2674) | One-day point — **too thin for EQT on its own**. Pull 30d from `fct_north_star_daily` for trajectory. |
| Activation | 3-step checklist funnel + drop-off | `load_daily_activation_checklist` (L2562) | Single-day cohort of signups. Small samples on a daily cadence — weekly cohort is often more robust. |
| Retention | **Not on this page.** | — | Pull `fct_retention_by_cohort_week` directly. Always a gap on Daily. |
| Engagement depth | Weekly intensity chart (12 weeks) | `load_daily_weekly_intensity` (L2834) | Good EQT fit (trajectory + per-user). |
| Engagement frequency | **Not on this page.** | — | Compute days-active-in-week spread from `stg_unified_sessions` if needed. |
| Network effects | Shares in topline (count only) | `load_daily_topline_kpis` | No share→signup attribution. Pull `fct_viral_loop`. |
| Growth | **Not on this page.** | — | Always Missing from Daily. |

## Weekly page (`pages/2_📅_Weekly.py`)

| EQT Lens | Evidence source | data_loader fn | Notes |
|----------|----------------|----------------|-------|
| Need / PMF | Like rate (topline) + 8-week trend + category popularity | `load_weekly_topline_kpis` (L2955), `load_weekly_multiweek_trend` (L3013), `load_weekly_category_popularity` (L3174) | 8-week trend is the strongest PMF-trajectory signal on this page. |
| Activation | 3-step checklist funnel on weekly cohort | `load_weekly_activation_checklist` (L3076) | Cohort = users who signed up this week. **Good EQT fit**. |
| Retention | **Not on this page.** | — | Pull `fct_retention_by_cohort_week` (D7/D30 by cohort), `fct_active_planners` (weekly). |
| Engagement depth | Weekly intensity chart (12 weeks) | `load_daily_weekly_intensity` (L2834) | Good EQT fit. |
| Engagement frequency | **Not on this page.** | — | Can compute from unified sessions. |
| Network effects | Shares count only | `load_weekly_topline_kpis` | Pull `fct_viral_loop` for attribution. |
| Growth | **Not on this page.** | — | Always Missing from Weekly. |

## Monthly page (`pages/3_📅_Monthly.py`)

| EQT Lens | Evidence source | data_loader fn | Notes |
|----------|----------------|----------------|-------|
| Need / PMF | Like rate + 6-month trend + category popularity | `load_monthly_topline_kpis` (L3374), `load_monthly_multimonth_trend` (L3432), `load_monthly_category_popularity` (L3593) | 6-month trend is EQT's preferred horizon. |
| Activation | 3-step checklist on monthly cohort | `load_monthly_activation_checklist` (L3495) | Largest sample; best funnel-stability read. |
| Retention | **Not on this page — biggest gap.** | — | Pull `fct_retention_by_cohort_week` for the ~4 weekly cohorts in the month + 12 weeks of prior cohorts for trajectory. Ideal EQT chart = monthly retention for acquisition cohorts (PDF page 5). |
| Engagement depth | Weekly intensity (12 weeks) | `load_daily_weekly_intensity` | Good EQT fit. |
| Engagement frequency | **Not on this page.** | — | Compute monthly spread. |
| Network effects | Shares count only | `load_monthly_topline_kpis` | Pull `fct_viral_loop`. |
| Growth | **Not on this page.** | — | Always Missing. Monthly is the natural place to add channel/CAC if it were tracked. |

## Gold tables to pull for the "missing" lenses (all three pages)

These gold models exist but are **not wired into the report pages**. Use them
in Phase 3 of the skill to close the retention / network / cohort gaps:

| Gold model | Purpose | Period grain |
|------------|---------|--------------|
| `fct_retention_by_cohort_week` | D1/D7/D30 retention for each weekly cohort | Weekly cohort × period_number |
| `fct_cohort_quality` | Weekly-cohort activation, planner rate, PSR share | Weekly cohort |
| `fct_active_planners` | Planner-count trajectory | Weekly |
| `fct_north_star_weekly` / `fct_north_star_daily` | PSR broad/strict + 7d rolling averages | Weekly / daily |
| `fct_viral_loop` | Share → signup attribution | Per share event |
| `fct_user_engagement_trajectory` | Individual user's engagement change over time | Per user |
| `fct_signup_to_activation_funnel` | Time-to-activation buckets | Aggregate |

## Period resolution helpers

- Daily period: single `YYYY-MM-DD`.
- Weekly period: Monday of the target week, e.g. `2026-04-13` covers
  `Mon Apr 13 – Sun Apr 19`.
- Monthly period: `YYYY-MM`, e.g. `2026-03` covers the full calendar month.
- For the cohort pulls in Phase 3, always go back at least **12 weeks** before
  the period start to give the chart enough trajectory to read.
