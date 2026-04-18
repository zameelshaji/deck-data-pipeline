---
name: eqt-insights
description: |
  Produce an EQT-framework insight memo for the Daily, Weekly, or Monthly report page
  in the deck-data-pipeline dashboard. Scores the chosen period against the EQT Metrics
  & Consumer Doc lenses (Need, Activation, Retention, Engagement, Network, Growth,
  Payback) and its principles (time-series > points, cohorts > averages, core-loop
  retention, hypothesis-driven). Outputs: Scorecard → Wins → Gaps → Hypotheses.
  Use when the user says "run EQT insights", "EQT memo for last week", "what would
  EQT say about this month", "insights on the daily/weekly/monthly report", or similar.
user-invocable: true
disable-model-invocation: false
argument-hint: "daily <YYYY-MM-DD> | weekly <monday YYYY-MM-DD> | monthly <YYYY-MM>"
---

# EQT Insights

Generate an EQT-framework insight memo for one of the three time-based dashboard pages.

**Reference source of truth:** the EQT Metrics & Consumer Doc, distilled in
`${CLAUDE_SKILL_DIR}/references/eqt-framework.md`. That doc is a **framework**, not a
benchmark doc — there are no target numbers. The skill's job is to tell the user
where their data **fits EQT's lenses and principles**, and where it **doesn't**.

**Metric ↔ lens map:** `${CLAUDE_SKILL_DIR}/references/page-metric-map.md` — which
dashboard queries / gold models supply evidence for each EQT lens, on each page.

**Output template:** `${CLAUDE_SKILL_DIR}/references/insight-memo-template.md`.

---

## Phase 1: Parse arguments

Expected `$ARGUMENTS`:

- `daily YYYY-MM-DD` — single-date Daily page
- `weekly YYYY-MM-DD` — Mon of the target week (Weekly page)
- `monthly YYYY-MM` — calendar month (Monthly page)
- No args, or vague ("last week", "yesterday") — resolve to the page's default:
  - Daily → yesterday
  - Weekly → last completed Mon–Sun
  - Monthly → last completed calendar month

Ask the user to confirm the resolved period if it was inferred, not explicit.

---

## Phase 2: Pull the period's headline metrics

The three pages share the same sections. Reuse the queries in
`dashboard/utils/data_loader.py` — **do not re-implement**:

| Page | Topline | Trend | Activation funnel | Engagement intensity |
|------|---------|-------|-------------------|----------------------|
| Daily | `load_daily_topline_kpis(date)` | `load_daily_7day_trend(date)` | `load_daily_activation_checklist(date)` | `load_daily_weekly_intensity(date, weeks=12)` |
| Weekly | `load_weekly_topline_kpis(monday)` | `load_weekly_multiweek_trend(monday, 8)` | `load_weekly_activation_checklist(monday)` | `load_daily_weekly_intensity(monday, weeks=12)` |
| Monthly | `load_monthly_topline_kpis(y, m)` | `load_monthly_multimonth_trend(y, m, 6)` | `load_monthly_activation_checklist(y, m)` | `load_daily_weekly_intensity(month_end, weeks=12)` |

Run each via the Supabase MCP tool `mcp__claude_ai_Supabase__execute_sql`
(project ref `lzapzucmzvztogacckee`) — copy the SQL out of `data_loader.py` at the
line ranges listed in `page-metric-map.md` and parameterise with the period.

---

## Phase 3: Pull the cohort / retention context the pages DON'T show

This is the most important step — it closes the biggest EQT gap the dashboard
pages leave open.

The three report pages show **period totals** and a **short trailing trend**.
That alone violates two EQT principles ("time-series, not points" and
"cohorts, not averages"). But the gold layer already has the cohort & retention
data — the pages just don't surface it. Pull it directly:

```sql
-- Cohort quality: how each weekly signup cohort performs
select * from analytics_prod_gold.fct_cohort_quality
where signup_week >= (date '{period_start}' - interval '12 weeks')
order by signup_week;

-- Retention by cohort week (D1/D7/D30 retention on the core loop)
select * from analytics_prod_gold.fct_retention_by_cohort_week
where cohort_week >= (date '{period_start}' - interval '12 weeks')
order by cohort_week, period_number;

-- Active planners trajectory (engagement retention proxy)
select * from analytics_prod_gold.fct_active_planners
where week_start >= (date '{period_start}' - interval '12 weeks')
order by week_start;

-- North star PSR trajectory
select * from analytics_prod_gold.fct_north_star_weekly
where week_start >= (date '{period_start}' - interval '12 weeks')
order by week_start;
```

If any of those tables error or return empty, note the gap in the memo but continue.

---

## Phase 4: Score the 7 lenses

For each lens below, rate the evidence **Strong / Partial / Missing** and write
one sentence of justification anchored in a specific number or trend you just
pulled.

1. **Need / PMF** — like rate trajectory, category demand spread, prompt volume
   per active user. "Are consumers saying yes at a rate that suggests the product
   is replacing *something*?"
2. **Activation** — the 3-step checklist funnel: deck → 3+ saves → multiplayer.
   Time-to-activation and funnel conversion. EQT asks: "does the definition make
   sense, and are we improving the funnel over time?"
3. **Retention (core loop, cohort)** — D1/D7/D30 on save/share, by cohort week.
   **Critical:** EQT explicitly flags retention on app-open and retention on
   averaged users as anti-patterns. If all the page shows is DAU/WAU/MAU, mark
   this Missing.
4. **Engagement** — depth (avg swipes/saves/shares per active activated user,
   from the intensity chart) and frequency (days active per week spread — not
   currently on these pages; flag).
5. **Network effects** — share volume → resulting signups. If the page shows
   shares but not share→signup conversion, mark Partial and reference
   `fct_viral_loop` in gold for the next iteration.
6. **Growth** — non-paid share, channel mix, CAC trends. These pages carry
   **none** of this; mark Missing and say so.
7. **Payback** — LTV/CAC and time-to-payback. Not applicable at current stage;
   mark N/A with a note on why (no monetisation yet / too early).

Present as a compact table.

---

## Phase 5: Write the memo

**Audience: the CEO.** Plain English, scannable, short. Every bullet leads
with the finding, with a "so what" — the number is the evidence, not the
point. Translate metric acronyms on first use (e.g. "save rate (SSR)", "Plan
Survival Rate (PSR) — a session ending with save AND share").

Follow `references/insight-memo-template.md` exactly. Sections:

1. **TL;DR** — one short paragraph, 2–3 sentences. Lead with the single most
   important takeaway. End with the biggest open question.
2. **What's going well (3 bullets)** — finding in plain English, with "so
   what". Cite a trajectory or cohort, not a single number.
3. **What to watch (3 bullets)** — concern + why it matters + which EQT
   principle relates (draw from `eqt-framework.md` Dos/Don'ts table).
4. **What we should test next (2–3)** — a question, then a 1-sentence
   hypothesis + 1-sentence "how we'd check it".
5. **Scorecard (at the bottom)** — the 8-lens table. This is detail, not
   headline — the CEO should already have the picture from sections 1–4.

Keep the whole memo ≤400 words. EQT rewards conciseness.

Also produce a machine-readable **scorecard_json** alongside the prose — shape:

```json
{
  "need_pmf":             {"rating": "Strong|Partial|Missing|N/A", "justification": "..."},
  "activation":           {"rating": "...", "justification": "..."},
  "retention":            {"rating": "...", "justification": "..."},
  "engagement_depth":     {"rating": "...", "justification": "..."},
  "engagement_frequency": {"rating": "...", "justification": "..."},
  "network_effects":      {"rating": "...", "justification": "..."},
  "growth":               {"rating": "...", "justification": "..."},
  "payback":              {"rating": "N/A", "justification": "pre-monetisation"}
}
```

Each `justification` ≤140 chars. This lets the dashboard later trend "how
many lenses rated Strong over time" without re-parsing prose.

---

## Phase 6: Persist to Supabase

INSERT the memo into `analytics_ops.eqt_insight_memos` via
`mcp__claude_ai_Supabase__execute_sql` on project
`lzapzucmzvztogacckee`.

Period boundaries to compute:

| Page | `period_key` | `period_start` | `period_end` |
|------|--------------|----------------|--------------|
| daily | `YYYY-MM-DD` | same as period_key | same as period_key |
| weekly | `YYYY-MM-DD` (Monday) | same as period_key | Monday + 6 days (Sunday) |
| monthly | `YYYY-MM` | first-of-month | last-of-month |

SQL shape (parameterise — never interpolate raw strings; escape single quotes
in markdown):

```sql
insert into analytics_ops.eqt_insight_memos
  (page, period_key, period_start, period_end, memo_markdown, scorecard_json, model_version)
values
  ('{page}', '{period_key}', '{period_start}'::date, '{period_end}'::date,
   $memo$ {memo_markdown} $memo$,
   '{scorecard_json}'::jsonb,
   '{model_version}')
;
```

Use a dollar-quoted literal (`$memo$ ... $memo$`) for `memo_markdown` to
handle embedded quotes and backticks safely. For `scorecard_json`, serialise
to a compact JSON string and escape any `'` to `''` before interpolation.

`model_version` — use the actual model id you're running as (e.g.
`'claude-opus-4-7'`). If unknown, insert `'claude-unknown'`.

The table is **append-only** — do not UPSERT or DELETE prior rows. The
dashboard always reads the latest by `generated_at`.

Confirm success by reading back the inserted row's `id` and `generated_at`.

---

## Phase 7: Offer next actions

After showing the memo and confirming the insert, ask whether the user wants
to:

- Save the memo to disk for sharing (e.g. as markdown).
- Re-run for a different period or page.
- Drill into one of the hypotheses by pulling the suggested data.
- Include the memo in the Streamlit page's downloadable PDF (requires a code
  change to the page's `_generate_*_pdf()` function — offer to draft it).

Do not do any of those automatically — always confirm first.

Note: when this skill is invoked by a **scheduled trigger** rather than a
human, skip Phase 7 — the insert from Phase 6 is the only side-effect needed.

---

## Reference files

- `${CLAUDE_SKILL_DIR}/references/eqt-framework.md` — distilled EQT lenses, Dos/Don'ts,
  and principles.
- `${CLAUDE_SKILL_DIR}/references/page-metric-map.md` — which dashboard query and which
  gold model feeds each EQT lens, per page, with `data_loader.py` line numbers.
- `${CLAUDE_SKILL_DIR}/references/insight-memo-template.md` — the exact output shape.
