# EQT Framework — Distilled

Source: `EQT Metrics & Consumer Doc.pdf` at the repo root. This file is the
skill's source of truth for the *shape* of the insights — the PDF has no
benchmark numbers, only lenses and principles.

## Core principles (apply to every bullet)

1. **Time-series, not data points.** A single number is almost always
   meaningless. Every claim should be a trajectory.
2. **Cohorts, not averages.** Aggregate retention/engagement masks the signal;
   cohort-based views reveal it.
3. **Historicals + trajectories.** Rate of change matters as much as level.
4. **Hypothesis-driven (scientific method).** Formulate → implement telemetry →
   verify qualitatively → cohort-compare. Applies to feature UX and metrics
   interpretation alike.
5. **Front with data, don't just "support with" data.** The story *is* the
   numbers.
6. **Dig for what's working, not only what's missing.** Benchmarking is lazy;
   causal levers are the prize.
7. **Measure from the top of the intake funnel.** Survivorship bias is the
   enemy — churned users hold the truth.

## Dos / Don'ts scorecard

Use this to score each memo. Every "Don't" the dashboard commits is a candidate
gap.

| ✅ Do | ❌ Don't |
|-------|----------|
| Standard retention definitions (D1/7/30/60/90) | Only home-brew metrics |
| Retention on the core loop | Retention on app-open or similar |
| Engagement retention (canary for revenue) | Revenue retention only |
| Historicals, trajectories, time-series | Vanity metrics, cumulatives |
| Fully-loaded CAC (incl. salaries) | CAC or ARPU/LTV in isolation |
| Cohorts | Averages |

## Seven lenses

### 1. Need / PMF
- How much better is the solution vs alternatives?
- Momentary need or perpetual?
- Is usage rigged (spam-y path) or authentic?
- **Signal on DECK:** like rate on swipes, category spread, prompts per active
  user, qualitative demand expressed in prompts.

### 2. Activation
- **Definition matters** — will we measure retention on it?
- **Time-to-activation** — successful consumer products deliver quick aha
  moments.
- **Funnel to activation** — you have seconds of clicks; it's under your
  control. Where are the biggest improvement hooks?
- **Has conversion changed over time?**
- **Signal on DECK:** the 3-step first-session checklist (deck → 3+ saves →
  multiplayer) is the activation funnel. Each step conversion and drop-off is
  the raw material.

### 3. Retention (the holy grail)
- **Flatline / floor** — a consistent share of every cohort that stays
  retained. That's the engine of long-term value.
- **D1/D7/D30/D60/D90** — core-loop action, not app-open.
- **Cohort cuts by time AND user attribute at the time of acquisition** — not
  attributes of *retained* users (that's survivor bias).
- **Engagement retention is a canary for payer retention.**
- **Signal on DECK:** `fct_retention_by_cohort_week`, `fct_cohort_quality`,
  `fct_active_planners`, `fct_north_star_weekly`.

### 4. Engagement
- **Depth** — how many hours / how many features adopted per user. Cohort-split
  to see if it's improving over time.
- **Frequency** — days active per week. EQT example: "what % of users opened
  the app ≥4 days of a calendar week a year ago vs now?"
- **Spread matters**, not just the mean — the shape of the distribution over
  "days active in week" buckets is a powerful chart.
- **Signal on DECK:** `load_daily_weekly_intensity` (depth — avg
  swipes/saves/shares per active activated user). Frequency spread is **not**
  currently surfaced on the report pages; flag as gap.

### 5. Network effects
- **Referral funnels** — if the product benefits from more users, organic share
  rises.
- **Dense atomic units improve faster** — retention of a local/friend cluster
  should move with cluster size.
- **Signal on DECK:** `fct_viral_loop` (share → signup attribution). Not on the
  report pages.

### 6. Growth
- Channel mix, CAC trend per channel, % non-paid.
- Geographic / cohort decomposition of growth.
- **Signal on DECK:** Not surfaced at all on the report pages. Always a gap
  in this skill's output unless/until added.

### 7. Payback
- LTV built from actual churn × ARPU × cohort size, against fully-loaded CAC.
- Payback period by cohort.
- **Signal on DECK:** N/A pre-monetisation. Note and move on.

## Scientific method (from the PDF appendix)

Use this shape for every hypothesis in Phase 5:

1. **Formulate a hypothesis** and articulate how it manifests in numbers.
   Example: "Users who save 3+ places in their first session will retain at
   D7 at 2× the rate of those who don't."
2. **Implement telemetry** — tracking of the specific action and the context
   needed to distinguish it from confounders.
3. **Verify qualitatively** — observe/interview a small sample to check
   whether the quantitative signal reflects the behavior it claims to.
4. **Keep tracking cohorts** — make sure the pattern generalises beyond early
   adopters.
