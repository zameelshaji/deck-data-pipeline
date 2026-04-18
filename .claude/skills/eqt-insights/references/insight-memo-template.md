# Insight Memo Template

The output of the skill. Keep the whole thing ≤400 words. Fill in the bracketed
fields with values pulled in Phases 2–3 of `SKILL.md`.

---

# EQT Lens Memo — {page} · {period label}

**Headline:** {one sentence. e.g. "Activation funnel held at 34%, but
engagement-retention trajectory is flat — EQT would call the cohort data
missing."}

**Active users in period:** {DAU/WAU/MAU}
**New signups in period:** {n}

## Scorecard

| Lens | Rating | One-line justification |
|------|--------|------------------------|
| Need / PMF | Strong / Partial / Missing | like rate {x}%, trending {up/flat/down} over {N} {days/weeks} |
| Activation | Strong / Partial / Missing | {X}/{Y} cohort completed 3-step checklist ({pct}%); {biggest drop-off step} is the bottleneck |
| Retention (cohort) | Strong / Partial / Missing | {pull from fct_retention_by_cohort_week, or "not surfaced on this page"} |
| Engagement depth | Strong / Partial / Missing | avg {swipes/saves/shares} per active activated user = {x} → {y} over {N} weeks |
| Engagement frequency | Strong / Partial / Missing | {"days-active-in-week spread not surfaced" if Missing} |
| Network effects | Strong / Partial / Missing | {shares count, and share→signup rate if available} |
| Growth | Strong / Partial / Missing / N/A | {channel/CAC data not surfaced} |
| Payback | N/A — pre-monetisation | — |

## What we're doing well

Cite a trajectory or cohort in every bullet. No single-point numbers.

1. {e.g. "Weekly intensity climbed 7.2 → 11.4 avg swipes/active-activated user
   over the last 6 weeks — depth-of-engagement trending the right way."}
2. {...}
3. {...}

## Gaps vs EQT framework

Each bullet names the specific EQT principle being violated.

1. **{Principle, e.g. "Retention on core loops, not averages"}** — this page
   shows {what it shows}; EQT expects {what EQT wants}. Closest existing gold
   model that could close the gap: `{fct_*}`.
2. {...}
3. {...}

## Hypotheses to test next

Use the PDF's scientific-method shape. Two to three items.

1. **Hypothesis:** {e.g. "Users who trigger multiplayer within their first
   session retain at D7 at ≥2× the rate of those who don't."}
   **How it'd manifest:** {specific measurable signal, e.g. "D7 retention gap
   of ≥20 pp between the multiplayer-in-session-1 cohort and the no-multiplayer
   cohort, holding signup week constant."}
   **What to pull:** {e.g. "`fct_retention_by_cohort_week` joined with the
   first-session checklist status from `stg_app_events_enriched`."}
2. {...}
3. {...}

---

*EQT Memo generated {YYYY-MM-DD}. Framework source:
`EQT Metrics & Consumer Doc.pdf`. Lenses distilled in
`.claude/skills/eqt-insights/references/eqt-framework.md`.*
