# CLAUDE.md

## Project Overview

This is the **deck_analytics** dbt project — the data pipeline for DECK, a social planning app where users discover places via AI (Dextr), save them to decks/boards, and share plans with friends. The pipeline transforms raw Supabase/Postgres tables into analytics-ready models following a bronze/silver/gold medallion architecture.

**Database**: PostgreSQL (Supabase)
**Runtime**: Python 3.11, dbt-core with dbt-postgres adapter
**Dashboard**: Streamlit app in `dashboard/` directory

## Architecture

### Medallion Layers

- **Bronze** (`models/bronze/`): Raw source mirrors. Prefix: `src_`. Organized by schema (`auth/`, `public/`, `legacy/`). 1:1 with source tables, minimal transformation (column selection only).
- **Silver** (`models/silver/`): Cleaned and enriched staging/intermediate models. Staging prefix: `stg_`, intermediate prefix: `int_`. Key models:
  - `stg_unified_events` — canonical event stream combining all data eras
  - `stg_unified_sessions` — unified sessions (inferred + native)
  - `int_place_resolver` — maps any card_id format to places.place_id
  - `stg_users` — user master table with test user flag
- **Gold** (`models/gold/`): Analytics-ready fact and visualization tables. Prefixes: `fct_` (fact tables), `vis_` (dashboard visualization tables), plus `gold_recommendation_training_set` (ML feature store). Several legacy models are disabled in `dbt_project.yml`. Key model groups:
  - **North Star**: `fct_north_star_daily`, `fct_north_star_weekly` — PSR metrics with WoW tracking
  - **Sessions**: `fct_session_outcomes`, `fct_session_explorer` — session-level PSR ladder and diagnostic JSONB details
  - **Users**: `fct_user_segments`, `fct_user_engagement_trajectory`, `fct_user_activation`, `fct_user_retention`
  - **Funnels**: `fct_onboarding_funnel`, `fct_signup_to_activation_funnel`, `fct_activation_funnel`
  - **Content**: `fct_place_performance` (post-gemini places only), `fct_pack_performance`, `fct_prompt_analysis`
  - **Growth**: `fct_viral_loop`, `fct_conversion_signals`, `fct_cohort_quality`, `fct_active_planners`, `fct_retention_by_cohort_week`, `fct_retention_activated`
  - **ML**: `gold_recommendation_training_set` — 28 engineered features for LightGBM/LambdaRank place recommendation models
  - **Visualization**: 12 `vis_*` tables powering Streamlit dashboard widgets (includes `vis_onboarding_daily_summary`)

### Data Eras

The app went through major schema changes. Events are tagged with `data_era`:
- `card_system`: Before Nov 20, 2025 (experience_cards + dextr_pack_cards)
- `places_system`: Nov 20, 2025 – Jan 29, 2026 (places + dextr_places + core_card_actions)
- `telemetry`: Jan 30, 2026+ (TelemetryManager: app_events, planning_sessions, share_links)

**Exception — `query` events:** The iOS client writes a prompt to both
`dextr_queries` (legacy direct insert) AND emits `app_events.dextr_query_submitted`
(telemetry) on every submission. Telemetry rollout for this event was staggered
over Jan 30 – Feb 4, so the handoff date for 'query' specifically is **2026-02-05**
(first day both sources reconcile). `stg_unified_events` gates the legacy CTE at
that date — anything earlier comes from `dextr_queries`, anything later from
telemetry only. Other event types (save, share, swipe, click) cut over cleanly
on Jan 30 and don't need this exception.

### Key Domain Concepts

- **Planner vs Passenger**: Core user segmentation. Planners have saved AND shared; Passengers are activated but haven't done both.
- **Activation**: User's first save, share, or AI-prompted save.
- **PSR (Plan Survival Rate)**: North star metric. Broad = session with save AND share. Strict = also has post-share interaction.
- **Session**: Either "native" (from `planning_sessions` table) or "inferred" (5-minute timeout gap detection from events).
- **Test users**: Identified via `seeds/test_accounts.csv` and filtered out in gold layer models via `stg_users.is_test_user`.

## Commands

```bash
# Run all models
dbt run

# Run a specific model and its upstream dependencies
dbt run --select +model_name

# Run all models in a layer
dbt run --select bronze
dbt run --select silver
dbt run --select gold

# Run tests
dbt test

# Run a specific test
dbt test --select test_name

# Load seed data
dbt seed

# Full build (seed + run + test)
dbt build

# Generate and serve docs
dbt docs generate
dbt docs serve

# Compile SQL without running (useful for debugging)
dbt compile --select model_name

# Dashboard (from repo root)
cd dashboard && streamlit run Home.py
```

## Project Structure

```
models/
  bronze/           # Source mirrors (src_*)
    auth/           # auth.users
    public/         # Main app tables
    legacy/         # Deprecated tables
    sources.yml     # Source definitions with status metadata
  silver/           # Staging (stg_*) and intermediate (int_*) models
    _schema.yml     # Column-level docs for silver models
  gold/             # Fact (fct_*) and visualization (vis_*) tables
    _schema.yml     # Column-level docs for gold models
seeds/              # Static reference data (test_accounts.csv, app_version_releases.csv, suppliers.csv)
tests/              # Custom data tests (singular tests)
analyses/           # Ad-hoc SQL analyses, data dictionaries, and Python inspection scripts
macros/             # (empty — no custom macros)
snapshots/          # (empty — no snapshots configured)
dashboard/          # Streamlit dashboard app
  Home.py           # Landing page
  pages/            # Multi-page Streamlit pages (11-page architecture)
    1_📅_Daily.py               # CEO daily report — single-date picker, top-line KPIs, activation checklist, category/place breakdowns, per-user activity
    2_📅_Weekly.py              # Weekly report — Mon–Sun aggregation, WAU, 8-week trend, full PDF with all 6 trend charts
    3_📅_Monthly.py             # Monthly report — calendar month aggregation, MAU, 6-month trend, full PDF with all 6 trend charts
    4_🎯_North_Star.py          # PSR ladder funnel, metrics over time, surface attribution, active planners
    5_📈_Engagement.py          # Session trends, engagement metrics
    6_👥_Users_&_Cohorts.py     # Archetypes, leaderboard, activation/signup funnel, retention, churn
    7_🤖_AI_&_Prompts.py        # Prompt analysis, AI performance, Dextr query → results funnel
    8_🃏_Content_&_Places.py    # Place/card performance (post-gemini only)
    9_🔄_Conversion_&_Viral.py  # Conversion signals, viral loop
    10_🚀_Onboarding.py         # Onboarding funnel + first-session experience (checklist → spin wheel)
    11_🔍_Power_User_Deep_Dive.py # Power user diagnostics
    13_🧹_Place_Curation.py       # Admin: curate/delete low-quality places (write-capable)
    14_🎁_Spin_Wheel_Winners.py   # Ops kanban: winner outreach + gift-card fulfillment (write-capable)
  utils/            # DB connection, data loading, styling, filters
  sql/              # Hand-applied migrations for dashboard-owned state (e.g. analytics_ops)
```

### Dashboard-owned schemas

Most dashboard pages are read-only consumers of `analytics_prod_gold`. Two pages
write back to the database, to schemas that sit **outside** the dbt pipeline so
`dbt run` cannot recreate or truncate them:

- **`public.*`** — page 13 (Place Curation) deletes from `public.places` and
  related tables. Source-of-truth tables owned by Dracon2; used cautiously.
- **`analytics_ops.*`** — page 14 (Spin Wheel Winners) reads+writes
  `analytics_ops.spin_wheel_winner_outreach` to track gift-card fulfillment
  state. This schema was introduced specifically to keep dashboard-writable
  operational state separate from both `public` (iOS-owned) and
  `analytics_prod_gold` (dbt-generated). DDL lives in `dashboard/sql/` and is
  applied manually via the Supabase SQL editor or MCP `apply_migration`.

## Naming Conventions

- **Bronze models**: `src_<source_table_name>` (e.g., `src_app_events`, `src_users`)
- **Silver staging**: `stg_<descriptive_name>` (e.g., `stg_unified_events`, `stg_users`)
- **Silver intermediate**: `int_<descriptive_name>` (e.g., `int_place_resolver`, `int_session_app_versions`)
- **Gold facts**: `fct_<metric_domain>` (e.g., `fct_session_outcomes`, `fct_north_star_daily`)
- **Gold visualizations**: `vis_<dashboard_widget>` (e.g., `vis_headline_metrics`, `vis_daily_active_users`)
- **Gold ML models**: `gold_<purpose>` (e.g., `gold_recommendation_training_set`)
- **Schema files**: `_schema.yml` for model documentation, `sources.yml` for source definitions. `gold_recommendation_training_set.yml` has its own dedicated schema file.

## SQL Style

- Lowercase keywords (`select`, `from`, `where`, `join`)
- CTEs preferred over subqueries, using `with ... as (` pattern
- Jinja `{{ ref('model_name') }}` for all inter-model references
- Jinja `{{ source('schema', 'table') }}` for bronze source references
- Explicit column casting (e.g., `::text`, `::timestamptz`)
- PostgreSQL-specific syntax: `filter (where ...)`, `generate_series()`, `percentile_cont()`
- All models materialized as tables (configured in `dbt_project.yml`)
- Test users excluded in gold layer via `where is_test_user = 0` or join to `stg_users`

## Testing

- **Schema tests**: Defined in `_schema.yml` files (`unique`, `not_null`, `accepted_values`)
- **Singular tests** (in `tests/`): Custom SQL assertions that return rows on failure
  - `test_no_test_users_in_gold` — ensures test accounts don't leak into gold tables
  - `test_activation_date_before_activity` — validates activation date ordering
  - `test_cohort_key_is_activation_week` — ensures cohort keys are Mondays
  - `test_place_resolver_no_duplicate_card_ids` — uniqueness check on place resolver
  - `test_planner_definition_consistency` — validates planner/passenger segmentation logic
- **ML data quality tests** (in `gold_recommendation_training_set.yml`): Schema-level tests including `test_no_future_leakage_training_set` to prevent temporal data leakage in ML features

## Disabling Models

Models are disabled in `dbt_project.yml` under the `models:` config block using `+enabled: false`. Comments explain the reason (broken dependencies, replaced by newer models, etc.). Do not delete disabled model files — they serve as reference.

## Dashboard–Model Column Mapping

When referencing gold model columns in dashboard code, use the actual column names from the SQL models — not assumed names. Key columns that have caused bugs:

- `fct_active_planners`: uses `active_planners` (not `planner_count`)
- `fct_north_star_daily`: rolling averages are `ssr_7d_avg`, `shr_7d_avg`, `psr_broad_7d_avg`, `nvr_7d_avg`
- `fct_user_segments`: has both `username` and `email`; dashboard uses `COALESCE(username, email) as display_name`
- `fct_place_performance`: filtered to `card_type = 'post-gemini'` only; aggregate KPIs (`avg_save_rate`, `avg_right_swipe_rate`) can be NULL — always guard with `pd.notna()` checks

## Important Notes

- The pipeline connects to a Supabase PostgreSQL database. Connection details are in dbt `profiles.yml` (not committed).
- Dashboard secrets are in `dashboard/.streamlit/secrets.toml` (gitignored). Use `secrets.toml.template` as reference.
- The `packages.txt` lists system dependencies (`libpq-dev`) for the Postgres adapter.
- Gold layer output tables use schema `analytics_prod_gold` in the database.
- Python utility scripts for ad-hoc database inspection are in `analyses/` (`check_tables.py`, `test_tables.py`, `show_summary.py`, etc.).
- `analyses/` also contains gold layer reference docs: `gold_layer_data_dictionary.md`, `gold_layer_example_queries.sql`, `gold_layer_cleanup.sql`.
- `gold_recommendation_training_set` is an ML feature store with 28 engineered features across 4 groups (candidate, prompt context, session context, interaction) for place recommendation ranking. It has its own dedicated `.yml` schema file.
