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
- **Gold** (`models/gold/`): Analytics-ready fact and visualization tables. Prefixes: `fct_` (fact tables), `vis_` (dashboard visualization tables). Several legacy models are disabled in `dbt_project.yml`.

### Data Eras

The app went through major schema changes. Events are tagged with `data_era`:
- `card_system`: Before Nov 20, 2025 (experience_cards + dextr_pack_cards)
- `places_system`: Nov 20, 2025 – Jan 29, 2026 (places + dextr_places + core_card_actions)
- `telemetry`: Jan 30, 2026+ (TelemetryManager: app_events, planning_sessions, share_links)

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
analyses/           # Ad-hoc SQL analyses and data dictionaries
macros/             # (empty — no custom macros)
snapshots/          # (empty — no snapshots configured)
dashboard/          # Streamlit dashboard app
  pages/            # Multi-page Streamlit pages
  utils/            # DB connection, data loading, styling, visualizations
```

## Naming Conventions

- **Bronze models**: `src_<source_table_name>` (e.g., `src_app_events`, `src_users`)
- **Silver staging**: `stg_<descriptive_name>` (e.g., `stg_unified_events`, `stg_users`)
- **Silver intermediate**: `int_<descriptive_name>` (e.g., `int_place_resolver`, `int_session_app_versions`)
- **Gold facts**: `fct_<metric_domain>` (e.g., `fct_session_outcomes`, `fct_north_star_daily`)
- **Gold visualizations**: `vis_<dashboard_widget>` (e.g., `vis_headline_metrics`, `vis_daily_active_users`)
- **Schema files**: `_schema.yml` for model documentation, `sources.yml` for source definitions

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

## Disabling Models

Models are disabled in `dbt_project.yml` under the `models:` config block using `+enabled: false`. Comments explain the reason (broken dependencies, replaced by newer models, etc.). Do not delete disabled model files — they serve as reference.

## Important Notes

- The pipeline connects to a Supabase PostgreSQL database. Connection details are in dbt `profiles.yml` (not committed).
- Dashboard secrets are in `dashboard/.streamlit/secrets.toml` (gitignored). Use `secrets.toml.template` as reference.
- The `packages.txt` lists system dependencies (`libpq-dev`) for the Postgres adapter.
- Gold layer output tables use schema `analytics_prod_gold` in the database.
- Several Python utility scripts exist at the repo root (`check_tables.py`, `test_tables.py`, `show_summary.py`, etc.) for ad-hoc database inspection.
