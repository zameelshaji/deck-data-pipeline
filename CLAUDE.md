# CLAUDE.md

## Project Overview

This is the **deck_analytics** dbt project — the data pipeline for DECK, a social planning app where users discover places via AI (Dextr), save them to decks/boards, and share plans with friends. The pipeline transforms raw Supabase/Postgres tables into analytics-ready models following a bronze/silver/gold medallion architecture.

**Database**: PostgreSQL (Supabase)
**Runtime**: Python 3.11, dbt-core with dbt-postgres adapter
**Dashboard**: Streamlit 1.32.0 app in `dashboard/` directory
**Devcontainer**: Python 3.11 (Bookworm), auto-starts Streamlit on port 8501

## Architecture

### Medallion Layers

- **Bronze** (`models/bronze/`): Raw source mirrors. Prefix: `src_`. Organized by schema (`auth/`, `public/`, `legacy/`). 1:1 with source tables, minimal transformation (column selection only). Output schema: `bronze`.
- **Silver** (`models/silver/`): Cleaned and enriched staging/intermediate models. Staging prefix: `stg_`, intermediate prefix: `int_`. Output schema: `silver`. Key models:
  - `stg_unified_events` — canonical event stream combining all data eras
  - `stg_unified_sessions` — unified sessions (inferred + native)
  - `int_place_resolver` — maps any card_id format to places.place_id
  - `int_session_app_versions` — maps sessions to app versions from seed data
  - `stg_users` — user master table with test user flag
  - `stg_app_events_enriched` — enriched app_events from telemetry era
  - `stg_session_saves` / `stg_session_shares` — session-level save/share aggregations
  - `stg_referral_relationships` — referral chain tracking
  - `stg_planning_sessions` — cleaned native planning sessions
- **Gold** (`models/gold/`): Analytics-ready fact and visualization tables. Output schema: `gold` (database: `analytics_prod_gold`). Prefixes:
  - `fct_` — fact tables for core metrics and analysis
  - `vis_` — dashboard visualization tables (pre-aggregated for Streamlit)
  - Unprefixed legacy models (mostly disabled)

### Data Eras

The app went through major schema changes. Events are tagged with `data_era`:
- `card_system`: Before Nov 20, 2025 (experience_cards + dextr_pack_cards)
- `places_system`: Nov 20, 2025 – Jan 29, 2026 (places + dextr_places + core_card_actions)
- `telemetry`: Jan 30, 2026+ (TelemetryManager: app_events, planning_sessions, share_links, share_interactions)

### Key Domain Concepts

- **Planner vs Passenger**: Core user segmentation. Planners have saved AND shared; Passengers are activated but haven't done both.
- **Activation**: User's first save, share, or AI-prompted save.
- **PSR (Plan Survival Rate)**: North star metric. Broad = session with save AND share. Strict = also has post-share interaction.
- **Session**: Either "native" (from `planning_sessions` table) or "inferred" (5-minute timeout gap detection from events).
- **Test users**: Identified via `seeds/test_accounts.csv` and filtered out in gold layer models via `stg_users.is_test_user`.
- **User archetypes**: one_and_done, browser, saver, planner, power_planner (defined in `fct_user_segments`).
- **Churn risk**: high (14+ days inactive, <3 sessions), medium (7-14 days), low (active within 7 days).

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
.devcontainer/
  devcontainer.json       # Python 3.11 Bookworm container, auto-starts Streamlit on port 8501
models/
  bronze/                 # Source mirrors (src_*) — 32 models
    auth/                 # auth.users (1 model)
    public/               # Main app tables (28 models)
    legacy/               # Deprecated tables (3 models)
    sources.yml           # Source definitions with status metadata and column tests
  silver/                 # Staging (stg_*) and intermediate (int_*) — 14 enabled, 13 disabled
    _schema.yml           # Column-level docs for key silver models
  gold/                   # Fact (fct_*) and visualization (vis_*) — ~36 enabled, 11 disabled
    _schema.yml           # Column-level docs for gold models (comprehensive, 880+ lines)
    gold_recommendation_training_set.yml  # ML training set schema (separate file)
seeds/                    # Static reference data
  _seeds.yml              # Seed documentation
  test_accounts.csv       # Test user IDs for filtering
  app_version_releases.csv  # App version release dates for version imputation
  suppliers.csv           # Supplier reference data
tests/                    # Custom singular data tests (5 tests)
analyses/                 # Ad-hoc SQL analyses
  gold_layer_data_dictionary.md   # Business term definitions
  gold_layer_cleanup.sql          # Migration/cleanup queries
  gold_layer_example_queries.sql  # Example analytics queries
macros/                   # (empty — no custom macros, uses dbt built-ins)
snapshots/                # (empty — no snapshots configured)
dashboard/                # Streamlit dashboard app
  Home.py                 # Executive Overview homepage
  requirements.txt        # Dashboard Python dependencies
  .streamlit/
    config.toml           # Streamlit theme and server config
    secrets.toml.template # Template for DB credentials (secrets.toml is gitignored)
  pages/                  # Multi-page Streamlit pages
    2_🎯_North_Star_Metrics.py
    3_🔄_Activation_Retention.py
    5_📊_Monthly_Summary.py
    6_🚀_Onboarding.py
    7_🔍_Power_User_Deep_Dive.py
  utils/                  # Shared dashboard utilities
    db_connection.py      # PostgreSQL connection via Supabase secrets
    data_loader.py        # SQL queries and data loading functions
    styling.py            # DECK branding and CSS
    visualizations.py     # Plotly chart components
```

### Top-Level Files

| File | Purpose |
|------|---------|
| `dbt_project.yml` | dbt project config, materialization, disabled models |
| `packages.txt` | System dependencies (`libpq-dev`) for Postgres adapter |
| `runtime.txt` | Python version (`python-3.11`) |
| `.gitignore` | Ignores `target/`, `dbt_packages/`, `logs/`, secrets |
| `USER_SEGMENTATION_README.md` | Planner vs Passenger segmentation documentation |
| `check_tables.py` | Checks which analytics gold tables exist in DB |
| `test_tables.py` | Table existence check using dashboard DB utilities |
| `show_summary.py` | Displays user segmentation summary |
| `check_referrals.py` | Referral metrics analysis |
| `top_planners.py` | Shows top 25 Planners by engagement |
| `verify_dextr_performance.py` | Validates Dextr query performance model |
| `verify_segmentation.py` | Validates user segmentation model |

## Model Inventory

### Bronze Layer (32 models)

All source mirrors, 1:1 with Supabase tables.

**auth/** (1): `src_users`

**public/** (28): `src_app_events`, `src_board_places`, `src_board_places_v2`, `src_boards`, `src_core_card_actions`, `src_dextr_pack_cards`, `src_dextr_packs`, `src_dextr_places`, `src_dextr_queries`, `src_experience_cards`, `src_featured_cards`, `src_featured_cards_category_mapping`, `src_featured_section_actions`, `src_giveaway_claims`, `src_multiplayer_sessions`, `src_places`, `src_planning_sessions`, `src_referral_codes`, `src_referral_usages`, `src_session_participants`, `src_session_places`, `src_session_votes`, `src_share_interactions`, `src_share_links`, `src_test_accounts`, `src_user_preferences`, `src_user_profiles`, `src_user_sessions`

**legacy/** (3): `src_learned_places_queries`, `src_user_liked_places`, `src_user_swipes_v2`

### Silver Layer (14 enabled, 13 disabled)

**Enabled staging models**: `stg_app_events_enriched`, `stg_cards`, `stg_dextr_interactions`, `stg_multiplayer`, `stg_onboarding_events`, `stg_planning_sessions`, `stg_referral_relationships`, `stg_session_saves`, `stg_session_shares`, `stg_share_interactions_clean`, `stg_unified_events`, `stg_unified_sessions`, `stg_users`

**Enabled intermediate models**: `int_place_resolver`, `int_session_app_versions`

**Disabled** (in `dbt_project.yml`): `stg_dextr_category`, `stg_dextr_category_cumalative`, `stg_dextr_category_metrics`, `stg_category_interestcount`, `stg_events` (replaced by `stg_unified_events`), `stg_events_intent`, `stg_events_count`, `stg_inferred_sessions` (replaced by `stg_unified_sessions`), `int_inferred_session_events`, `int_inferred_sessions`, `int_inferred_session_saves`, `int_inferred_session_shares`

### Gold Layer (~36 enabled, 11 disabled)

**Fact tables** (20): `fct_activation_funnel`, `fct_active_planners`, `fct_cohort_quality`, `fct_conversion_signals`, `fct_north_star_daily`, `fct_north_star_weekly`, `fct_onboarding_funnel`, `fct_pack_performance`, `fct_place_performance`, `fct_prompt_analysis`, `fct_retention_activated`, `fct_retention_by_cohort_week`, `fct_session_explorer`, `fct_session_outcomes`, `fct_signup_to_activation_funnel`, `fct_user_activation`, `fct_user_engagement_trajectory`, `fct_user_retention`, `fct_user_segments`, `fct_viral_loop`

**Visualization tables** (12): `vis_content_performance`, `vis_daily_active_users`, `vis_dextr_performance`, `vis_dextr_query_user_performance`, `vis_executive_summary`, `vis_headline_metrics`, `vis_homepage_totals`, `vis_monthly_active_users`, `vis_onboarding_daily_summary`, `vis_referral_analytics`, `vis_user_acquisition_funnel`, `vis_weekly_active_users`

**Other enabled**: `daily_referral_leaderboard`, `referral_metrics_daily`, `gold_recommendation_training_set` (ML training set with dedicated `.yml` schema)

**Disabled** (11 total):
- Broken dependencies on `stg_events_intent`: `supplier_performance`, `category_combination_intent`, `category_combination_metrics`, `category_price_combinations`, `category_combination_ratings`, `price_band_performance`, `category_intent_count`, `interest_to_conversion`
- Obsolete (replaced by `fct_`/`vis_` models): `user_session_derived`, `user_segmentation`, `user_segmentation_summary`, `user_cohort_retention_monthly`

## Naming Conventions

- **Bronze models**: `src_<source_table_name>` (e.g., `src_app_events`, `src_users`)
- **Silver staging**: `stg_<descriptive_name>` (e.g., `stg_unified_events`, `stg_users`)
- **Silver intermediate**: `int_<descriptive_name>` (e.g., `int_place_resolver`, `int_session_app_versions`)
- **Gold facts**: `fct_<metric_domain>` (e.g., `fct_session_outcomes`, `fct_north_star_daily`)
- **Gold visualizations**: `vis_<dashboard_widget>` (e.g., `vis_headline_metrics`, `vis_daily_active_users`)
- **Schema files**: `_schema.yml` for model documentation, `sources.yml` for source definitions
- **Seed docs**: `_seeds.yml` for seed documentation

## SQL Style

- Lowercase keywords (`select`, `from`, `where`, `join`)
- CTEs preferred over subqueries, using `with ... as (` pattern
- Jinja `{{ ref('model_name') }}` for all inter-model references
- Jinja `{{ source('schema', 'table') }}` for bronze source references
- Explicit column casting (e.g., `::text`, `::timestamptz`)
- PostgreSQL-specific syntax: `filter (where ...)`, `generate_series()`, `percentile_cont()`
- All models materialized as tables (configured in `dbt_project.yml`)
- Test users excluded in gold layer via `where is_test_user = 0` or join to `stg_users`
- Comment style: `-- CTE N: Description` above CTEs for documentation

## Testing

- **Schema tests**: Defined in `_schema.yml` files and `sources.yml` (`unique`, `not_null`, `accepted_values`)
- **Singular tests** (in `tests/`): Custom SQL assertions that return rows on failure
  - `test_no_test_users_in_gold` — ensures test accounts don't leak into gold tables
  - `test_activation_date_before_activity` — validates activation date ordering
  - `test_cohort_key_is_activation_week` — ensures cohort keys are Mondays
  - `test_place_resolver_no_duplicate_card_ids` — uniqueness check on place resolver
  - `test_planner_definition_consistency` — validates planner/passenger segmentation logic

## Source Table Status

Sources defined in `models/bronze/sources.yml` include status metadata:
- `current`: Actively written to by the app
- `historical`: Deprecated, no longer receiving new data (includes `deprecated_date`)

Key deprecated tables:
- `experience_cards` (deprecated 2026-01-10, replaced by `places`)
- `dextr_pack_cards` (deprecated 2025-11-19, replaced by `dextr_places`)
- `core_card_actions` (deprecated 2026-01-29, replaced by `app_events`)
- `featured_section_actions` (deprecated 2026-01-29)
- `board_places` (deprecated 2026-01-02, replaced by `board_places_v2`)

## Disabling Models

Models are disabled in `dbt_project.yml` under the `models:` config block using `+enabled: false`. Comments explain the reason (broken dependencies, replaced by newer models, etc.). Do not delete disabled model files — they serve as reference.

## Dashboard

The Streamlit dashboard reads from gold layer tables. Pages:

| Page | Focus |
|------|-------|
| Home | Executive overview with headline metrics |
| North Star Metrics | PSR, SSR, SHR tracking with daily/weekly trends |
| Activation & Retention | Cohort retention, activation funnels, D7/D30/D60 |
| Monthly Summary | Monthly aggregate reporting |
| Onboarding | Signup-to-activation funnel analysis |
| Power User Deep Dive | User-level exploration and segmentation |

Dashboard dependencies are in `dashboard/requirements.txt` (Streamlit, Plotly, Pandas, psycopg2, SQLAlchemy). DB connection credentials go in `dashboard/.streamlit/secrets.toml` (gitignored) — see `secrets.toml.template`.

## Important Notes

- The pipeline connects to a Supabase PostgreSQL database. Connection details are in dbt `profiles.yml` (not committed).
- The `packages.txt` lists system dependencies (`libpq-dev`) for the Postgres adapter.
- Gold layer output tables use schema `analytics_prod_gold` in the database.
- Several Python utility scripts at the repo root (`check_tables.py`, `verify_segmentation.py`, etc.) connect directly to the database for ad-hoc inspection.
- The `gold_recommendation_training_set` model has its own dedicated schema file (`gold_recommendation_training_set.yml`) with ~28 engineered features for ML recommendation training.
- The devcontainer (`.devcontainer/devcontainer.json`) is configured for GitHub Codespaces with Python 3.11 and auto-starts the Streamlit dashboard.
- Project timezone is UTC (set via `vars.timezone` in `dbt_project.yml`).
