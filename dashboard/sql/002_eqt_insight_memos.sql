-- 002_eqt_insight_memos.sql
--
-- Creates analytics_ops.eqt_insight_memos — append-only store for EQT-framework
-- insight memos generated on a schedule by the Claude eqt-insights skill.
--
-- The Streamlit Daily/Weekly/Monthly report pages read the latest memo for the
-- selected period and render it inline. Scheduled Claude triggers (daily at
-- 02:00 UTC, weekly at 02:00 UTC Monday, monthly at 02:00 UTC on the 1st) run
-- after the 01:00 UTC dbt rebuild completes, analyze the period, and INSERT a
-- row here.
--
-- Applied to Supabase project lzapzucmzvztogacckee on 2026-04-18 via MCP
-- apply_migration (migration name: create_eqt_insight_memos).
--
-- Idempotent — CREATEs use IF NOT EXISTS. Re-running is a no-op.

create schema if not exists analytics_ops;

create table if not exists analytics_ops.eqt_insight_memos (
  id             bigserial primary key,
  page           text not null check (page in ('daily', 'weekly', 'monthly')),
  -- Canonical period key per page:
  --   daily:   'YYYY-MM-DD'        (the report date)
  --   weekly:  'YYYY-MM-DD'        (Monday of the report week)
  --   monthly: 'YYYY-MM'           (calendar month)
  period_key     text not null,
  period_start   date not null,
  period_end     date not null,
  memo_markdown  text not null,
  -- Structured scorecard: one key per EQT lens, each mapping to
  --   { "rating": "Strong"|"Partial"|"Missing"|"N/A", "justification": "..." }
  -- Lenses: need_pmf, activation, retention, engagement_depth,
  --         engagement_frequency, network_effects, growth, payback.
  scorecard_json jsonb,
  model_version  text,
  generated_at   timestamptz not null default now(),
  -- Append-only. If the same period is regenerated we keep both rows; the
  -- dashboard always reads `order by generated_at desc limit 1`.
  unique (page, period_key, generated_at)
);

create index if not exists eqt_insight_memos_page_period_idx
  on analytics_ops.eqt_insight_memos (page, period_key, generated_at desc);

create index if not exists eqt_insight_memos_generated_at_idx
  on analytics_ops.eqt_insight_memos (generated_at desc);
