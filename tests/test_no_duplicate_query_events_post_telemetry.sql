-- Regression guard for the 'query' event double-count bug.
--
-- Before 2026-04-14, stg_unified_events pulled queries from BOTH
-- src_dextr_queries (legacy direct-insert) AND app_events.dextr_query_submitted
-- (telemetry) for the telemetry era, doubling the daily prompt count (e.g.
-- 2026-04-12 showed 152 vs the true ~83 after test-user filtering).
--
-- The fix bounds the legacy CTE at 2026-02-05 (the first day telemetry and
-- legacy totals reconciled). This test fails if that gate is ever removed or
-- a new legacy source is added without era-gating.
--
-- Fails if any (user_id, minute-truncated event_timestamp) has query rows from
-- BOTH dextr_queries (legacy) AND app_events (telemetry) on or after 2026-02-05
-- — the exact signature of the original double-count bug. Rapid legitimate
-- repeat prompts from the same user within a minute are allowed, as long as
-- they all come from a single source_table.
select
    user_id,
    date_trunc('minute', event_timestamp) as minute_bucket,
    array_agg(distinct source_table order by source_table) as source_tables
from {{ ref('stg_unified_events') }}
where event_type = 'query'
  and event_timestamp >= '2026-02-05'::timestamptz
group by 1, 2
having array_agg(distinct source_table order by source_table)
       @> array['app_events', 'dextr_queries']
