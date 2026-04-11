-- Phase A regression guard: telemetry-era swipes must come through the app_events path.
--
-- The iOS app (TelemetryManager.swift) emits `card_swiped_right` / `card_swiped_left`,
-- which the telemetry_events CTE in stg_unified_events maps to swipe_right / swipe_left.
-- swipes_current (dextr_places) is capped at < 2026-01-30 to prevent double-counting
-- with dextr-context swipes that are also written to app_events.
--
-- This test fails if the telemetry-era swipe counts in stg_unified_events diverge from
-- the raw app_events counts by more than a small dedup tolerance. A large divergence
-- means either (a) the CASE mapping regressed, or (b) swipes_current started
-- leaking telemetry-era rows again.

with unified_telemetry_swipes as (
    select count(*) as n
    from {{ ref('stg_unified_events') }}
    where data_era = 'telemetry'
      and event_type in ('swipe_right', 'swipe_left')
),
raw_telemetry_swipes as (
    select count(*) as n
    from {{ source('public', 'app_events') }}
    where event_timestamp >= '2026-01-30'::timestamptz
      and event_name in ('card_swiped_right', 'card_swiped_left')
      and user_id is not null
)
-- Allow up to 1% tolerance for client_event_id dedup; fail otherwise.
select
    u.n as unified_count,
    r.n as raw_count,
    u.n - r.n as delta
from unified_telemetry_swipes u
cross join raw_telemetry_swipes r
where abs(u.n - r.n) > greatest(r.n * 0.01, 10)
