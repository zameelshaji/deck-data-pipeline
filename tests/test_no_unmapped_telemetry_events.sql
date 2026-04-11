-- Phase B regression guard: every frequently-emitted telemetry event must
-- have a dedicated event_type mapping in stg_unified_events.sql (not fall
-- through to event_category='Other').
--
-- Fails if any event_name in the telemetry era has > 10 occurrences and is
-- bucketed as 'Other'. A small allowlist covers onboarding events (handled
-- via LIKE pattern downstream) and any transient noise.
--
-- When iOS adds a new event_name, this test will fail, prompting a pipeline
-- update to extend the telemetry_events CASE in stg_unified_events.

with offenders as (
    select event_type, count(*) as n
    from {{ ref('stg_unified_events') }}
    where data_era = 'telemetry'
      and event_category = 'Other'
      and event_type not like 'onboarding_%'
    group by 1
)
select *
from offenders
where n > 10
