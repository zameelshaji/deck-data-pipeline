-- Phase B.5 regression guard: stg_unified_events and stg_app_events_enriched
-- must produce identical aggregate origin_surface distributions for telemetry-era
-- app_events rows, since both models apply the same normalization CASE.
--
-- If these diverge, one of the two models has been updated without the other.
-- Fail to force them back in sync.
--
-- This uses an aggregate comparison (count per surface) rather than a row-level
-- join because stg_unified_events does not carry a unique row identifier —
-- (event_timestamp, user_id, card_id) is not unique across events and leads
-- to spurious join matches.

with unified_counts as (
    select
        coalesce(origin_surface, '__null__') as surface,
        count(*) as n
    from {{ ref('stg_unified_events') }}
    where data_era = 'telemetry'
      and source_table = 'app_events'
    group by 1
),
enriched_counts as (
    select
        coalesce(origin_surface, '__null__') as surface,
        count(*) as n
    from {{ ref('stg_app_events_enriched') }}
    where event_timestamp >= '2026-01-30'::timestamptz
    group by 1
),
combined as (
    select surface, n, 'unified' as src from unified_counts
    union all
    select surface, n, 'enriched' as src from enriched_counts
),
pivoted as (
    select
        surface,
        max(n) filter (where src = 'unified') as unified_n,
        max(n) filter (where src = 'enriched') as enriched_n
    from combined
    group by surface
)
select *
from pivoted
where coalesce(unified_n, 0) <> coalesce(enriched_n, 0)
