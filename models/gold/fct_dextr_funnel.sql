{{ config(materialized='table') }}

-- Dextr Query → Results → Conversion Funnel
--
-- Grain: one row per (metric_date, app_version).
-- Answers: "What % of dextr queries actually show results to the user?"
--          "What's the median query-to-display latency?"
--          "What's the query → save conversion rate?"
--          "How does the suggestion-pill fast path compare to normal queries?"
--
-- Telemetry era only (>= 2026-01-30), since dextr_results_viewed is a
-- telemetry-only event.

with dextr_events as (
    select
        date(event_timestamp) as metric_date,
        event_name,
        event_timestamp,
        user_id,
        effective_session_id as session_id,
        query_id_prop as query_id,
        fast_path
    from {{ ref('stg_app_events_enriched') }}
    inner join {{ ref('stg_users') }} using (user_id)
    where event_name in ('dextr_query_submitted', 'dextr_results_viewed')
      and event_timestamp >= '2026-01-30'::timestamptz
      and is_test_user = 0
),

-- Version attribution from seed (dates → release)
version_lookup as (
    select
        app_version::text as app_version,
        release_date::date as release_date,
        release_date_end::date as release_date_end
    from {{ ref('app_version_releases') }}
),

-- Sessions that have saves post-dextr-query (for query → save conversion)
session_saves_flag as (
    select session_id, has_save
    from {{ ref('fct_session_outcomes') }}
    where has_native_session_id
      and session_date >= '2026-01-30'
),

-- Per-query attribution: did this query lead to a results view in the same session?
per_query as (
    select
        q.metric_date,
        q.session_id,
        q.user_id,
        q.fast_path,
        min(q.event_timestamp) filter (where q.event_name = 'dextr_query_submitted') as query_at,
        min(q.event_timestamp) filter (where q.event_name = 'dextr_results_viewed') as results_at
    from dextr_events q
    group by q.metric_date, q.session_id, q.user_id, q.fast_path
)

select
    pq.metric_date,
    vl.app_version,
    count(*) as queries_submitted,
    count(*) filter (where pq.results_at is not null) as queries_with_results_view,
    count(*) filter (where pq.fast_path = 'suggestion_pill') as fast_path_queries,
    count(*) filter (where pq.fast_path = 'suggestion_pill' and pq.results_at is not null) as fast_path_queries_with_results,

    -- Query → save conversion (count queries whose session had any save)
    count(*) filter (where ss.has_save) as queries_leading_to_save,

    -- Rates
    round(
        count(*) filter (where pq.results_at is not null)::numeric
        / nullif(count(*), 0), 4
    ) as results_view_rate,
    round(
        count(*) filter (where ss.has_save)::numeric
        / nullif(count(*), 0), 4
    ) as query_to_save_rate,

    -- Latency (query → results)
    round(percentile_cont(0.5) within group (
        order by extract(epoch from (pq.results_at - pq.query_at))
    )::numeric, 2) as median_query_to_results_seconds,
    round(percentile_cont(0.9) within group (
        order by extract(epoch from (pq.results_at - pq.query_at))
    )::numeric, 2) as p90_query_to_results_seconds,

    min(pq.query_at) as first_query_at,
    max(pq.query_at) as last_query_at

from per_query pq
left join version_lookup vl on pq.metric_date between vl.release_date and vl.release_date_end
left join session_saves_flag ss on pq.session_id = ss.session_id
group by pq.metric_date, vl.app_version
order by pq.metric_date, vl.app_version
