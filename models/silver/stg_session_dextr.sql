-- Per-session Dextr query→result funnel aggregate.
--
-- Tracks the two-step funnel at the session grain:
--   1. dextr_query_submitted — user submitted a query
--   2. dextr_results_viewed  — Dextr's returned card pack was displayed
--
-- Previously only step 1 was visible (via src_dextr_queries). This model
-- surfaces step 2 from the telemetry-era app_events and adds timing so
-- downstream fct_dextr_funnel can compute query→display latency.

with dextr_events as (
    select
        effective_session_id as session_id,
        user_id,
        event_name,
        event_timestamp as dextr_at,
        query_id_prop as query_id,
        fast_path
    from {{ ref('stg_app_events_enriched') }}
    where event_name in ('dextr_query_submitted', 'dextr_results_viewed')
      and effective_session_id is not null
),

session_dextr as (
    select
        session_id,
        user_id,
        count(*) filter (where event_name = 'dextr_query_submitted') as queries_submitted,
        count(*) filter (where event_name = 'dextr_results_viewed') as results_views,
        count(*) filter (where event_name = 'dextr_query_submitted' and fast_path = 'suggestion_pill') as fast_path_queries,
        count(distinct query_id) filter (where event_name = 'dextr_query_submitted' and query_id is not null) as distinct_query_ids,
        min(dextr_at) filter (where event_name = 'dextr_query_submitted') as first_query_at,
        min(dextr_at) filter (where event_name = 'dextr_results_viewed') as first_result_view_at,
        -- Query-to-result latency in seconds. Uses per-session min timestamps
        -- as a heuristic — works when there's a single query per session;
        -- fct_dextr_funnel will compute more precise per-query latencies.
        extract(epoch from (
            min(dextr_at) filter (where event_name = 'dextr_results_viewed')
          - min(dextr_at) filter (where event_name = 'dextr_query_submitted')
        )) as first_query_to_result_seconds
    from dextr_events
    group by session_id, user_id
)

select * from session_dextr
