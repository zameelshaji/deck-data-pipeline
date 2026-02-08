{{ config(materialized='table') }}

-- ===================================================================
-- Part A: Inferred sessions (before Jan 30 2026)
-- Uses 5-minute inactivity timeout, same windowing logic as old model
-- ===================================================================
with pre_telemetry_events as (
    select
        user_id,
        event_timestamp,
        event_type,
        card_id,
        source_table,
        event_category,
        app_version,
        extract(epoch from (
            event_timestamp - lag(event_timestamp) over (
                partition by user_id order by event_timestamp
            )
        )) / 60.0 as minutes_since_prev_event
    from {{ ref('stg_unified_events') }}
    where event_timestamp < '2026-01-30'::timestamptz
      and user_id is not null
      and event_timestamp is not null
),

events_with_session_breaks as (
    select
        *,
        case
            when minutes_since_prev_event is null or minutes_since_prev_event > 5
            then 1
            else 0
        end as is_session_start
    from pre_telemetry_events
),

events_with_session_number as (
    select
        *,
        sum(is_session_start) over (
            partition by user_id
            order by event_timestamp
            rows unbounded preceding
        ) as inferred_session_number
    from events_with_session_breaks
),

inferred_session_aggregates as (
    select
        user_id,
        inferred_session_number,

        md5(user_id::text || '_inferred_' || inferred_session_number::text)::uuid
            as session_id,

        min(event_timestamp) as started_at,
        max(event_timestamp) as ended_at,
        date(min(event_timestamp)) as session_date,
        date_trunc('week', min(event_timestamp))::date as session_week,
        extract(epoch from (max(event_timestamp) - min(event_timestamp)))::integer
            as session_duration_seconds,

        count(*) as event_count,
        count(*) filter (where event_type = 'query') as query_count,
        count(*) filter (where event_type in ('swipe_right', 'swipe_left')) as swipe_count,
        count(*) filter (where event_type = 'save') as save_count,
        count(*) filter (where event_category = 'Share') as share_count,
        count(distinct card_id) as unique_cards_interacted,

        -- First event source for initiation surface
        (array_agg(source_table order by event_timestamp))[1] as first_event_source,

        -- Get the most common app_version in this session
        (array_agg(app_version order by event_timestamp) filter (where app_version is not null))[1] as app_version,

        -- Data era from first event
        case
            when min(event_timestamp) < '2025-11-20'::timestamptz then 'card_system'
            else 'places_system'
        end as data_era

    from events_with_session_number
    group by user_id, inferred_session_number
),

inferred_sessions as (
    select
        session_id,
        user_id,
        started_at,
        ended_at,
        session_date,
        session_week,
        session_duration_seconds,
        event_count,
        query_count,
        swipe_count,
        save_count,
        share_count,
        unique_cards_interacted,
        case
            when first_event_source in ('dextr_queries') then 'dextr'
            when first_event_source = 'featured_section_actions' then 'featured'
            else 'organic'
        end as initiation_surface,
        null::text as device_type,
        app_version,
        'inferred' as session_source,
        false as has_native_session_id,
        data_era,
        (query_count > 0 or save_count > 0 or share_count > 0) as is_genuine_planning_attempt
    from inferred_session_aggregates
),

-- ===================================================================
-- Part B: Native sessions (Jan 30 2026 onwards)
-- From planning_sessions, enriched with event counts
-- ===================================================================
native_sessions_base as (
    select
        session_id,
        user_id,
        started_at,
        ended_at,
        session_date,
        session_week,
        session_duration_seconds,
        initiation_surface,
        device_type,
        app_version
    from {{ ref('stg_planning_sessions') }}
    where started_at >= '2026-01-30'::timestamptz
),

native_event_counts as (
    select
        ns.session_id,
        count(*) as event_count,
        count(*) filter (where e.event_type = 'query') as query_count,
        count(*) filter (where e.event_type in ('swipe_right', 'swipe_left')) as swipe_count,
        count(*) filter (where e.event_type = 'save') as save_count,
        count(*) filter (where e.event_category = 'Share') as share_count,
        count(distinct e.card_id) as unique_cards_interacted
    from native_sessions_base ns
    inner join {{ ref('stg_unified_events') }} e
        on ns.user_id = e.user_id
        and e.event_timestamp >= ns.started_at
        and e.event_timestamp <= coalesce(ns.ended_at, ns.started_at + interval '2 hours')
    where e.event_timestamp >= '2026-01-30'::timestamptz
    group by ns.session_id
),

native_sessions as (
    select
        ns.session_id,
        ns.user_id,
        ns.started_at,
        ns.ended_at,
        ns.session_date,
        ns.session_week,
        ns.session_duration_seconds,
        coalesce(ec.event_count, 0) as event_count,
        coalesce(ec.query_count, 0) as query_count,
        coalesce(ec.swipe_count, 0) as swipe_count,
        coalesce(ec.save_count, 0) as save_count,
        coalesce(ec.share_count, 0) as share_count,
        coalesce(ec.unique_cards_interacted, 0) as unique_cards_interacted,
        ns.initiation_surface,
        ns.device_type,
        ns.app_version,
        'native' as session_source,
        true as has_native_session_id,
        'telemetry' as data_era,
        (coalesce(ec.query_count, 0) > 0 or coalesce(ec.save_count, 0) > 0 or coalesce(ec.share_count, 0) > 0) as is_genuine_planning_attempt
    from native_sessions_base ns
    left join native_event_counts ec on ns.session_id = ec.session_id
)

-- ===================================================================
-- UNION both session types
-- ===================================================================
select * from inferred_sessions
union all
select * from native_sessions
