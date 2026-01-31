{{ config(materialized='table') }}

with events_ordered as (
    select
        *,
        lag(event_timestamp) over (
            partition by user_id
            order by event_timestamp
        ) as prev_event_timestamp
    from {{ ref('int_inferred_session_events') }}
),

events_with_gaps as (
    select
        *,
        extract(epoch from (event_timestamp - prev_event_timestamp)) / 60.0
            as minutes_since_prev
    from events_ordered
),

events_with_session_breaks as (
    select
        *,
        case
            when prev_event_timestamp is null then 1
            when minutes_since_prev > 10 then 1
            else 0
        end as is_session_start
    from events_with_gaps
),

events_with_session_number as (
    select
        *,
        sum(is_session_start) over (
            partition by user_id
            order by event_timestamp
            rows unbounded preceding
        ) as session_number
    from events_with_session_breaks
),

session_aggregates as (
    select
        user_id,
        session_number,

        md5(user_id::text || '_inferred_' || session_number::text)::uuid
            as inferred_session_id,

        min(event_timestamp) as started_at,
        max(event_timestamp) as ended_at,
        extract(epoch from (max(event_timestamp) - min(event_timestamp)))::integer
            as session_duration_seconds,
        date(min(event_timestamp)) as session_date,
        date_trunc('week', min(event_timestamp))::date as session_week,

        case
            when bool_or(event_type = 'query') then 'dextr'
            when bool_or(source_table = 'featured_section_actions') then 'featured'
            else 'organic'
        end as initiation_surface,

        array_agg(distinct pack_id) filter (where pack_id is not null) as pack_ids,
        (array_agg(pack_id order by event_timestamp) filter (where pack_id is not null))[1]
            as primary_pack_id,

        (array_agg(app_version order by event_timestamp) filter (where app_version is not null))[1]
            as app_version,

        count(*) as event_count,
        count(*) filter (where event_type = 'query') as query_count,
        count(*) filter (where event_type in ('swipe_right', 'swipe_left')) as swipe_count,
        count(*) filter (where event_type = 'save') as save_event_count,
        count(*) filter (where event_type = 'card_share') as share_event_count

    from events_with_session_number
    group by user_id, session_number
)

select
    inferred_session_id as session_id,
    user_id,
    started_at,
    ended_at,
    session_duration_seconds,
    session_date,
    session_week,
    initiation_surface,
    pack_ids,
    primary_pack_id,
    app_version,
    event_count,
    query_count,
    swipe_count,
    save_event_count,
    share_event_count,
    query_count > 0 as is_prompt_session,
    'inferred' as data_source,
    false as has_native_session_id
from session_aggregates
