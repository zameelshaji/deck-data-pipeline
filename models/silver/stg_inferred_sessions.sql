{{ config(materialized='table') }}

with events_with_gaps as (
    select
        user_id,
        event_timestamp,
        card_id,
        event_name,
        event_source,
        extract(epoch from (
            event_timestamp - lag(event_timestamp) over (
                partition by user_id order by event_timestamp
            )
        )) / 60.0 as minutes_since_prev_event
    from {{ ref('stg_events') }}
    where user_id is not null
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
    from events_with_gaps
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

session_aggregates as (
    select
        user_id,
        inferred_session_number,

        md5(user_id::text || '_inferred_' || inferred_session_number::text)::uuid
            as inferred_session_id,

        min(event_timestamp) as session_started_at,
        max(event_timestamp) as session_ended_at,
        date(min(event_timestamp)) as session_date,
        date_trunc('week', min(event_timestamp))::date as session_week,
        extract(epoch from (max(event_timestamp) - min(event_timestamp)))::integer
            as session_duration_seconds,

        count(*) as total_events,
        count(distinct card_id) as unique_cards_interacted,
        count(*) filter (where event_name = 'dextr_query') as prompt_count,
        count(*) filter (where event_name in ('saved', 'swipe_right')) as save_count,
        count(*) filter (where event_name = 'share') as share_count,

        -- First event source for initiation surface derivation
        (array_agg(event_source order by event_timestamp))[1] as first_event_source

    from events_with_session_number
    group by user_id, inferred_session_number
)

select
    inferred_session_id,
    user_id,
    inferred_session_number,
    session_started_at,
    session_ended_at,
    session_date,
    session_week,
    session_duration_seconds,
    total_events,
    unique_cards_interacted,
    prompt_count,
    save_count,
    share_count,
    (prompt_count > 0 or save_count > 0 or share_count > 0) as is_genuine_planning_attempt,
    first_event_source,
    case
        when first_event_source in ('dextr', 'dextr_pack', 'dextr_pack_legacy') then 'dextr'
        when first_event_source in ('featured_section', 'featured_carousel') then 'featured'
        else 'other'
    end as inferred_initiation_surface
from session_aggregates
