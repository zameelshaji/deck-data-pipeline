-- All events tagged with their derived_session_id
-- Enables session-level analysis and metric calculation

with sessions as (
    select
        derived_session_id,
        user_id,
        session_start,
        session_end
    from {{ ref('int_derived_sessions') }}
),

events as (
    select
        event_id,
        user_id,
        card_id,
        event_source,
        event_name,
        event_category,
        source,
        source_id,
        event_timestamp,
        event_date,
        event_week
    from {{ ref('int_all_events_unified') }}
)

select
    s.derived_session_id,
    e.event_id,
    e.user_id,
    e.card_id,
    e.event_source,
    e.event_name,
    e.event_category,
    e.source,
    e.source_id,
    e.event_timestamp,
    e.event_date,
    e.event_week,

    -- Position within session
    row_number() over (
        partition by s.derived_session_id
        order by e.event_timestamp
    ) as event_sequence_in_session,

    -- Time since session start (in seconds)
    extract(epoch from (e.event_timestamp - s.session_start)) as seconds_since_session_start,

    -- Is this the first/last event in session?
    case
        when e.event_timestamp = s.session_start then true
        else false
    end as is_first_event,

    case
        when e.event_timestamp = s.session_end then true
        else false
    end as is_last_event

from events e
inner join sessions s
    on e.user_id = s.user_id
    and e.event_timestamp >= s.session_start
    and e.event_timestamp <= s.session_end
