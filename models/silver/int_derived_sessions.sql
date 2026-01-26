-- Derive user sessions from events using 10-minute gap threshold
-- Different from user_session_derived.sql which uses 30-minute gap
-- This is optimized for North Star metrics planning session analysis

with events_with_gaps as (
    select
        user_id,
        event_id,
        event_timestamp,
        event_name,
        event_source,
        event_category,
        card_id,
        source,

        -- Calculate seconds since last event for this user
        extract(epoch from (
            event_timestamp - lag(event_timestamp) over (
                partition by user_id
                order by event_timestamp
            )
        )) as seconds_since_last_event

    from {{ ref('int_all_events_unified') }}
    where user_id is not null
),

session_boundaries as (
    select
        *,
        -- Mark session starts: first event or gap > 10 minutes (600 seconds)
        case
            when seconds_since_last_event is null then 1  -- First event ever
            when seconds_since_last_event > 600 then 1    -- New session after 10-min gap
            else 0
        end as is_session_start
    from events_with_gaps
),

sessions_numbered as (
    select
        *,
        -- Create session number by cumulative sum of session starts
        sum(is_session_start) over (
            partition by user_id
            order by event_timestamp
            rows between unbounded preceding and current row
        ) as session_number
    from session_boundaries
),

session_aggregates as (
    select
        user_id,
        session_number,

        -- Session timing
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,

        -- Session duration in seconds
        extract(epoch from (
            max(event_timestamp) - min(event_timestamp)
        )) as session_duration_seconds,

        -- Event counts
        count(*) as event_count,
        count(distinct event_name) as unique_event_types,
        count(distinct card_id) filter (where card_id is not null) as unique_cards,
        count(distinct event_source) as unique_sources,

        -- Surface flags
        max(case when event_source in ('dextr', 'dextr_pack', 'dextr_pack_legacy') then 1 else 0 end) as has_prompt,
        max(case when event_source in ('featured_section', 'featured_carousel') then 1 else 0 end) as has_featured,
        max(case when event_source = 'multiplayer' then 1 else 0 end) as has_multiplayer,
        max(case when event_source = 'board' then 1 else 0 end) as has_board,

        -- Determine primary surface (first event source in session)
        min(case when is_session_start = 1 then event_source end) as first_event_source,

        -- Planning initiation check
        max(case
            when event_name in ('dextr_query', 'multiplayer_create', 'multiplayer_join')
                or event_source in ('dextr', 'featured_section')
            then 1
            else 0
        end) as is_planning_session

    from sessions_numbered
    group by user_id, session_number
)

select
    -- Generate unique session ID
    md5(user_id::text || '-' || session_number::text || '-' || session_start::text) as derived_session_id,
    user_id,
    session_number,
    session_start,
    session_end,
    session_duration_seconds,
    round(session_duration_seconds / 60.0, 2) as session_duration_minutes,
    event_count,
    unique_event_types,
    unique_cards,
    unique_sources,

    -- Surface flags
    has_prompt::boolean as has_prompt,
    has_featured::boolean as has_featured,
    has_multiplayer::boolean as has_multiplayer,
    has_board::boolean as has_board,

    -- Primary surface determination
    case
        when first_event_source in ('dextr', 'dextr_pack', 'dextr_pack_legacy') then 'prompt'
        when first_event_source in ('featured_section', 'featured_carousel') then 'featured'
        when first_event_source = 'multiplayer' then 'multiplayer'
        when first_event_source = 'board' then 'board'
        else 'other'
    end as primary_surface,

    is_planning_session::boolean as is_planning_session,

    -- Temporal dimensions
    date(session_start) as session_date,
    date_trunc('week', session_start)::date as session_week,
    extract(hour from session_start) as session_hour,
    extract(dow from session_start) as session_day_of_week

from session_aggregates
