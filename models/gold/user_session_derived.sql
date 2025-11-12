-- Derive user sessions from events using time-based gaps

with events_with_gaps as (
    select
        user_id,
        event_timestamp,
        event_name,
        event_source,
        card_id,
        
        -- Calculate time since last event for this user (in minutes)
        extract(epoch from (
            event_timestamp - lag(event_timestamp) over (
                partition by user_id 
                order by event_timestamp
            )
        )) / 60 as minutes_since_last_event
        
    from {{ ref('stg_events') }}
    where user_id is not null
),

session_boundaries as (
    select
        user_id,
        event_timestamp,
        event_name,
        event_source,
        card_id,
        minutes_since_last_event,
        
        -- Mark session starts: first event or gap > 30 minutes
        case 
            when minutes_since_last_event is null then 1  -- First event ever
            when minutes_since_last_event > 30 then 1      -- New session after gap
            else 0 
        end as is_session_start
        
    from events_with_gaps
),

sessions_numbered as (
    select
        user_id,
        event_timestamp,
        event_name,
        event_source,
        card_id,
        minutes_since_last_event,
        is_session_start,
        
        -- Create session ID by cumulative sum of session starts
        sum(is_session_start) over (
            partition by user_id 
            order by event_timestamp
            rows between unbounded preceding and current row
        ) as session_number
        
    from session_boundaries
),

session_metrics as (
    select
        user_id,
        session_number,
        
        -- Session timing
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,
        
        -- Session length in minutes
        extract(epoch from (
            max(event_timestamp) - min(event_timestamp)
        )) / 60 as session_length_minutes,
        
        -- Session activity
        count(*) as total_events,
        count(distinct event_name) as unique_event_types,
        count(distinct card_id) as unique_cards_interacted,
        
        -- Breakdown by event source
        count(case when event_source = 'dextr' then 1 end) as ai_events,
        count(case when event_source = 'featured_section' then 1 end) as featured_events,
        count(case when event_source = 'multiplayer' then 1 end) as multiplayer_events,
        
        -- Engagement indicators
        count(case when event_name in ('swipe_right', 'saved') then 1 end) as positive_actions,
        count(case when event_name in ('swipe_left') then 1 end) as negative_actions,
        count(case when event_name in (
            'opened_website', 'book_button_click', 'click_directions', 
            'book_with_deck', 'click_phone'
        ) then 1 end) as conversion_actions
        
    from sessions_numbered
    group by user_id, session_number
)

select
    -- Session identifiers
    user_id,
    session_number,
    session_start,
    session_end,
    
    -- Temporal dimensions
    date(session_start) as session_date,
    extract(hour from session_start) as session_hour,
    extract(dow from session_start) as day_of_week,
    date_trunc('week', session_start)::date as session_week,
    date_trunc('month', session_start)::date as session_month,
    
    -- Session metrics
    session_length_minutes,
    total_events,
    unique_event_types,
    unique_cards_interacted,
    
    -- Event source breakdown
    ai_events,
    featured_events,
    multiplayer_events,
    
    -- Engagement metrics
    positive_actions,
    negative_actions,
    conversion_actions,
    
    -- Calculated metrics
    case 
        when session_length_minutes > 0 
        then total_events / session_length_minutes 
        else total_events 
    end as events_per_minute,
    
    case 
        when total_events > 0 
        then positive_actions::float / total_events 
        else 0 
    end as positive_action_rate

from session_metrics
order by user_id, session_start