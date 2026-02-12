{{ config(materialized='table') }}

-- Conversion Signals — Booking Intent & Monetisation Readiness
-- Grain: one row per conversion action
-- Answers: "How many saved cards have external links clicked?",
--          "Which card types get more booking action?",
--          "Share rate by user type?"

with conversion_events as (
    select
        e.user_id,
        e.card_id,
        e.event_type,
        e.event_timestamp,
        date(e.event_timestamp) as action_date,
        e.pack_id,
        e.data_era
    from {{ ref('stg_unified_events') }} e
    inner join {{ ref('stg_users') }} u on e.user_id = u.user_id
    where e.event_category = 'Conversion'
      and u.is_test_user = 0
),

-- Check if user saved this card before converting
prior_saves as (
    select
        ce.user_id,
        ce.card_id,
        ce.event_timestamp as conversion_timestamp,
        max(s.event_timestamp) as last_save_before_conversion,
        true as was_saved_first
    from conversion_events ce
    inner join {{ ref('stg_unified_events') }} s
        on ce.user_id = s.user_id
        and ce.card_id = s.card_id
        and s.event_type in ('save', 'saved')
        and s.event_timestamp < ce.event_timestamp
    group by ce.user_id, ce.card_id, ce.event_timestamp
),

-- Map to sessions
conversion_sessions as (
    select
        ce.user_id,
        ce.event_timestamp,
        so.session_id,
        so.is_prompt_session
    from conversion_events ce
    inner join {{ ref('fct_session_outcomes') }} so
        on ce.user_id = so.user_id
        and ce.event_timestamp between so.started_at and so.ended_at
)

select
    row_number() over (order by ce.event_timestamp) as action_id,
    ce.user_id,
    ce.card_id,

    -- Place attributes (from stg_cards)
    c.name as place_name,
    c.category as place_category,
    c.price_level,
    c.rating,
    c.is_featured,
    c.source_type,

    -- Action details
    ce.event_type as action_type,
    ce.event_timestamp as action_timestamp,
    ce.action_date,
    ce.data_era,

    -- Save context
    coalesce(ps.was_saved_first, false) as was_saved_first,
    case
        when ps.last_save_before_conversion is not null
        then round(extract(epoch from (ce.event_timestamp - ps.last_save_before_conversion)) / 60.0, 1)
        else null
    end as time_from_save_to_conversion_minutes,

    -- Session context
    cs.session_id,
    coalesce(cs.is_prompt_session, false) as was_prompt_initiated

from conversion_events ce
left join {{ ref('stg_cards') }} c on ce.card_id = c.card_id
left join prior_saves ps
    on ce.user_id = ps.user_id
    and ce.card_id = ps.card_id
    and ce.event_timestamp = ps.conversion_timestamp
left join conversion_sessions cs
    on ce.user_id = cs.user_id
    and ce.event_timestamp = cs.event_timestamp
