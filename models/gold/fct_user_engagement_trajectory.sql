{{ config(materialized='table') }}

-- User Engagement Trajectory
-- Grain: one row per user per activity week
-- Answers: "Prompts per user per week — increasing or decreasing?",
--          "Saves per user per week by cohort?",
--          "Is time to first save getting faster?",
--          "Average session duration trend?"

with user_activation as (
    select
        user_id,
        activation_date,
        activation_week
    from {{ ref('fct_user_activation') }}
    where is_activated = true
      and activation_date is not null
),

-- Weekly session aggregates per user
session_weekly as (
    select
        s.user_id,
        s.session_week as activity_week,
        count(*) as sessions_count,
        sum(s.save_count) as saves_count,
        sum(s.share_count) as shares_count,
        count(*) filter (where s.is_prompt_session) as prompt_sessions_count,
        count(*) filter (where s.has_save) as sessions_with_save,
        count(*) filter (where s.has_share) as sessions_with_share,
        count(*) filter (where s.is_no_value_session) as sessions_zero_actions,
        count(*) filter (
            where not s.has_save and not s.is_no_value_session
        ) as sessions_swipe_no_save,
        avg(s.session_duration_seconds) as avg_session_duration_seconds,
        avg(s.save_count) as avg_saves_per_session,
        avg(s.save_count + s.share_count) as avg_cards_per_session,
        avg(s.time_to_first_save_seconds) filter (
            where s.time_to_first_save_seconds is not null
        ) as avg_time_to_first_save_seconds
    from {{ ref('fct_session_outcomes') }} s
    group by s.user_id, s.session_week
),

-- Weekly event-level aggregates per user
event_weekly as (
    select
        user_id,
        date_trunc('week', event_timestamp)::date as activity_week,
        count(*) filter (where event_type = 'query') as prompts_count,
        count(*) filter (where event_type in ('swipe_left', 'swipe_right')) as swipes_count,
        count(*) filter (
            where event_type in ('swipe_left', 'swipe_right', 'place_detail_view_open', 'detail_view')
        ) as cards_viewed,
        count(*) filter (
            where event_type in ('opened_website', 'book_with_deck', 'click_directions', 'click_phone')
        ) as conversions_count,
        count(distinct card_id) filter (
            where card_id is not null
        ) as unique_places_interacted,
        count(distinct date(event_timestamp)) as days_active_in_week
    from {{ ref('stg_unified_events') }}
    group by user_id, date_trunc('week', event_timestamp)::date
),

-- Cumulative metrics via window functions
combined as (
    select
        coalesce(sw.user_id, ew.user_id) as user_id,
        coalesce(sw.activity_week, ew.activity_week) as activity_week,
        a.activation_week,

        -- Weeks since activation
        case
            when a.activation_week is not null
            then ((coalesce(sw.activity_week, ew.activity_week) - a.activation_week) / 7)::integer
            else null
        end as weeks_since_activation,

        -- Session metrics
        coalesce(sw.sessions_count, 0) as sessions_count,
        coalesce(ew.prompts_count, 0) as prompts_count,
        coalesce(ew.swipes_count, 0) as swipes_count,
        coalesce(sw.saves_count, 0) as saves_count,
        coalesce(sw.shares_count, 0) as shares_count,
        coalesce(ew.cards_viewed, 0) as cards_viewed,
        coalesce(ew.conversions_count, 0) as conversions_count,
        coalesce(ew.unique_places_interacted, 0) as unique_places_interacted,
        coalesce(ew.days_active_in_week, 0) as days_active_in_week,

        -- Session quality
        sw.avg_session_duration_seconds,
        sw.avg_cards_per_session,
        sw.avg_saves_per_session,
        sw.avg_time_to_first_save_seconds,

        -- Session outcome rates
        case
            when coalesce(sw.sessions_count, 0) > 0
            then round(coalesce(sw.sessions_with_save, 0)::numeric / sw.sessions_count, 4)
            else null
        end as pct_sessions_with_save,
        case
            when coalesce(sw.sessions_count, 0) > 0
            then round(coalesce(sw.sessions_with_share, 0)::numeric / sw.sessions_count, 4)
            else null
        end as pct_sessions_with_share,
        case
            when coalesce(sw.sessions_count, 0) > 0
            then round(coalesce(sw.sessions_zero_actions, 0)::numeric / sw.sessions_count, 4)
            else null
        end as pct_sessions_zero_actions,
        case
            when coalesce(sw.sessions_count, 0) > 0
            then round(coalesce(sw.sessions_swipe_no_save, 0)::numeric / sw.sessions_count, 4)
            else null
        end as pct_sessions_swipe_no_save,

        -- Swipe to save rate
        case
            when coalesce(ew.swipes_count, 0) > 0
            then round(coalesce(sw.saves_count, 0)::numeric / ew.swipes_count, 4)
            else null
        end as swipe_to_save_rate,

        -- Active flag
        true as is_active_week

    from session_weekly sw
    full outer join event_weekly ew
        on sw.user_id = ew.user_id
        and sw.activity_week = ew.activity_week
    left join user_activation a
        on coalesce(sw.user_id, ew.user_id) = a.user_id
)

select
    c.*,

    -- Cumulative metrics (running totals)
    sum(c.sessions_count) over (
        partition by c.user_id order by c.activity_week
        rows unbounded preceding
    ) as cumulative_sessions,
    sum(c.saves_count) over (
        partition by c.user_id order by c.activity_week
        rows unbounded preceding
    ) as cumulative_saves,
    sum(c.shares_count) over (
        partition by c.user_id order by c.activity_week
        rows unbounded preceding
    ) as cumulative_shares

from combined c
