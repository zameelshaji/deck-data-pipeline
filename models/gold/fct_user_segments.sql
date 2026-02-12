{{ config(materialized='table') }}

-- User Archetypes & Segmentation
-- Grain: one row per user (non-test)
-- Answers: "Who are your best users?", "What % are one-and-done?",
--          "Top 10 users by saves", "Users who shared 3+ times — what do they have in common?"
--
-- Activation: first prompt OR save OR share (deliberately low bar)
-- Planner:   saved AND shared at least once (lifetime)
-- Passenger:  any activated user who is NOT a planner

with activation as (
    select
        user_id,
        signup_date,
        signup_week,
        is_activated,
        activation_date,
        activation_week,
        cohort_week,
        days_to_activation,
        activation_type,
        first_prompt_at,
        first_save_at,
        first_share_at
    from {{ ref('fct_user_activation') }}
),

-- Session-level aggregates per user
session_metrics as (
    select
        user_id,
        count(*) as total_sessions,
        count(*) filter (where has_save) as sessions_with_save,
        count(*) filter (where has_share) as sessions_with_share,
        count(*) filter (where is_no_value_session) as sessions_zero_value,
        sum(save_count) as total_saves_sessions,
        sum(share_count) as total_shares_sessions,
        avg(session_duration_seconds) as avg_session_duration_seconds,
        avg(save_count) as avg_saves_per_session,
        avg(time_to_first_save_seconds) filter (where time_to_first_save_seconds is not null)
            as avg_time_to_first_save_seconds,
        max(session_date) as last_session_date
    from {{ ref('fct_session_outcomes') }}
    group by user_id
),

-- Engagement totals from unified events
user_activity as (
    select
        user_id,
        count(*) filter (where event_type = 'query') as total_prompts,
        count(*) filter (where event_type in ('swipe_left', 'swipe_right')) as total_swipes,
        count(*) filter (where event_type in ('save', 'saved')) as total_saves,
        count(*) filter (where event_category = 'Share') as total_shares,
        count(*) filter (
            where event_type in ('opened_website', 'book_with_deck', 'click_directions', 'click_phone')
        ) as total_conversions,
        count(distinct date(event_timestamp)) as days_active,
        max(date(event_timestamp)) as last_activity_date
    from {{ ref('stg_unified_events') }}
    group by user_id
),

-- Boards/decks created
user_decks as (
    select
        user_id,
        count(*) filter (where is_default = false) as total_boards_created
    from {{ ref('src_boards') }}
    group by user_id
),

-- Multiplayer sessions
user_multiplayer as (
    select
        creator_id as user_id,
        count(*) as total_multiplayer_sessions
    from {{ ref('stg_multiplayer') }}
    group by creator_id
),

-- Referrals given
user_referrals as (
    select
        referrer_user_id as user_id,
        count(distinct referred_user_id) as total_referrals_given
    from {{ ref('stg_referral_relationships') }}
    group by referrer_user_id
),

-- Check referral source for each user
user_referral_source as (
    select
        referred_user_id as user_id,
        'referral' as referral_source
    from {{ ref('stg_referral_relationships') }}
),

-- Retention data
retention as (
    select
        user_id,
        had_activity_d7 as retained_d7,
        had_activity_d30 as retained_d30,
        had_activity_d60 as retained_d60,
        is_mature_d7,
        is_mature_d30,
        is_mature_d60
    from {{ ref('fct_retention_activated') }}
),

-- Combine everything
combined as (
    select
        -- Identity
        u.user_id,
        u.email,
        u.username,
        u.full_name,
        date(u.created_at) as signup_date,
        date_trunc('week', u.created_at)::date as signup_week,

        -- Activation
        coalesce(a.is_activated, false) as is_activated,
        a.activation_date,
        a.activation_week,
        case
            when a.first_prompt_at is not null
                 and a.first_prompt_at = least(a.first_prompt_at, a.first_save_at, a.first_share_at)
                then 'first_prompt'
            when a.first_save_at is not null
                 and a.first_save_at = least(a.first_prompt_at, a.first_save_at, a.first_share_at)
                then 'first_save'
            when a.first_share_at is not null
                 and a.first_share_at = least(a.first_prompt_at, a.first_save_at, a.first_share_at)
                then 'first_share'
            else null
        end as activation_trigger,
        a.days_to_activation,

        -- Lifecycle
        (current_date - date(u.created_at))::integer as days_since_signup,
        coalesce(ua.last_activity_date, date(sm.last_session_date)) as last_activity_date,
        case
            when coalesce(ua.last_activity_date, date(sm.last_session_date)) is not null
            then (current_date - coalesce(ua.last_activity_date, date(sm.last_session_date)))::integer
            else null
        end as days_since_last_activity,

        -- Engagement totals
        coalesce(sm.total_sessions, 0) as total_sessions,
        coalesce(ua.total_prompts, 0) as total_prompts,
        coalesce(ua.total_swipes, 0) as total_swipes,
        coalesce(ua.total_saves, 0) as total_saves,
        coalesce(ua.total_shares, 0) as total_shares,
        coalesce(ud.total_boards_created, 0) as total_boards_created,
        coalesce(um.total_multiplayer_sessions, 0) as total_multiplayer_sessions,
        coalesce(ua.total_conversions, 0) as total_conversions,
        coalesce(ur.total_referrals_given, 0) as total_referrals_given,

        -- Engagement quality
        coalesce(sm.avg_saves_per_session, 0) as avg_saves_per_session,
        sm.avg_session_duration_seconds,
        case
            when coalesce(ua.total_prompts, 0) > 0
            then round(coalesce(ua.total_saves, 0)::numeric / ua.total_prompts, 4)
            else null
        end as prompt_to_save_rate,
        case
            when coalesce(ua.total_swipes, 0) > 0
            then round(coalesce(ua.total_saves, 0)::numeric / ua.total_swipes, 4)
            else null
        end as swipe_to_save_rate,
        case
            when coalesce(sm.total_sessions, 0) > 0
            then round(coalesce(sm.sessions_with_save, 0)::numeric / sm.total_sessions, 4)
            else null
        end as sessions_with_save_pct,
        case
            when coalesce(sm.total_sessions, 0) > 0
            then round(coalesce(sm.sessions_with_share, 0)::numeric / sm.total_sessions, 4)
            else null
        end as sessions_with_share_pct,

        -- Activated in first session?
        -- (activation date = first session date)
        case
            when a.activation_date is not null and sm.total_sessions > 0
            then a.days_to_activation = 0
            else null
        end as activated_in_first_session,

        -- Retention
        coalesce(r.retained_d7, false) as retained_d7,
        coalesce(r.retained_d30, false) as retained_d30,
        coalesce(r.retained_d60, false) as retained_d60,

        -- Preferences
        u.likes_adventure,
        u.likes_dining,
        u.likes_drinks,
        u.likes_culture,
        u.likes_entertainment,
        u.likes_health,
        u.prefers_solo,
        u.prefers_friends,
        u.prefers_family,
        u.prefers_date,

        -- Referral source
        coalesce(urs.referral_source, 'organic') as referral_source,

        coalesce(ua.days_active, 0) as days_active

    from {{ ref('stg_users') }} u
    left join activation a on u.user_id = a.user_id
    left join session_metrics sm on u.user_id = sm.user_id
    left join user_activity ua on u.user_id = ua.user_id
    left join user_decks ud on u.user_id = ud.user_id
    left join user_multiplayer um on u.user_id = um.user_id
    left join user_referrals ur on u.user_id = ur.user_id
    left join user_referral_source urs on u.user_id = urs.user_id
    left join retention r on u.user_id = r.user_id
    where u.is_test_user = 0
)

select
    *,

    -- User archetype (post-activation behavior)
    case
        when not is_activated then null
        when total_sessions = 1 and days_active <= 1 then 'one_and_done'
        when total_saves = 0 and total_shares = 0 then 'browser'
        when total_saves > 0 and total_shares = 0 then 'saver'
        when total_saves > 0 and total_shares > 0 and retained_d7 then 'power_planner'
        when total_saves > 0 and total_shares > 0 then 'planner'
        else 'browser'
    end as user_archetype,

    -- Planner = saved AND shared at least once (lifetime)
    -- Passenger = any activated user who is NOT a planner
    case
        when is_activated
             and total_saves > 0
             and total_shares > 0
        then true
        else false
    end as is_planner,

    case
        when is_activated
             and not (total_saves > 0 and total_shares > 0)
        then true
        else false
    end as is_passenger,

    -- Churn risk
    case
        when days_since_last_activity is null then 'high'
        when days_since_last_activity >= 14 and total_sessions < 3 then 'high'
        when days_since_last_activity between 7 and 13 then 'medium'
        when days_since_last_activity < 7 then 'low'
        else 'high'
    end as churn_risk,

    -- Is churned (no activity in 30+ days)
    case
        when days_since_last_activity is null then true
        when days_since_last_activity > 30 then true
        else false
    end as is_churned

from combined
