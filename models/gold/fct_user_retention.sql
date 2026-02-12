-- CEO User Retention Fact Table
-- Comprehensive user-level retention view for executive reporting
-- Grain: one row per activated user
-- Joins activation, retention, engagement, and segmentation data
-- Definitions:
--   Activated = ≥1 prompt OR ≥1 save OR ≥1 share
--   Retained  = came back and did ≥1 prompt/save/share within window

with activated_users as (
    select
        user_id,
        signup_date,
        activation_date,
        cohort_week,
        days_to_activation,
        activation_type,
        is_activated
    from {{ ref('fct_user_activation') }}
    where is_activated = true
      and activation_date is not null
),

-- User profile & segmentation (from fct_user_segments, replaces disabled user_segmentation)
user_profiles as (
    select
        user_id,
        email,
        username,
        full_name,
        case when is_planner then 'Planner' else 'Passenger' end as user_type,
        total_prompts,
        total_saves,
        total_shares,
        total_boards_created as total_decks_created,
        total_multiplayer_sessions as total_multiplayer_sessions_created,
        total_referrals_given,
        total_conversions,
        days_active,
        last_activity_date,
        days_since_last_activity
    from {{ ref('fct_user_segments') }}
),

-- Post-activation session activity (prompt/save/share only)
post_activation_sessions as (
    select
        s.user_id,
        a.activation_date,

        -- Session counts in retention windows
        count(*) filter (
            where s.session_date between a.activation_date + 1 and a.activation_date + 7
        ) as sessions_d7,
        count(*) filter (
            where s.session_date between a.activation_date + 1 and a.activation_date + 30
        ) as sessions_d30,
        count(*) filter (
            where s.session_date between a.activation_date + 1 and a.activation_date + 60
        ) as sessions_d60,
        count(*) filter (
            where s.session_date between a.activation_date + 1 and a.activation_date + 90
        ) as sessions_d90,

        -- Total sessions all-time (post-activation)
        count(*) filter (
            where s.session_date > a.activation_date
        ) as sessions_post_activation,

        -- Total sessions to date (all sessions including activation day)
        count(*) as total_sessions_to_date,

        -- Last meaningful activity date (prompt, save, or share)
        max(s.session_date) filter (
            where s.has_save or s.has_share or s.is_prompt_session
        ) as last_meaningful_activity_date,

        -- Save counts in windows
        sum(s.save_count) filter (
            where s.session_date between a.activation_date + 1 and a.activation_date + 7
        ) as saves_d7,
        sum(s.save_count) filter (
            where s.session_date between a.activation_date + 1 and a.activation_date + 30
        ) as saves_d30,

        -- Share counts in windows
        sum(s.share_count) filter (
            where s.session_date between a.activation_date + 1 and a.activation_date + 7
        ) as shares_d7,
        sum(s.share_count) filter (
            where s.session_date between a.activation_date + 1 and a.activation_date + 30
        ) as shares_d30,

        -- Retention flags (had meaningful activity = prompt, save, or share)
        bool_or(
            s.session_date between a.activation_date + 1 and a.activation_date + 7
            and (s.has_save or s.has_share or s.is_prompt_session)
        ) as retained_d7,
        bool_or(
            s.session_date between a.activation_date + 1 and a.activation_date + 30
            and (s.has_save or s.has_share or s.is_prompt_session)
        ) as retained_d30,
        bool_or(
            s.session_date between a.activation_date + 1 and a.activation_date + 60
            and (s.has_save or s.has_share or s.is_prompt_session)
        ) as retained_d60,
        bool_or(
            s.session_date between a.activation_date + 1 and a.activation_date + 90
            and (s.has_save or s.has_share or s.is_prompt_session)
        ) as retained_d90

    from activated_users a
    left join {{ ref('fct_session_outcomes') }} s
        on a.user_id = s.user_id
    group by s.user_id, a.activation_date
)

select
    -- Identity
    a.user_id,
    p.email,
    p.username,
    p.full_name,

    -- Activation details
    a.signup_date,
    a.activation_date,
    a.cohort_week,
    a.days_to_activation,
    a.activation_type,

    -- Segmentation
    p.user_type,

    -- Maturity flags
    current_date >= a.activation_date + 7 as is_mature_d7,
    current_date >= a.activation_date + 30 as is_mature_d30,
    current_date >= a.activation_date + 60 as is_mature_d60,
    current_date >= a.activation_date + 90 as is_mature_d90,

    -- Retention flags
    coalesce(s.retained_d7, false) as retained_d7,
    coalesce(s.retained_d30, false) as retained_d30,
    coalesce(s.retained_d60, false) as retained_d60,
    coalesce(s.retained_d90, false) as retained_d90,

    -- Post-activation engagement depth
    coalesce(s.sessions_d7, 0) as sessions_d7,
    coalesce(s.sessions_d30, 0) as sessions_d30,
    coalesce(s.sessions_d60, 0) as sessions_d60,
    coalesce(s.sessions_d90, 0) as sessions_d90,
    coalesce(s.sessions_post_activation, 0) as sessions_post_activation,
    coalesce(s.total_sessions_to_date, 0) as total_sessions_to_date,
    s.last_meaningful_activity_date,

    coalesce(s.saves_d7, 0) as saves_d7,
    coalesce(s.saves_d30, 0) as saves_d30,
    coalesce(s.shares_d7, 0) as shares_d7,
    coalesce(s.shares_d30, 0) as shares_d30,

    -- All-time engagement (from segmentation)
    coalesce(p.total_prompts, 0) as total_prompts,
    coalesce(p.total_saves, 0) as total_saves,
    coalesce(p.total_shares, 0) as total_shares,
    coalesce(p.total_decks_created, 0) as total_decks_created,
    coalesce(p.total_multiplayer_sessions_created, 0) as total_multiplayer_sessions_created,
    coalesce(p.total_referrals_given, 0) as total_referrals_given,
    coalesce(p.total_conversions, 0) as total_conversions,
    coalesce(p.days_active, 0) as days_active,
    p.last_activity_date,
    p.days_since_last_activity,

    -- Churned flag (no activity in last 30 days)
    case
        when p.days_since_last_activity is null then true
        when p.days_since_last_activity > 60 then true
        else false
    end as is_churned

from activated_users a
left join user_profiles p on a.user_id = p.user_id
left join post_activation_sessions s on a.user_id = s.user_id
