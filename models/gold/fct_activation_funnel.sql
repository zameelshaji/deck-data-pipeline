with users as (
    select
        user_id,
        date(created_at) as signup_date,
        date_trunc('week', created_at)::date as signup_week
    from {{ ref('stg_users') }}
    where is_test_user = 0
      and created_at is not null
),

-- User activity from session outcomes
user_sessions as (
    select
        user_id,
        session_date,
        has_save,
        has_share,
        has_post_share_interaction,
        is_prompt_session,
        initiation_surface
    from {{ ref('fct_session_outcomes') }}
),

user_activity as (
    select
        u.user_id,
        u.signup_date,
        u.signup_week,

        -- F1: Planning initiation within 7 days
        bool_or(
            s.session_date between u.signup_date and u.signup_date + 7
        ) as has_planning_initiation_7d,

        -- F2: Activation (save, share, or prompt) within 7 days
        bool_or(
            (s.has_save or s.has_share or s.is_prompt_session)
            and s.session_date between u.signup_date and u.signup_date + 7
        ) as has_activation_7d,

        -- F2b: Activation within 30 days
        bool_or(
            (s.has_save or s.has_share or s.is_prompt_session)
            and s.session_date between u.signup_date and u.signup_date + 30
        ) as has_activation_30d,

        -- Prompt within 7 days
        bool_or(
            s.is_prompt_session
            and s.session_date between u.signup_date and u.signup_date + 7
        ) as has_prompt_7d,

        -- F3: First share within 7 days
        bool_or(
            s.has_share
            and s.session_date between u.signup_date and u.signup_date + 7
        ) as has_first_share_7d,

        -- F4: First validation within 7 days
        bool_or(
            s.has_post_share_interaction
            and s.session_date between u.signup_date and u.signup_date + 7
        ) as has_first_validation_7d,

        -- Timing
        min(s.session_date) filter (where s.has_save or s.has_share or s.is_prompt_session) as first_activation_date,
        min(s.session_date) filter (where s.has_share) as first_share_date,

        -- Activation type
        bool_or(s.has_save and s.session_date between u.signup_date and u.signup_date + 7) as had_save_7d,
        bool_or(s.has_share and s.session_date between u.signup_date and u.signup_date + 7) as had_share_7d,
        bool_or(s.is_prompt_session and s.session_date between u.signup_date and u.signup_date + 7) as had_prompt_7d

    from users u
    left join user_sessions s on u.user_id = s.user_id
    group by u.user_id, u.signup_date, u.signup_week
)

select
    user_id,
    signup_date,
    signup_week,
    coalesce(has_planning_initiation_7d, false) as has_planning_initiation_7d,
    coalesce(has_activation_7d, false) as has_activation_7d,
    coalesce(has_activation_30d, false) as has_activation_30d,
    coalesce(has_prompt_7d, false) as has_prompt_7d,
    coalesce(has_first_share_7d, false) as has_first_share_7d,
    coalesce(has_first_validation_7d, false) as has_first_validation_7d,

    first_activation_date,

    case
        when first_activation_date is not null
            then (first_activation_date - signup_date)
        else null
    end as days_to_activation,
    case
        when first_share_date is not null
            then (first_share_date - signup_date)
        else null
    end as days_to_first_share,

    case
        when coalesce(had_save_7d, false) and coalesce(had_share_7d, false) and coalesce(had_prompt_7d, false) then 'prompt_save_share'
        when coalesce(had_save_7d, false) and coalesce(had_share_7d, false) then 'save_and_share'
        when coalesce(had_prompt_7d, false) and coalesce(had_save_7d, false) then 'prompt_and_save'
        when coalesce(had_prompt_7d, false) and coalesce(had_share_7d, false) then 'prompt_and_share'
        when coalesce(had_share_7d, false) then 'share_only'
        when coalesce(had_save_7d, false) then 'save_only'
        when coalesce(had_prompt_7d, false) then 'prompt_only'
        else null
    end as activation_type

from user_activity
