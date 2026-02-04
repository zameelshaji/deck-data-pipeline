-- User-level activation model
-- Tracks when each user first activated (saved, shared, or was prompted to save)
-- Cohort_week is the Monday of the activation week for weekly cohort analysis

with users as (
    select
        user_id,
        date(created_at) as signup_date,
        date_trunc('week', created_at)::date as signup_week
    from {{ ref('stg_users') }}
    where is_test_user = 0
      and created_at is not null
),

-- Get all activation-qualifying sessions from fct_session_outcomes
-- Activation events: save, share, or prompt (which leads to save recommendation)
user_activation_events as (
    select
        s.user_id,
        s.session_date as event_date,
        s.has_save,
        s.has_share,
        s.is_prompt_session,
        -- Flag for save_prompted: prompt session that resulted in a save
        (s.is_prompt_session and s.has_save) as is_save_prompted
    from {{ ref('fct_session_outcomes') }} s
    where s.has_save or s.has_share or s.is_prompt_session
),

-- Find the first activation event for each user
-- Consider: save_prompted (prompt + save), saved (save without prompt), shared
first_activation as (
    select
        u.user_id,
        u.signup_date,
        u.signup_week,

        -- First date of any activation activity
        min(e.event_date) as activation_date,

        -- First date of each specific type
        min(e.event_date) filter (where e.has_save and not e.is_prompt_session) as first_save_date,
        min(e.event_date) filter (where e.has_share) as first_share_date,
        min(e.event_date) filter (where e.is_save_prompted) as first_save_prompted_date,

        -- Flags for what happened in first 7 days
        bool_or(e.has_save and e.event_date between u.signup_date and u.signup_date + 7) as had_save_7d,
        bool_or(e.has_share and e.event_date between u.signup_date and u.signup_date + 7) as had_share_7d,
        bool_or(e.is_save_prompted and e.event_date between u.signup_date and u.signup_date + 7) as had_save_prompted_7d

    from users u
    left join user_activation_events e on u.user_id = e.user_id
    group by u.user_id, u.signup_date, u.signup_week
),

-- Determine activation details
activation_details as (
    select
        user_id,
        signup_date,
        signup_week,
        activation_date,

        -- Cohort week is Monday of the activation week
        date_trunc('week', activation_date)::date as cohort_week,

        -- Days to activation
        case
            when activation_date is not null
                then (activation_date - signup_date)
            else null
        end as days_to_activation,

        -- Determine first activation event type
        -- Priority: save_prompted > saved > shared (based on value to the user)
        case
            when first_save_prompted_date is not null
                 and first_save_prompted_date <= coalesce(first_save_date, 'infinity'::date)
                 and first_save_prompted_date <= coalesce(first_share_date, 'infinity'::date)
                then 'save_prompted'
            when first_save_date is not null
                 and first_save_date <= coalesce(first_share_date, 'infinity'::date)
                then 'saved'
            when first_share_date is not null
                then 'shared'
            else null
        end as first_activation_event,

        -- Count of activation types within 7 days
        (case when coalesce(had_save_7d, false) then 1 else 0 end +
         case when coalesce(had_share_7d, false) then 1 else 0 end +
         case when coalesce(had_save_prompted_7d, false) then 1 else 0 end) as activation_types_count,

        had_save_7d,
        had_share_7d,
        had_save_prompted_7d,

        -- Is user activated?
        activation_date is not null as is_activated

    from first_activation
)

select
    user_id,
    signup_date,
    activation_date,
    cohort_week,
    days_to_activation,

    -- Activation type: categorize based on what types of activation occurred
    case
        when not is_activated then null
        when activation_types_count > 1 then 'multiple'
        else first_activation_event
    end as activation_type,

    first_activation_event,
    is_activated

from activation_details
