-- Weekly cohort retention summary
-- Aggregates activated users by cohort_week (Monday of activation week)
-- Tracks D7/D30/D60/D90 retention with maturity flags
-- Retention = had another session with ≥1 prompt OR ≥1 save OR ≥1 share after activation_date

with activated_users as (
    select
        user_id,
        activation_date,
        cohort_week,
        activation_type
    from {{ ref('fct_user_activation') }}
    where is_activated = true
      and activation_date is not null
),

-- Retention = had another session with prompt, save, or share after activation_date
user_activity_dates as (
    select distinct
        user_id,
        session_date
    from {{ ref('fct_session_outcomes') }}
    where has_save or has_share or is_prompt_session
),

-- Calculate retention flags for each user
user_retention as (
    select
        a.user_id,
        a.activation_date,
        a.cohort_week,
        a.activation_type,

        -- Maturity flags (has enough time passed to measure retention?)
        current_date >= a.activation_date + 7 as is_mature_d7,
        current_date >= a.activation_date + 30 as is_mature_d30,
        current_date >= a.activation_date + 60 as is_mature_d60,
        current_date >= a.activation_date + 90 as is_mature_d90,

        -- Retention flags (had activity in retention window?)
        -- Note: Activity must be AFTER activation_date (days 1-X, not day 0)
        bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 7
        ) as had_activity_d7,
        bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 30
        ) as had_activity_d30,
        bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 60
        ) as had_activity_d60,
        bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 90
        ) as had_activity_d90

    from activated_users a
    left join user_activity_dates ua on a.user_id = ua.user_id
    group by a.user_id, a.activation_date, a.cohort_week, a.activation_type
),

-- Aggregate by cohort_week
cohort_aggregates as (
    select
        cohort_week,

        -- Cohort size
        count(*) as cohort_size,

        -- Mature counts (users with enough time elapsed)
        count(*) filter (where is_mature_d7) as mature_d7,
        count(*) filter (where is_mature_d30) as mature_d30,
        count(*) filter (where is_mature_d60) as mature_d60,
        count(*) filter (where is_mature_d90) as mature_d90,

        -- Retained counts (mature users who returned)
        count(*) filter (where is_mature_d7 and coalesce(had_activity_d7, false)) as retained_d7,
        count(*) filter (where is_mature_d30 and coalesce(had_activity_d30, false)) as retained_d30,
        count(*) filter (where is_mature_d60 and coalesce(had_activity_d60, false)) as retained_d60,
        count(*) filter (where is_mature_d90 and coalesce(had_activity_d90, false)) as retained_d90,

        -- Breakdown by activation type for drill-down
        count(*) filter (where activation_type = 'save_prompted') as cohort_save_prompted,
        count(*) filter (where activation_type = 'saved') as cohort_saved,
        count(*) filter (where activation_type = 'shared') as cohort_shared,
        count(*) filter (where activation_type = 'multiple') as cohort_multiple

    from user_retention
    group by cohort_week
)

select
    cohort_week,
    cohort_size,

    -- Mature counts
    mature_d7,
    mature_d30,
    mature_d60,
    mature_d90,

    -- Retained counts
    retained_d7,
    retained_d30,
    retained_d60,
    retained_d90,

    -- Retention rates (as decimal 0-1)
    -- Only calculate if there are mature users to avoid division by zero
    case when mature_d7 > 0 then retained_d7::numeric / mature_d7 else null end as retention_rate_d7,
    case when mature_d30 > 0 then retained_d30::numeric / mature_d30 else null end as retention_rate_d30,
    case when mature_d60 > 0 then retained_d60::numeric / mature_d60 else null end as retention_rate_d60,
    case when mature_d90 > 0 then retained_d90::numeric / mature_d90 else null end as retention_rate_d90,

    -- Activation type breakdown
    cohort_save_prompted,
    cohort_saved,
    cohort_shared,
    cohort_multiple

from cohort_aggregates
order by cohort_week desc
