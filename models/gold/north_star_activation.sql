-- User activation and retention metrics based on North Star behaviors
-- Activation = first session with at least 1 save

with user_signups as (
    select
        user_id,
        created_at as signup_timestamp,
        date(created_at) as signup_date,
        date_trunc('week', created_at)::date as signup_week
    from {{ ref('stg_users') }}
    where is_test_user = 0
),

user_sessions as (
    select
        user_id,
        derived_session_id,
        session_start,
        session_week,
        has_save,
        is_scr3,
        unique_saves,
        has_multiplayer_activity,
        has_conversion
    from {{ ref('north_star_session_metrics') }}
),

user_first_session as (
    select
        user_id,
        min(session_start) as first_session_at,
        date(min(session_start)) as first_session_date
    from user_sessions
    group by user_id
),

user_first_save_session as (
    select
        user_id,
        min(session_start) as first_save_session_at,
        date(min(session_start)) as first_save_date
    from user_sessions
    where has_save = true
    group by user_id
),

user_activation as (
    select
        us.user_id,
        us.signup_date,
        us.signup_week,
        ufs.first_session_date,
        ufss.first_save_date,

        -- Time to first session (days)
        ufs.first_session_date - us.signup_date as days_to_first_session,

        -- Time to first save (days from signup)
        ufss.first_save_date - us.signup_date as days_to_first_save,

        -- Activation within time windows
        case when ufss.first_save_date - us.signup_date <= 7 then 1 else 0 end as activated_7d,
        case when ufss.first_save_date - us.signup_date <= 30 then 1 else 0 end as activated_30d,

        -- First week activity metrics
        (
            select count(distinct usess.derived_session_id)
            from user_sessions usess
            where usess.user_id = us.user_id
              and usess.session_start between us.signup_timestamp and us.signup_timestamp + interval '7 days'
        ) as sessions_week1,

        (
            select sum(usess.unique_saves)
            from user_sessions usess
            where usess.user_id = us.user_id
              and usess.session_start between us.signup_timestamp and us.signup_timestamp + interval '7 days'
        ) as saves_week1,

        -- Strong activation (SCR3 in first week)
        (
            select max(case when usess.is_scr3 then 1 else 0 end)
            from user_sessions usess
            where usess.user_id = us.user_id
              and usess.session_start between us.signup_timestamp and us.signup_timestamp + interval '7 days'
        ) as scr3_week1,

        -- Retention check (any activity after first week)
        (
            select max(1)
            from user_sessions usess
            where usess.user_id = us.user_id
              and usess.session_start > us.signup_timestamp + interval '7 days'
        ) as retained_after_week1,

        -- Activity in week 2-4 (day 8-30)
        (
            select max(1)
            from user_sessions usess
            where usess.user_id = us.user_id
              and usess.session_start between us.signup_timestamp + interval '7 days'
                                         and us.signup_timestamp + interval '30 days'
        ) as active_week2_4

    from user_signups us
    left join user_first_session ufs on us.user_id = ufs.user_id
    left join user_first_save_session ufss on us.user_id = ufss.user_id
),

cohort_metrics as (
    select
        signup_week as cohort_week,
        count(distinct user_id) as cohort_size,

        -- Activation rates
        sum(activated_7d) as activated_7d,
        sum(activated_30d) as activated_30d,
        round(100.0 * sum(activated_7d) / nullif(count(*), 0), 2) as activation_rate_7d,
        round(100.0 * sum(activated_30d) / nullif(count(*), 0), 2) as activation_rate_30d,

        -- Time to activation (among activated users)
        avg(case when activated_7d = 1 then days_to_first_save end) as avg_days_to_activation_7d,
        avg(case when activated_30d = 1 then days_to_first_save end) as avg_days_to_activation_30d,
        percentile_cont(0.5) within group (order by days_to_first_save)
            filter (where activated_30d = 1) as median_days_to_activation,

        -- Strong activation (SCR3 first week)
        sum(coalesce(scr3_week1, 0)) as activated_with_scr3,
        round(100.0 * sum(coalesce(scr3_week1, 0)) / nullif(count(*), 0), 2) as strong_activation_rate,

        -- First week engagement
        avg(coalesce(sessions_week1, 0)) as avg_sessions_week1,
        avg(coalesce(saves_week1, 0)) as avg_saves_week1,

        -- Retention (among all users)
        sum(coalesce(retained_after_week1, 0)) as retained_d7,
        round(100.0 * sum(coalesce(retained_after_week1, 0)) / nullif(count(*), 0), 2) as retention_d7,

        -- Retention among activated users
        sum(case when activated_7d = 1 then coalesce(retained_after_week1, 0) else 0 end) as activated_retained_d7,
        round(100.0 *
            sum(case when activated_7d = 1 then coalesce(retained_after_week1, 0) else 0 end) /
            nullif(sum(activated_7d), 0), 2
        ) as activated_retention_d7,

        -- D30 activity
        sum(coalesce(active_week2_4, 0)) as active_d30,
        round(100.0 * sum(coalesce(active_week2_4, 0)) / nullif(count(*), 0), 2) as retention_d30

    from user_activation
    group by signup_week
)

select
    cohort_week,
    cohort_size,

    -- Activation
    activated_7d,
    activated_30d,
    activation_rate_7d,
    activation_rate_30d,

    -- Time to activation
    round(avg_days_to_activation_7d, 1) as avg_days_to_activation_7d,
    round(avg_days_to_activation_30d, 1) as avg_days_to_activation_30d,
    round(median_days_to_activation, 1) as median_days_to_activation,

    -- Strong activation
    activated_with_scr3,
    strong_activation_rate,

    -- First week engagement
    round(avg_sessions_week1, 2) as avg_sessions_week1,
    round(avg_saves_week1, 2) as avg_saves_week1,

    -- Retention
    retained_d7,
    retention_d7,
    activated_retained_d7,
    activated_retention_d7,
    active_d30,
    retention_d30,

    -- WoW changes
    lag(activation_rate_7d) over (order by cohort_week) as prev_week_activation_rate,
    activation_rate_7d - lag(activation_rate_7d) over (order by cohort_week) as activation_rate_change

from cohort_metrics
order by cohort_week desc
