with activated_users as (
    select
        user_id,
        signup_date as activation_date,
        signup_week as activation_week,
        activation_type
    from {{ ref('fct_activation_funnel') }}
    where has_activation_7d = true
),

user_activity_dates as (
    select distinct
        user_id,
        session_date
    from {{ ref('fct_session_outcomes') }}
),

retention as (
    select
        a.user_id,
        a.activation_date,
        a.activation_week,
        a.activation_type,

        -- Retention flags
        bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 7
        ) as had_activity_d7,
        bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 30
        ) as had_activity_d30,
        bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 60
        ) as had_activity_d60,

        -- Maturity flags
        current_date >= a.activation_date + 7 as is_mature_d7,
        current_date >= a.activation_date + 30 as is_mature_d30,
        current_date >= a.activation_date + 60 as is_mature_d60

    from activated_users a
    left join user_activity_dates ua on a.user_id = ua.user_id
    group by a.user_id, a.activation_date, a.activation_week, a.activation_type
)

select
    user_id,
    activation_date,
    activation_week,
    activation_type,
    coalesce(had_activity_d7, false) as had_activity_d7,
    coalesce(had_activity_d30, false) as had_activity_d30,
    coalesce(had_activity_d60, false) as had_activity_d60,
    is_mature_d7,
    is_mature_d30,
    is_mature_d60
from retention
