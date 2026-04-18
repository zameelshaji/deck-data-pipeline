-- Retention split by group-membership count at acquisition.
-- One row per (cohort_week, connectivity_bucket).
-- Connectivity = count of distinct groups the user joined on or before
-- (activation_date + 7 days), bucketed into 0 / 1-2 / 3+.
-- Retention windows D7/D30/D60/D90 computed like fct_retention_by_cohort_week.

with activated_users as (
    select
        user_id,
        activation_date,
        cohort_week
    from {{ ref('fct_user_activation') }}
    where is_activated = true
      and activation_date is not null
),

user_group_count as (
    select
        a.user_id,
        count(distinct gm.group_id) as groups_joined_near_activation
    from activated_users a
    left join {{ ref('src_group_members') }} gm
        on gm.user_id = a.user_id
       and gm.joined_at is not null
       and gm.joined_at::date <= a.activation_date + 7
    group by a.user_id
),

user_bucket as (
    select
        a.user_id,
        a.activation_date,
        a.cohort_week,
        case
            when coalesce(g.groups_joined_near_activation, 0) = 0 then '0_groups'
            when g.groups_joined_near_activation between 1 and 2 then '1-2_groups'
            else '3+_groups'
        end as connectivity_bucket
    from activated_users a
    left join user_group_count g on a.user_id = g.user_id
),

user_activity_dates as (
    select distinct
        user_id,
        session_date
    from {{ ref('fct_session_outcomes') }}
    where has_save or has_share or is_prompt_session
),

user_retention as (
    select
        u.user_id,
        u.activation_date,
        u.cohort_week,
        u.connectivity_bucket,

        current_date >= u.activation_date + 7  as is_mature_d7,
        current_date >= u.activation_date + 30 as is_mature_d30,
        current_date >= u.activation_date + 60 as is_mature_d60,
        current_date >= u.activation_date + 90 as is_mature_d90,

        coalesce(bool_or(
            ua.session_date between u.activation_date + 1 and u.activation_date + 7
        ), false) as had_activity_d7,
        coalesce(bool_or(
            ua.session_date between u.activation_date + 1 and u.activation_date + 30
        ), false) as had_activity_d30,
        coalesce(bool_or(
            ua.session_date between u.activation_date + 1 and u.activation_date + 60
        ), false) as had_activity_d60,
        coalesce(bool_or(
            ua.session_date between u.activation_date + 1 and u.activation_date + 90
        ), false) as had_activity_d90

    from user_bucket u
    left join user_activity_dates ua on u.user_id = ua.user_id
    group by u.user_id, u.activation_date, u.cohort_week, u.connectivity_bucket
),

aggregated as (
    select
        cohort_week,
        connectivity_bucket,

        count(*) as cohort_size,

        count(*) filter (where is_mature_d7)  as mature_d7,
        count(*) filter (where is_mature_d30) as mature_d30,
        count(*) filter (where is_mature_d60) as mature_d60,
        count(*) filter (where is_mature_d90) as mature_d90,

        count(*) filter (where is_mature_d7  and had_activity_d7)  as retained_d7,
        count(*) filter (where is_mature_d30 and had_activity_d30) as retained_d30,
        count(*) filter (where is_mature_d60 and had_activity_d60) as retained_d60,
        count(*) filter (where is_mature_d90 and had_activity_d90) as retained_d90

    from user_retention
    group by cohort_week, connectivity_bucket
)

select
    cohort_week,
    connectivity_bucket,
    cohort_size,
    mature_d7,
    mature_d30,
    mature_d60,
    mature_d90,
    retained_d7,
    retained_d30,
    retained_d60,
    retained_d90,
    case when mature_d7  > 0 then retained_d7::numeric  / mature_d7  else null end as retention_rate_d7,
    case when mature_d30 > 0 then retained_d30::numeric / mature_d30 else null end as retention_rate_d30,
    case when mature_d60 > 0 then retained_d60::numeric / mature_d60 else null end as retention_rate_d60,
    case when mature_d90 > 0 then retained_d90::numeric / mature_d90 else null end as retention_rate_d90
from aggregated
order by cohort_week desc, connectivity_bucket
