-- Monthly user cohort retention analysis
-- Tracks how many users from each signup cohort return in subsequent months

with user_cohorts as (
    select
        user_id,
        date_trunc('month', created_at)::date as cohort_month,
        created_at
    from {{ ref('stg_users') }}
    where created_at is not null
),

user_activity_months as (
    select distinct
        e.user_id,
        date_trunc('month', e.event_timestamp)::date as activity_month
    from {{ ref('stg_unified_events') }} e
    where e.user_id is not null
),

cohort_activity as (
    select
        uc.cohort_month,
        uc.user_id,
        uam.activity_month,
        extract(year from age(uam.activity_month, uc.cohort_month)) * 12 +
        extract(month from age(uam.activity_month, uc.cohort_month)) as months_since_signup
    from user_cohorts uc
    left join user_activity_months uam
        on uc.user_id = uam.user_id
),

cohort_size as (
    select
        cohort_month,
        count(distinct user_id) as cohort_size
    from user_cohorts
    group by cohort_month
),

cohort_retention_counts as (
    select
        ca.cohort_month,
        ca.months_since_signup,
        count(distinct ca.user_id) as users_active
    from cohort_activity ca
    where ca.months_since_signup > 0
    group by ca.cohort_month, ca.months_since_signup
),

cohort_retention_rates as (
    select
        crc.cohort_month,
        cs.cohort_size,
        crc.months_since_signup,
        crc.users_active,
        round(100.0 * crc.users_active / cs.cohort_size, 1) as retention_rate,

        case
            when crc.months_since_signup = 1 then 'Month 1'
            when crc.months_since_signup = 2 then 'Month 2'
            when crc.months_since_signup = 3 then 'Month 3'
            when crc.months_since_signup = 6 then 'Month 6'
            when crc.months_since_signup = 12 then 'Month 12'
            else 'Month ' || crc.months_since_signup::text
        end as month_label

    from cohort_retention_counts crc
    inner join cohort_size cs
        on crc.cohort_month = cs.cohort_month
)

select
    cohort_month,
    cohort_size,
    months_since_signup,
    month_label,
    users_active,
    retention_rate,

    date_trunc('quarter', cohort_month)::date as cohort_quarter,
    extract(year from cohort_month) as cohort_year

from cohort_retention_rates
order by cohort_month desc, months_since_signup asc
