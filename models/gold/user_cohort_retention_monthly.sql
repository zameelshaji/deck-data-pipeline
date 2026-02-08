-- Monthly user cohort retention analysis
-- Tracks how many users from each activation cohort return in subsequent months
-- Activated = ≥1 prompt OR ≥1 save OR ≥1 share

with user_cohorts as (
    select
        user_id,
        date_trunc('month', activation_date)::date as cohort_month,
        activation_date
    from {{ ref('fct_user_activation') }}
    where is_activated = true
      and activation_date is not null
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
        extract(month from age(uam.activity_month, uc.cohort_month)) as months_since_activation
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
        ca.months_since_activation,
        count(distinct ca.user_id) as users_active
    from cohort_activity ca
    where ca.months_since_activation > 0
    group by ca.cohort_month, ca.months_since_activation
),

cohort_retention_rates as (
    select
        crc.cohort_month,
        cs.cohort_size,
        crc.months_since_activation,
        crc.users_active,
        round(100.0 * crc.users_active / cs.cohort_size, 1) as retention_rate,

        case
            when crc.months_since_activation = 1 then 'Month 1'
            when crc.months_since_activation = 2 then 'Month 2'
            when crc.months_since_activation = 3 then 'Month 3'
            when crc.months_since_activation = 6 then 'Month 6'
            when crc.months_since_activation = 12 then 'Month 12'
            else 'Month ' || crc.months_since_activation::text
        end as month_label

    from cohort_retention_counts crc
    inner join cohort_size cs
        on crc.cohort_month = cs.cohort_month
)

select
    cohort_month,
    cohort_size,
    months_since_activation,
    month_label,
    users_active,
    retention_rate,

    date_trunc('quarter', cohort_month)::date as cohort_quarter,
    extract(year from cohort_month) as cohort_year

from cohort_retention_rates
order by cohort_month desc, months_since_activation asc
