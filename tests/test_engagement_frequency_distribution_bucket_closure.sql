-- Sum of users_in_bucket across the 8 buckets must equal total_users_in_month
-- for every snapshot_month. If this fails, either bucket_zero math is off or
-- the reference-week selection split users across multiple months.

with per_month as (
    select
        snapshot_month,
        sum(users_in_bucket) as bucket_sum,
        max(total_users_in_month) as total
    from {{ ref('fct_engagement_frequency_distribution') }}
    group by snapshot_month
)

select *
from per_month
where bucket_sum <> total
