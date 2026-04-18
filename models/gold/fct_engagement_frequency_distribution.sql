-- Engagement frequency distribution
-- One row per (snapshot_month, days_active_bucket) where bucket ∈ 0..7.
-- For each calendar month, pick a "reference week" (the latest activity_week
-- whose Monday falls in the month) and bucket each user by their
-- days_active_in_week value for that week. Users who are MAU-of-month but
-- had zero active days in the reference week fall into bucket 0, so the
-- 8 buckets sum to total_users_in_month.

with trajectory as (
    select
        user_id,
        activity_week,
        days_active_in_week,
        date_trunc('month', activity_week)::date as snapshot_month,
        is_active_week
    from {{ ref('fct_user_engagement_trajectory') }}
    where activity_week is not null
),

reference_week_per_month as (
    select
        snapshot_month,
        max(activity_week) as reference_week
    from trajectory
    where is_active_week
    group by snapshot_month
),

mau_per_month as (
    select
        snapshot_month,
        count(distinct user_id) filter (where is_active_week) as total_users_in_month
    from trajectory
    group by snapshot_month
    having count(distinct user_id) filter (where is_active_week) > 0
),

user_in_reference_week as (
    select
        t.user_id,
        r.snapshot_month,
        r.reference_week,
        t.days_active_in_week
    from trajectory t
    inner join reference_week_per_month r
        on t.snapshot_month = r.snapshot_month
       and t.activity_week = r.reference_week
    where t.is_active_week
),

bucket_1_to_7 as (
    select
        snapshot_month,
        reference_week,
        case
            when days_active_in_week >= 7 then 7
            when days_active_in_week <= 0 then 1  -- defensive; is_active_week implies >=1
            else days_active_in_week
        end as days_active_bucket,
        count(distinct user_id) as users_in_bucket
    from user_in_reference_week
    group by snapshot_month, reference_week,
        case
            when days_active_in_week >= 7 then 7
            when days_active_in_week <= 0 then 1
            else days_active_in_week
        end
),

users_in_ref as (
    select
        snapshot_month,
        count(distinct user_id) as users_in_reference
    from user_in_reference_week
    group by snapshot_month
),

bucket_zero as (
    select
        m.snapshot_month,
        r.reference_week,
        0 as days_active_bucket,
        greatest(m.total_users_in_month - coalesce(u.users_in_reference, 0), 0)::bigint
            as users_in_bucket
    from mau_per_month m
    left join reference_week_per_month r on m.snapshot_month = r.snapshot_month
    left join users_in_ref u on m.snapshot_month = u.snapshot_month
),

combined as (
    select snapshot_month, reference_week, days_active_bucket, users_in_bucket
    from bucket_1_to_7
    union all
    select snapshot_month, reference_week, days_active_bucket, users_in_bucket
    from bucket_zero
),

buckets_spine as (
    select generate_series(0, 7) as days_active_bucket
)

select
    m.snapshot_month,
    r.reference_week,
    b.days_active_bucket,
    coalesce(c.users_in_bucket, 0)::bigint as users_in_bucket,
    m.total_users_in_month,
    case
        when m.total_users_in_month > 0
        then round(coalesce(c.users_in_bucket, 0)::numeric / m.total_users_in_month, 4)
        else 0
    end as pct_of_users
from mau_per_month m
left join reference_week_per_month r on m.snapshot_month = r.snapshot_month
cross join buckets_spine b
left join combined c
    on m.snapshot_month = c.snapshot_month
   and b.days_active_bucket = c.days_active_bucket
order by m.snapshot_month desc, b.days_active_bucket
