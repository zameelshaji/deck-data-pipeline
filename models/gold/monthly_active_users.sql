-- Monthly Active Users (MAU) with engagement breakdown
with monthly_user_activity as (
    select
        date_trunc('month', event_timestamp)::date as activity_month,
        user_id,
        event_source,
        event_category,
        count(*) as monthly_events
    from {{ ref('stg_events') }}
    group by 1, 2, 3, 4
),

monthly_metrics as (
    select
        activity_month,
        count(distinct user_id) as monthly_active_users,
        count(distinct case when event_source = 'dextr' then user_id end) as mau_ai_users,
        count(distinct case when event_category = 'Content Curation' then user_id end) as mau_curation_users,
        count(distinct case when event_category = 'Conversion Action' then user_id end) as mau_conversion_users,
        count(distinct case when event_source = 'multiplayer' then user_id end) as mau_multiplayer_users,
        count(distinct case when event_source = 'featured_section' then user_id end) as mau_featured_users,

        -- Engagement intensity
        avg(monthly_events) as avg_events_per_user,
        percentile_cont(0.5) within group (order by monthly_events) as median_events_per_user,
        max(monthly_events) as max_events_per_user
    from monthly_user_activity
    group by activity_month
),

monthly_with_growth as (
    select
        *,
        -- Month-over-month growth
        lag(monthly_active_users, 1) over (order by activity_month) as mau_last_month,
        lag(monthly_active_users, 3) over (order by activity_month) as mau_3_months_ago,

        -- 3-month rolling average
        avg(monthly_active_users) over (
            order by activity_month
            rows between 2 preceding and current row
        ) as mau_3month_avg
    from monthly_metrics
)

select
    activity_month,
    monthly_active_users,
    mau_ai_users as ai_active_users,
    mau_curation_users as curation_active_users,
    mau_conversion_users as conversion_active_users,
    mau_multiplayer_users as multiplayer_active_users,
    mau_featured_users as featured_active_users,
    avg_events_per_user,
    median_events_per_user,
    max_events_per_user,
    mau_3month_avg as rolling_3month_avg,

    -- Growth calculations
    case
        when mau_last_month > 0
        then round(100.0 * (monthly_active_users - mau_last_month) / mau_last_month, 2)
        else null
    end as mom_growth_percent,

    case
        when mau_3_months_ago > 0
        then round(100.0 * (monthly_active_users - mau_3_months_ago) / mau_3_months_ago, 2)
        else null
    end as growth_vs_3_months_ago_percent,

    -- Engagement ratios
    case
        when monthly_active_users > 0
        then round(100.0 * mau_ai_users / monthly_active_users, 2)
        else 0
    end as ai_adoption_rate,

    case
        when monthly_active_users > 0
        then round(100.0 * mau_conversion_users / monthly_active_users, 2)
        else 0
    end as conversion_user_rate,

    case
        when monthly_active_users > 0
        then round(100.0 * mau_multiplayer_users / monthly_active_users, 2)
        else 0
    end as multiplayer_adoption_rate,

    case
        when monthly_active_users > 0
        then round(100.0 * mau_featured_users / monthly_active_users, 2)
        else 0
    end as featured_adoption_rate

from monthly_with_growth
order by activity_month desc
