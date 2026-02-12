-- Daily Active Users with breakdown by engagement type
with daily_user_activity as (
    select
        date(event_timestamp) as activity_date,
        user_id,
        source_table,
        event_category,
        count(*) as daily_events
    from {{ ref('stg_unified_events') }}
    group by 1, 2, 3, 4
),

daily_metrics as (
    select
        activity_date,
        count(distinct user_id) as daily_active_users,
        count(distinct case when source_table = 'dextr_queries' then user_id end) as ai_active_users,
        count(distinct case when event_category in ('Swipe', 'Save') then user_id end) as curation_active_users,
        count(distinct case when event_category = 'Conversion' then user_id end) as conversion_active_users,
        count(distinct case when source_table = 'featured_section_actions' then user_id end) as multiplayer_active_users,

        -- Engagement intensity
        avg(daily_events) as avg_events_per_user,
        percentile_cont(0.5) within group (order by daily_events) as median_events_per_user,
        max(daily_events) as max_events_per_user
    from daily_user_activity
    group by activity_date
),

rolling_metrics as (
    select
        *,
        avg(daily_active_users) over (
            order by activity_date
            rows between 6 preceding and current row
        ) as dau_7day_avg,

        avg(ai_active_users) over (
            order by activity_date
            rows between 6 preceding and current row
        ) as ai_users_7day_avg,

        lag(daily_active_users, 7) over (order by activity_date) as dau_week_ago,
        lag(daily_active_users, 30) over (order by activity_date) as dau_month_ago
    from daily_metrics
)

select
    *,
    case
        when dau_week_ago > 0
        then round(100.0 * (daily_active_users - dau_week_ago) / dau_week_ago, 2)
        else null
    end as dau_wow_growth_percent,

    case
        when dau_month_ago > 0
        then round(100.0 * (daily_active_users - dau_month_ago) / dau_month_ago, 2)
        else null
    end as dau_mom_growth_percent,

    case
        when daily_active_users > 0
        then round(100.0 * ai_active_users / daily_active_users, 2)
        else 0
    end as ai_adoption_rate,

    case
        when daily_active_users > 0
        then round(100.0 * conversion_active_users / daily_active_users, 2)
        else 0
    end as conversion_user_rate

from rolling_metrics
order by activity_date desc
