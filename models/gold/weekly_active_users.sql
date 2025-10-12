-- Weekly Active Users (WAU) with engagement breakdown
with weekly_user_activity as (
    select
        date_trunc('week', event_timestamp)::date as activity_week,
        user_id,
        event_source,
        event_category,
        count(*) as weekly_events
    from {{ ref('stg_events') }}
    group by 1, 2, 3, 4
),

weekly_metrics as (
    select
        activity_week,
        count(distinct user_id) as weekly_active_users,
        count(distinct case when event_source = 'dextr' then user_id end) as wau_ai_users,
        count(distinct case when event_category = 'Content Curation' then user_id end) as wau_curation_users,
        count(distinct case when event_category = 'Conversion Action' then user_id end) as wau_conversion_users,
        count(distinct case when event_source = 'multiplayer' then user_id end) as wau_multiplayer_users,
        count(distinct case when event_source = 'featured_section' then user_id end) as wau_featured_users,

        -- Engagement intensity
        avg(weekly_events) as avg_events_per_user,
        percentile_cont(0.5) within group (order by weekly_events) as median_events_per_user,
        max(weekly_events) as max_events_per_user
    from weekly_user_activity
    group by activity_week
),

weekly_with_growth as (
    select
        *,
        -- Week-over-week growth
        lag(weekly_active_users, 1) over (order by activity_week) as wau_last_week,
        lag(weekly_active_users, 4) over (order by activity_week) as wau_4_weeks_ago,

        -- 4-week rolling average
        avg(weekly_active_users) over (
            order by activity_week
            rows between 3 preceding and current row
        ) as wau_4week_avg
    from weekly_metrics
)

select
    activity_week,
    weekly_active_users,
    wau_ai_users as ai_active_users,
    wau_curation_users as curation_active_users,
    wau_conversion_users as conversion_active_users,
    wau_multiplayer_users as multiplayer_active_users,
    wau_featured_users as featured_active_users,
    avg_events_per_user,
    median_events_per_user,
    max_events_per_user,
    wau_4week_avg as rolling_4week_avg,

    -- Growth calculations
    case
        when wau_last_week > 0
        then round(100.0 * (weekly_active_users - wau_last_week) / wau_last_week, 2)
        else null
    end as wow_growth_percent,

    case
        when wau_4_weeks_ago > 0
        then round(100.0 * (weekly_active_users - wau_4_weeks_ago) / wau_4_weeks_ago, 2)
        else null
    end as growth_vs_4_weeks_ago_percent,

    -- Engagement ratios
    case
        when weekly_active_users > 0
        then round(100.0 * wau_ai_users / weekly_active_users, 2)
        else 0
    end as ai_adoption_rate,

    case
        when weekly_active_users > 0
        then round(100.0 * wau_conversion_users / weekly_active_users, 2)
        else 0
    end as conversion_user_rate,

    case
        when weekly_active_users > 0
        then round(100.0 * wau_multiplayer_users / weekly_active_users, 2)
        else 0
    end as multiplayer_adoption_rate,

    case
        when weekly_active_users > 0
        then round(100.0 * wau_featured_users / weekly_active_users, 2)
        else 0
    end as featured_adoption_rate

from weekly_with_growth
order by activity_week desc
