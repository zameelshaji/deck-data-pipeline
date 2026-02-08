-- User acquisition and onboarding funnel analysis
with user_journey as (
    select
        u.user_id,
        u.created_at as signup_date,
        u.onboarding_completed,
        u.preferences_completed,

        min(case when e.event_category = 'AI' then e.event_timestamp end) as first_ai_query,
        min(case when e.event_category in ('Swipe', 'Save') then e.event_timestamp end) as first_curation,
        min(case when e.event_category = 'Conversion' then e.event_timestamp end) as first_conversion,

        count(case
            when e.event_timestamp between u.created_at and u.created_at + interval '7 days'
            then 1
        end) as events_first_7_days,

        count(case
            when e.event_timestamp between u.created_at and u.created_at + interval '1 day'
            then 1
        end) as events_first_day,

        count(distinct case
            when e.event_timestamp between u.created_at and u.created_at + interval '7 days'
            then date(e.event_timestamp)
        end) as active_days_first_week

    from {{ ref('stg_users') }} u
    left join {{ ref('stg_unified_events') }} e on u.user_id = e.user_id
    group by u.user_id, u.created_at, u.onboarding_completed, u.preferences_completed
),

cohort_summary as (
    select
        date(signup_date) as signup_date,
        count(*) as signups,

        count(case when onboarding_completed then 1 end) as completed_onboarding,
        count(case when preferences_completed then 1 end) as completed_preferences,

        count(case when first_ai_query is not null then 1 end) as used_ai,
        count(case when first_curation is not null then 1 end) as performed_curation,
        count(case when first_conversion is not null then 1 end) as performed_conversion,

        count(case when events_first_7_days >= 5 then 1 end) as active_first_week,
        count(case when events_first_day >= 1 then 1 end) as active_first_day,
        count(case when active_days_first_week >= 3 then 1 end) as multi_day_users,

        avg(extract(epoch from (first_ai_query - signup_date))/3600) as avg_hours_to_first_ai,
        avg(extract(epoch from (first_curation - signup_date))/3600) as avg_hours_to_first_curation,

        avg(events_first_7_days) as avg_events_first_week,
        percentile_cont(0.5) within group (order by events_first_7_days) as median_events_first_week

    from user_journey
    group by date(signup_date)
)

select
    *,

    case when signups > 0 then round(100.0 * completed_onboarding / signups, 2) else 0 end as onboarding_completion_rate,
    case when signups > 0 then round(100.0 * completed_preferences / signups, 2) else 0 end as preferences_completion_rate,
    case when signups > 0 then round(100.0 * used_ai / signups, 2) else 0 end as ai_adoption_rate,
    case when signups > 0 then round(100.0 * performed_curation / signups, 2) else 0 end as curation_rate,
    case when signups > 0 then round(100.0 * performed_conversion / signups, 2) else 0 end as conversion_rate,
    case when signups > 0 then round(100.0 * active_first_week / signups, 2) else 0 end as week_1_activation_rate,
    case when signups > 0 then round(100.0 * multi_day_users / signups, 2) else 0 end as multi_day_retention_rate,

    sum(signups) over (
        order by signup_date
        rows between 6 preceding and current row
    ) as signups_7day_total,

    sum(completed_onboarding) over (
        order by signup_date
        rows between 6 preceding and current row
    ) as onboarding_7day_total

from cohort_summary
order by signup_date desc
