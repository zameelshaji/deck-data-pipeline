{{ config(materialized='table') }}

-- Gold model: Daily aggregates for onboarding funnel analytics
-- Provides step counts, conversion rates, permission rates, and feature selection distribution

with daily_funnel as (
    select
        onboarding_date,

        -- Step counts
        count(*) as users_started,
        count(*) filter (where reached_welcome) as reached_welcome,
        count(*) filter (where reached_referral) as reached_referral,
        count(*) filter (where reached_location) as reached_location,
        count(*) filter (where reached_notification) as reached_notification,
        count(*) filter (where reached_contacts) as reached_contacts,
        count(*) filter (where reached_feature_router) as reached_feature_router,
        count(*) filter (where reached_completion) as reached_completion,
        count(*) filter (where completed_onboarding) as completed,

        -- Permission grant counts
        count(*) filter (where location_granted) as location_granted_count,
        count(*) filter (where notification_granted) as notification_granted_count,
        count(*) filter (where contacts_granted) as contacts_granted_count,

        -- Referral submission count
        count(*) filter (where referral_submitted) as referral_submitted_count,

        -- Time to complete statistics
        percentile_cont(0.5) within group (order by time_to_complete_seconds)
            filter (where completed_onboarding and time_to_complete_seconds is not null) as median_time_to_complete_seconds,

        -- Feature selection counts
        count(*) filter (where feature_selected is not null) as feature_selected_count,
        count(*) filter (where feature_selected = 'explore') as feature_explore_count,
        count(*) filter (where feature_selected = 'plan') as feature_plan_count,
        count(*) filter (where feature_selected = 'discover') as feature_discover_count,
        count(*) filter (where feature_selected = 'save') as feature_save_count,
        count(*) filter (where feature_selected = 'share') as feature_share_count

    from {{ ref('fct_onboarding_funnel') }}
    where onboarding_date is not null
    group by onboarding_date
),

with_rates as (
    select
        onboarding_date,
        users_started,
        reached_welcome,
        reached_referral,
        reached_location,
        reached_notification,
        reached_contacts,
        reached_feature_router,
        reached_completion,
        completed,

        -- Conversion rates (as percentages)
        case when users_started > 0
            then round(100.0 * completed / users_started, 2)
            else 0 end as completion_rate,

        case when reached_welcome > 0
            then round(100.0 * reached_referral / reached_welcome, 2)
            else 0 end as welcome_to_referral_rate,

        case when reached_referral > 0
            then round(100.0 * reached_location / reached_referral, 2)
            else 0 end as referral_to_location_rate,

        case when reached_location > 0
            then round(100.0 * reached_notification / reached_location, 2)
            else 0 end as location_to_notification_rate,

        case when reached_notification > 0
            then round(100.0 * reached_contacts / reached_notification, 2)
            else 0 end as notification_to_contacts_rate,

        case when reached_contacts > 0
            then round(100.0 * reached_feature_router / reached_contacts, 2)
            else 0 end as contacts_to_feature_router_rate,

        case when reached_feature_router > 0
            then round(100.0 * completed / reached_feature_router, 2)
            else 0 end as feature_router_to_completion_rate,

        -- Permission grant rates (as percentages, based on users who reached each step)
        case when reached_location > 0
            then round(100.0 * location_granted_count / reached_location, 2)
            else 0 end as location_grant_rate,

        case when reached_notification > 0
            then round(100.0 * notification_granted_count / reached_notification, 2)
            else 0 end as notification_grant_rate,

        case when reached_contacts > 0
            then round(100.0 * contacts_granted_count / reached_contacts, 2)
            else 0 end as contacts_grant_rate,

        -- Referral submission rate
        case when reached_referral > 0
            then round(100.0 * referral_submitted_count / reached_referral, 2)
            else 0 end as referral_submission_rate,

        -- Time metrics
        median_time_to_complete_seconds,

        -- Feature selection counts
        feature_selected_count,
        feature_explore_count,
        feature_plan_count,
        feature_discover_count,
        feature_save_count,
        feature_share_count

    from daily_funnel
),

with_rolling_avg as (
    select
        *,
        -- Rolling 7-day average for completion rate
        avg(completion_rate) over (
            order by onboarding_date
            rows between 6 preceding and current row
        ) as completion_rate_7d_avg
    from with_rates
)

select * from with_rolling_avg
order by onboarding_date desc
