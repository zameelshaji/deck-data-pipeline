{{ config(materialized='table') }}

-- Gold model: One row per user who started onboarding
-- Tracks progression through each onboarding step with timestamps and outcomes

with user_onboarding_events as (
    select
        user_id,

        -- Welcome step timestamps
        min(event_timestamp) filter (where event_name = 'onboarding_welcome_viewed') as welcome_viewed_at,
        min(event_timestamp) filter (where event_name = 'onboarding_welcome_continue') as welcome_continue_at,

        -- Referral step timestamps
        min(event_timestamp) filter (where event_name = 'onboarding_referral_viewed') as referral_viewed_at,
        min(event_timestamp) filter (where event_name in ('onboarding_referral_submitted', 'onboarding_referral_skipped')) as referral_action_at,

        -- Location step timestamps
        min(event_timestamp) filter (where event_name = 'onboarding_location_viewed') as location_viewed_at,
        min(event_timestamp) filter (where event_name in ('onboarding_location_enabled', 'onboarding_location_skipped')) as location_action_at,

        -- Notification step timestamps
        min(event_timestamp) filter (where event_name = 'onboarding_notification_viewed') as notification_viewed_at,
        min(event_timestamp) filter (where event_name in ('onboarding_notification_enabled', 'onboarding_notification_skipped')) as notification_action_at,

        -- Contacts step timestamps
        min(event_timestamp) filter (where event_name = 'onboarding_contacts_viewed') as contacts_viewed_at,
        min(event_timestamp) filter (where event_name in ('onboarding_contacts_enabled', 'onboarding_contacts_skipped')) as contacts_action_at,

        -- Feature router step timestamps
        min(event_timestamp) filter (where event_name = 'onboarding_feature_router_viewed') as feature_router_viewed_at,
        min(event_timestamp) filter (where event_name in ('onboarding_feature_selected', 'onboarding_feature_skipped')) as feature_action_at,

        -- Completion step timestamps
        min(event_timestamp) filter (where event_name = 'onboarding_completion_viewed') as completion_viewed_at,
        min(event_timestamp) filter (where event_name = 'onboarding_completed') as completed_at,

        -- Permission outcomes (true if enabled, false if skipped)
        bool_or(event_name = 'onboarding_location_enabled' and permission_granted = true) as location_granted,
        bool_or(event_name = 'onboarding_notification_enabled' and permission_granted = true) as notification_granted,
        bool_or(event_name = 'onboarding_contacts_enabled' and permission_granted = true) as contacts_granted,

        -- Referral submitted flag
        bool_or(event_name = 'onboarding_referral_submitted') as referral_submitted,

        -- Feature selected (take the first one if multiple)
        min(feature_selected) filter (where event_name = 'onboarding_feature_selected') as feature_selected

    from {{ ref('stg_onboarding_events') }}
    group by user_id
),

user_signup_dates as (
    select
        user_id,
        date(created_at) as signup_date
    from {{ ref('stg_users') }}
    where is_test_user = 0
)

select
    uoe.user_id,
    usd.signup_date,

    -- Timestamps
    uoe.welcome_viewed_at,
    uoe.welcome_continue_at,
    uoe.referral_viewed_at,
    uoe.referral_action_at,
    uoe.location_viewed_at,
    uoe.location_action_at,
    uoe.notification_viewed_at,
    uoe.notification_action_at,
    uoe.contacts_viewed_at,
    uoe.contacts_action_at,
    uoe.feature_router_viewed_at,
    uoe.feature_action_at,
    uoe.completion_viewed_at,
    uoe.completed_at,

    -- Boolean flags for reaching each step
    uoe.welcome_viewed_at is not null as reached_welcome,
    uoe.referral_viewed_at is not null as reached_referral,
    uoe.location_viewed_at is not null as reached_location,
    uoe.notification_viewed_at is not null as reached_notification,
    uoe.contacts_viewed_at is not null as reached_contacts,
    uoe.feature_router_viewed_at is not null as reached_feature_router,
    uoe.completion_viewed_at is not null as reached_completion,
    uoe.completed_at is not null as completed_onboarding,

    -- Permission outcomes
    coalesce(uoe.location_granted, false) as location_granted,
    coalesce(uoe.notification_granted, false) as notification_granted,
    coalesce(uoe.contacts_granted, false) as contacts_granted,

    -- Referral and feature selection
    coalesce(uoe.referral_submitted, false) as referral_submitted,
    uoe.feature_selected,

    -- Time to complete in seconds
    extract(epoch from (uoe.completed_at - uoe.welcome_viewed_at)) as time_to_complete_seconds,

    -- Onboarding date (date when they started)
    date(uoe.welcome_viewed_at) as onboarding_date

from user_onboarding_events uoe
inner join user_signup_dates usd
    on uoe.user_id = usd.user_id
where uoe.welcome_viewed_at is not null  -- Only users who started onboarding
