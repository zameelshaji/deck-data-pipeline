-- User-grain permission funnel state.
--
-- Collapses the permission-prompt event stream (deferred_location_* and
-- post_spin_notification_*) into one row per (user_id, permission_type)
-- showing the latest outcome and full timing history.
--
-- Handles the two permission flows iOS currently tracks:
--   - location: deferred_location_prompt_shown / _granted / _denied
--   - notif:    post_spin_notification_prompt_shown / _enabled / _skipped
--
-- Downstream: fct_permission_funnel turns this into a simple conversion
-- funnel (shown → granted / denied / skipped).

with permission_events as (
    select
        user_id,
        event_name,
        event_timestamp as occurred_at,
        case
            when event_name like 'deferred_location_%' then 'location'
            when event_name like 'post_spin_notification_%' then 'notification'
        end as permission_type,
        case
            when event_name in ('deferred_location_prompt_shown', 'post_spin_notification_prompt_shown') then 'prompt_shown'
            when event_name in ('deferred_location_granted', 'post_spin_notification_enabled') then 'granted'
            when event_name in ('deferred_location_denied', 'post_spin_notification_skipped') then 'denied'
        end as state
    from {{ ref('stg_app_events_enriched') }}
    where event_name in (
        'deferred_location_prompt_shown',
        'deferred_location_granted',
        'deferred_location_denied',
        'post_spin_notification_prompt_shown',
        'post_spin_notification_enabled',
        'post_spin_notification_skipped'
    )
),

per_user as (
    select
        user_id,
        permission_type,
        count(*) filter (where state = 'prompt_shown') as prompts_shown,
        count(*) filter (where state = 'granted') as grants,
        count(*) filter (where state = 'denied') as denials,
        min(occurred_at) filter (where state = 'prompt_shown') as first_prompt_at,
        min(occurred_at) filter (where state = 'granted') as first_granted_at,
        min(occurred_at) filter (where state = 'denied') as first_denied_at,
        -- Latest state: granted > denied > prompt_shown (prefer positive outcome)
        case
            when max(case when state = 'granted' then 1 else 0 end) = 1 then 'granted'
            when max(case when state = 'denied' then 1 else 0 end) = 1 then 'denied'
            else 'prompt_shown'
        end as latest_state,
        max(occurred_at) as last_event_at
    from permission_events
    where permission_type is not null
    group by user_id, permission_type
)

select * from per_user
