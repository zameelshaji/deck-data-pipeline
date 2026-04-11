{{ config(materialized='table') }}

-- First-Session Experience — per-user funnel for the first-session UX
-- (checklist, spin wheel, post-spin notification prompt).
--
-- Grain: one row per user_id who has entered the first-session experience.
-- Answers: "What's the drop-off from checklist_viewed → task_completed → spin_unlocked?"
--          "What fraction of spin-winners grant the notification permission?"
--          "How long does the first-session experience take?"

with checklist_events as (
    select
        user_id,
        min(event_timestamp) filter (where event_name = 'checklist_viewed') as checklist_viewed_at,
        count(*) filter (where event_name = 'checklist_task_completed') as tasks_completed_count,
        array_agg(task_name order by event_timestamp) filter (where event_name = 'checklist_task_completed') as tasks_completed_list,
        min(event_timestamp) filter (where event_name = 'checklist_task_completed') as first_task_completed_at,
        max(event_timestamp) filter (where event_name = 'checklist_task_completed') as last_task_completed_at,
        min(event_timestamp) filter (where event_name = 'checklist_all_completed') as checklist_all_completed_at
    from {{ ref('stg_app_events_enriched') }}
    where event_name in ('checklist_viewed', 'checklist_task_completed', 'checklist_all_completed')
    group by user_id
),

spin_events as (
    select
        user_id,
        min(event_timestamp) filter (where event_name = 'spin_wheel_unlocked') as spin_unlocked_at,
        min(event_timestamp) filter (where event_name = 'spin_wheel_rigged_win') as spin_won_at
    from {{ ref('stg_app_events_enriched') }}
    where event_name in ('spin_wheel_unlocked', 'spin_wheel_rigged_win')
    group by user_id
),

post_spin_notification as (
    select
        user_id,
        min(event_timestamp) filter (where event_name = 'post_spin_notification_prompt_shown') as notif_prompt_shown_at,
        min(event_timestamp) filter (where event_name = 'post_spin_notification_enabled') as notif_granted_at,
        min(event_timestamp) filter (where event_name = 'post_spin_notification_skipped') as notif_denied_at
    from {{ ref('stg_app_events_enriched') }}
    where event_name in (
        'post_spin_notification_prompt_shown',
        'post_spin_notification_enabled',
        'post_spin_notification_skipped'
    )
    group by user_id
)

select
    u.user_id,
    u.username,

    -- Checklist milestones
    ce.checklist_viewed_at,
    ce.first_task_completed_at,
    ce.last_task_completed_at,
    ce.tasks_completed_count,
    ce.tasks_completed_list,
    ce.checklist_all_completed_at,

    -- Spin wheel milestones
    se.spin_unlocked_at,
    se.spin_won_at,

    -- Post-spin notification prompt
    pn.notif_prompt_shown_at,
    pn.notif_granted_at,
    pn.notif_denied_at,

    -- Outcome flags
    ce.checklist_viewed_at is not null as saw_checklist,
    ce.checklist_all_completed_at is not null as completed_checklist,
    se.spin_unlocked_at is not null as unlocked_spin,
    se.spin_won_at is not null as won_spin,
    pn.notif_prompt_shown_at is not null as saw_notif_prompt,
    pn.notif_granted_at is not null as granted_notif,

    -- Durations
    case
        when ce.checklist_viewed_at is not null and ce.checklist_all_completed_at is not null
        then extract(epoch from (ce.checklist_all_completed_at - ce.checklist_viewed_at)) / 60.0
    end as minutes_checklist_view_to_complete,
    case
        when se.spin_unlocked_at is not null and se.spin_won_at is not null
        then extract(epoch from (se.spin_won_at - se.spin_unlocked_at)) / 60.0
    end as minutes_spin_unlock_to_win

from {{ ref('stg_users') }} u
inner join checklist_events ce on u.user_id = ce.user_id
left join spin_events se on u.user_id = se.user_id
left join post_spin_notification pn on u.user_id = pn.user_id
where u.is_test_user = 0
