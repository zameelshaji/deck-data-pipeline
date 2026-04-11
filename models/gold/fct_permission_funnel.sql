{{ config(materialized='table') }}

-- Permission Funnel — notification + location permission prompt conversion
--
-- Grain: one row per (permission_type) — rolled up from stg_permission_events.
-- Answers: "What % of users who see the notification prompt actually grant it?"
--          "Does the deferred location prompt convert better than the native one?"
--          "Are we losing users to permission fatigue?"

with per_user as (
    select *
    from {{ ref('stg_permission_events') }}
    inner join {{ ref('stg_users') }} u using (user_id)
    where u.is_test_user = 0
)

select
    permission_type,
    count(*) as users_prompted,
    count(*) filter (where latest_state = 'granted') as users_granted,
    count(*) filter (where latest_state = 'denied') as users_denied,
    count(*) filter (where latest_state = 'prompt_shown') as users_pending,
    round(
        count(*) filter (where latest_state = 'granted')::numeric
        / nullif(count(*), 0), 4
    ) as grant_rate,
    round(
        count(*) filter (where latest_state = 'denied')::numeric
        / nullif(count(*), 0), 4
    ) as denial_rate,
    sum(prompts_shown) as total_prompts_shown,
    sum(grants) as total_grants,
    sum(denials) as total_denials
from per_user
group by permission_type
order by permission_type
