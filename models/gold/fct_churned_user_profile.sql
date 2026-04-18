-- Profile of churned users
--
-- One row per user where fct_user_segments.is_churned = true AND
-- is_activated = true (an activated user who has since gone dormant).
-- Pulls together the user's acquisition attributes, lifetime totals, their
-- last 3 sessions, and first/last prompt intent — so we can answer EQT's
-- "what did churned users look like before they left?"

with churned_users as (
    select
        user_id,
        activation_week as cohort_week,
        referral_source,
        activation_trigger,
        days_to_activation,
        last_activity_date,
        days_since_last_activity,
        total_sessions,
        total_prompts,
        total_saves,
        total_shares,
        user_archetype,
        is_activated
    from {{ ref('fct_user_segments') }}
    where is_churned = true
      and is_activated = true
),

last_3_sessions as (
    select
        user_id,
        (array_agg(started_at order by started_at desc))[1:3] as last_3_sessions_started_at,
        (array_agg(has_save   order by started_at desc))[1:3] as last_3_sessions_had_save,
        (array_agg(has_share  order by started_at desc))[1:3] as last_3_sessions_had_share
    from {{ ref('fct_session_outcomes') }}
    group by user_id
),

prompts_ordered as (
    select
        user_id,
        prompt_intent,
        row_number() over (partition by user_id order by query_timestamp asc)  as rn_asc,
        row_number() over (partition by user_id order by query_timestamp desc) as rn_desc
    from {{ ref('fct_prompt_analysis') }}
),

first_last_prompt as (
    select
        user_id,
        max(prompt_intent) filter (where rn_asc  = 1) as first_prompt_intent,
        max(prompt_intent) filter (where rn_desc = 1) as last_prompt_intent
    from prompts_ordered
    group by user_id
)

select
    cu.user_id,
    cu.cohort_week,
    cu.referral_source,
    cu.activation_trigger,
    cu.days_to_activation,
    cu.last_activity_date,
    cu.days_since_last_activity,
    cu.total_sessions,
    cu.total_prompts,
    cu.total_saves,
    cu.total_shares,
    cu.user_archetype,
    ls.last_3_sessions_started_at,
    ls.last_3_sessions_had_save,
    ls.last_3_sessions_had_share,
    flp.first_prompt_intent,
    flp.last_prompt_intent,
    (cu.total_saves = 0)       as churned_without_save,
    cu.is_activated            as churned_after_activation
from churned_users cu
left join last_3_sessions  ls  on cu.user_id = ls.user_id
left join first_last_prompt flp on cu.user_id = flp.user_id
