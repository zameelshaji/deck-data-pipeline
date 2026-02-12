{{ config(materialized='table') }}

-- Investor-Ready Cohort Analysis
-- Grain: one row per activation week cohort
-- Answers: "Which activation week produced best D30 retention?",
--          "D7/D30/D60 by user type?",
--          "What % of churned users had 0 saves?",
--          "What first-session behaviors predict D30 retention?"

with user_data as (
    select
        us.user_id,
        us.activation_week,
        us.is_activated,
        us.user_archetype,
        us.is_planner,
        us.total_sessions,
        us.total_prompts,
        us.total_saves,
        us.total_shares,
        us.total_swipes,
        us.retained_d7,
        us.retained_d30,
        us.retained_d60,
        us.is_churned,
        us.days_since_last_activity,
        us.referral_source,
        us.activated_in_first_session
    from {{ ref('fct_user_segments') }} us
    where us.is_activated = true
      and us.activation_week is not null
),

-- First-session metrics per user
first_session_metrics as (
    select distinct on (s.user_id)
        s.user_id,
        s.save_count as first_session_saves,
        case
            when s.save_count > 0 or s.share_count > 0 then true
            else false
        end as first_session_activated,
        s.session_duration_seconds as first_session_duration
    from {{ ref('fct_session_outcomes') }} s
    inner join {{ ref('fct_user_activation') }} a
        on s.user_id = a.user_id
    where a.is_activated = true
    order by s.user_id, s.started_at
),

-- First-week post-activation activity
first_week_activity as (
    select
        s.user_id,
        count(*) as sessions_first_week,
        count(distinct s.session_date) as days_active_first_week,
        count(distinct s.session_date) > 1 as is_multi_day_first_week
    from {{ ref('fct_session_outcomes') }} s
    inner join {{ ref('fct_user_activation') }} a
        on s.user_id = a.user_id
    where a.is_activated = true
      and s.session_date between a.activation_date and a.activation_date + 7
    group by s.user_id
),

-- First-week prompts
first_week_prompts as (
    select
        e.user_id,
        count(*) as prompts_first_week
    from {{ ref('stg_unified_events') }} e
    inner join {{ ref('fct_user_activation') }} a
        on e.user_id = a.user_id
    where a.is_activated = true
      and e.event_type = 'dextr_query'
      and date(e.event_timestamp) between a.activation_date and a.activation_date + 7
    group by e.user_id
),

-- Retention maturity from fct_retention_activated
maturity as (
    select
        user_id,
        is_mature_d7,
        is_mature_d30,
        is_mature_d60
    from {{ ref('fct_retention_activated') }}
),

-- Activation timing from fct_user_activation
activation_timing as (
    select
        user_id,
        days_to_activation
    from {{ ref('fct_user_activation') }}
    where is_activated = true
)

select
    ud.activation_week,
    count(*) as cohort_size,

    -- Activation timing
    round(avg(at.days_to_activation), 1) as avg_days_signup_to_activation,

    -- By archetype
    count(*) filter (where ud.user_archetype = 'one_and_done') as one_and_done_count,
    round(100.0 * count(*) filter (where ud.user_archetype = 'one_and_done') / count(*), 1) as one_and_done_pct,
    count(*) filter (where ud.user_archetype = 'browser') as browser_count,
    round(100.0 * count(*) filter (where ud.user_archetype = 'browser') / count(*), 1) as browser_pct,
    count(*) filter (where ud.user_archetype = 'saver') as saver_count,
    round(100.0 * count(*) filter (where ud.user_archetype = 'saver') / count(*), 1) as saver_pct,
    count(*) filter (where ud.user_archetype in ('planner', 'power_planner')) as planner_count,
    round(100.0 * count(*) filter (where ud.user_archetype in ('planner', 'power_planner')) / count(*), 1) as planner_pct,

    -- Retention (only for mature users)
    count(*) filter (where m.is_mature_d7) as mature_d7,
    count(*) filter (where m.is_mature_d7 and ud.retained_d7) as retained_d7_count,
    case
        when count(*) filter (where m.is_mature_d7) > 0
        then round(count(*) filter (where m.is_mature_d7 and ud.retained_d7)::numeric
                    / count(*) filter (where m.is_mature_d7), 4)
        else null
    end as retention_rate_d7,

    count(*) filter (where m.is_mature_d30) as mature_d30,
    count(*) filter (where m.is_mature_d30 and ud.retained_d30) as retained_d30_count,
    case
        when count(*) filter (where m.is_mature_d30) > 0
        then round(count(*) filter (where m.is_mature_d30 and ud.retained_d30)::numeric
                    / count(*) filter (where m.is_mature_d30), 4)
        else null
    end as retention_rate_d30,

    count(*) filter (where m.is_mature_d60) as mature_d60,
    count(*) filter (where m.is_mature_d60 and ud.retained_d60) as retained_d60_count,
    case
        when count(*) filter (where m.is_mature_d60) > 0
        then round(count(*) filter (where m.is_mature_d60 and ud.retained_d60)::numeric
                    / count(*) filter (where m.is_mature_d60), 4)
        else null
    end as retention_rate_d60,

    -- Quality signals
    round(avg(fsm.first_session_saves), 2) as avg_first_session_saves,
    round(100.0 * count(*) filter (where ud.activated_in_first_session) / count(*), 1) as pct_activated_in_first_session,
    case
        when count(*) filter (where ud.is_churned) > 0
        then round(100.0 * count(*) filter (where ud.is_churned and ud.total_saves = 0)
                    / count(*) filter (where ud.is_churned), 1)
        else null
    end as pct_churned_with_zero_saves,
    case
        when count(*) filter (where ud.is_churned) > 0
        then round(100.0 * count(*) filter (where ud.is_churned and ud.total_saves > 0)
                    / count(*) filter (where ud.is_churned), 1)
        else null
    end as pct_churned_with_saves,

    -- Leading indicators (first week post-activation)
    round(avg(fwa.sessions_first_week), 2) as avg_sessions_first_week,
    round(100.0 * count(*) filter (where fwa.is_multi_day_first_week) / count(*), 1)
        as pct_multi_day_first_week,
    round(avg(fwp.prompts_first_week), 2) as avg_prompts_first_week,

    -- Referral source breakdown
    count(*) filter (where ud.referral_source = 'organic') as organic_count,
    count(*) filter (where ud.referral_source = 'referral') as referral_count,
    case
        when count(*) filter (where ud.referral_source = 'organic' and m.is_mature_d30) > 0
        then round(count(*) filter (where ud.referral_source = 'organic' and m.is_mature_d30 and ud.retained_d30)::numeric
                    / count(*) filter (where ud.referral_source = 'organic' and m.is_mature_d30), 4)
        else null
    end as organic_retention_d30,
    case
        when count(*) filter (where ud.referral_source = 'referral' and m.is_mature_d30) > 0
        then round(count(*) filter (where ud.referral_source = 'referral' and m.is_mature_d30 and ud.retained_d30)::numeric
                    / count(*) filter (where ud.referral_source = 'referral' and m.is_mature_d30), 4)
        else null
    end as referral_retention_d30

from user_data ud
left join first_session_metrics fsm on ud.user_id = fsm.user_id
left join first_week_activity fwa on ud.user_id = fwa.user_id
left join first_week_prompts fwp on ud.user_id = fwp.user_id
left join maturity m on ud.user_id = m.user_id
left join activation_timing at on ud.user_id = at.user_id
group by ud.activation_week
order by ud.activation_week
