-- Retention by attribute at acquisition
-- One row per (cohort_week, attribute_name, attribute_value)
-- attribute_name ∈ { 'referral_source', 'activation_trigger',
--                    'first_prompt_intent', 'app_version_at_signup' }
-- Retention windows D7/D30/D60/D90 computed the same way as
-- fct_retention_by_cohort_week (activity on activation_date + 1..N).

with activated_users as (
    select
        user_id,
        activation_date,
        cohort_week,
        activation_type
    from {{ ref('fct_user_activation') }}
    where is_activated = true
      and activation_date is not null
),

user_activity_dates as (
    select distinct
        user_id,
        session_date
    from {{ ref('fct_session_outcomes') }}
    where has_save or has_share or is_prompt_session
),

user_retention as (
    select
        a.user_id,
        a.activation_date,
        a.cohort_week,
        a.activation_type,

        current_date >= a.activation_date + 7  as is_mature_d7,
        current_date >= a.activation_date + 30 as is_mature_d30,
        current_date >= a.activation_date + 60 as is_mature_d60,
        current_date >= a.activation_date + 90 as is_mature_d90,

        coalesce(bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 7
        ), false) as had_activity_d7,
        coalesce(bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 30
        ), false) as had_activity_d30,
        coalesce(bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 60
        ), false) as had_activity_d60,
        coalesce(bool_or(
            ua.session_date between a.activation_date + 1 and a.activation_date + 90
        ), false) as had_activity_d90

    from activated_users a
    left join user_activity_dates ua on a.user_id = ua.user_id
    group by a.user_id, a.activation_date, a.cohort_week, a.activation_type
),

-- Attribute 1: referral source (organic vs referral).
attr_referral_source as (
    select
        s.user_id,
        'referral_source'::text               as attribute_name,
        coalesce(s.referral_source, 'unknown') as attribute_value
    from {{ ref('fct_user_segments') }} s
),

-- Attribute 2: first activation trigger.
-- Uses fct_user_segments.activation_trigger (first_prompt / first_save / first_share)
-- which is the canonical "what came first" label.
attr_activation_trigger as (
    select
        s.user_id,
        'activation_trigger'::text                  as attribute_name,
        coalesce(s.activation_trigger, 'unknown')   as attribute_value
    from {{ ref('fct_user_segments') }} s
),

-- Attribute 3: first-prompt intent.
-- Each user's earliest prompt intent from fct_prompt_analysis. Users who
-- never prompted get the '_no_prompt' bucket so the partition is total.
user_first_prompt as (
    select distinct on (user_id)
        user_id,
        prompt_intent
    from {{ ref('fct_prompt_analysis') }}
    order by user_id, query_timestamp asc
),

attr_first_prompt_intent as (
    select
        au.user_id,
        'first_prompt_intent'::text                    as attribute_name,
        coalesce(fp.prompt_intent, '_no_prompt')       as attribute_value
    from activated_users au
    left join user_first_prompt fp on au.user_id = fp.user_id
),

-- Attribute 4: app version at signup.
-- Take the user's earliest session's app_version. NULL / pre-telemetry users
-- fall into the 'unknown' bucket.
user_first_session as (
    select distinct on (user_id)
        user_id,
        app_version
    from {{ ref('stg_unified_sessions') }}
    where user_id is not null
    order by user_id, started_at asc
),

attr_app_version as (
    select
        au.user_id,
        'app_version_at_signup'::text                  as attribute_name,
        coalesce(ufs.app_version, 'unknown')           as attribute_value
    from activated_users au
    left join user_first_session ufs on au.user_id = ufs.user_id
),

-- Union all attribute assignments; each user contributes one row per attribute.
user_attributes as (
    select user_id, attribute_name, attribute_value from attr_referral_source
    union all
    select user_id, attribute_name, attribute_value from attr_activation_trigger
    union all
    select user_id, attribute_name, attribute_value from attr_first_prompt_intent
    union all
    select user_id, attribute_name, attribute_value from attr_app_version
),

-- Per-user retention × attribute assignment.
user_retention_with_attrs as (
    select
        ur.cohort_week,
        ua.attribute_name,
        ua.attribute_value,
        ur.is_mature_d7,
        ur.is_mature_d30,
        ur.is_mature_d60,
        ur.is_mature_d90,
        ur.had_activity_d7,
        ur.had_activity_d30,
        ur.had_activity_d60,
        ur.had_activity_d90
    from user_retention ur
    inner join user_attributes ua on ur.user_id = ua.user_id
),

aggregated as (
    select
        cohort_week,
        attribute_name,
        attribute_value,

        count(*) as cohort_size,

        count(*) filter (where is_mature_d7)  as mature_d7,
        count(*) filter (where is_mature_d30) as mature_d30,
        count(*) filter (where is_mature_d60) as mature_d60,
        count(*) filter (where is_mature_d90) as mature_d90,

        count(*) filter (where is_mature_d7  and had_activity_d7)  as retained_d7,
        count(*) filter (where is_mature_d30 and had_activity_d30) as retained_d30,
        count(*) filter (where is_mature_d60 and had_activity_d60) as retained_d60,
        count(*) filter (where is_mature_d90 and had_activity_d90) as retained_d90

    from user_retention_with_attrs
    group by cohort_week, attribute_name, attribute_value
)

select
    cohort_week,
    attribute_name,
    attribute_value,
    cohort_size,

    mature_d7,
    mature_d30,
    mature_d60,
    mature_d90,

    retained_d7,
    retained_d30,
    retained_d60,
    retained_d90,

    case when mature_d7  > 0 then retained_d7::numeric  / mature_d7  else null end as retention_rate_d7,
    case when mature_d30 > 0 then retained_d30::numeric / mature_d30 else null end as retention_rate_d30,
    case when mature_d60 > 0 then retained_d60::numeric / mature_d60 else null end as retention_rate_d60,
    case when mature_d90 > 0 then retained_d90::numeric / mature_d90 else null end as retention_rate_d90

from aggregated
order by cohort_week desc, attribute_name, attribute_value
