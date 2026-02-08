{{ config(materialized='table') }}

-- User-level activation model
-- Activated = any user who has EVER prompted (>=1 query), saved (>=1 save), or shared (>=1 share)
-- Test users excluded

with users as (
    select
        user_id,
        date(created_at) as signup_date,
        date_trunc('week', created_at)::date as signup_week
    from {{ ref('stg_users') }}
    where is_test_user = 0
      and created_at is not null
),

user_first_actions as (
    select
        user_id,
        min(event_timestamp) filter (where event_type = 'query') as first_prompt_at,
        min(event_timestamp) filter (where event_type = 'save') as first_save_at,
        min(event_timestamp) filter (where event_category = 'Share') as first_share_at
    from {{ ref('stg_unified_events') }}
    group by user_id
),

activation as (
    select
        u.user_id,
        u.signup_date,
        u.signup_week,
        fa.first_prompt_at,
        fa.first_save_at,
        fa.first_share_at,
        least(fa.first_prompt_at, fa.first_save_at, fa.first_share_at) as activation_timestamp,
        (fa.first_prompt_at is not null or fa.first_save_at is not null or fa.first_share_at is not null) as is_activated
    from users u
    left join user_first_actions fa on u.user_id = fa.user_id
)

select
    user_id,
    signup_date,
    signup_week,
    is_activated,
    date(activation_timestamp) as activation_date,
    date_trunc('week', activation_timestamp)::date as activation_week,
    date_trunc('week', activation_timestamp)::date as cohort_week,
    (date(activation_timestamp) - signup_date)::integer as days_to_activation,
    case
        when first_prompt_at is not null and first_save_at is null and first_share_at is null then 'prompt_only'
        when first_save_at is not null and first_share_at is null then 'save_only'
        when first_share_at is not null and first_save_at is null then 'share_only'
        when first_save_at is not null and first_share_at is not null then 'save_and_share'
        else null
    end as activation_type,
    case
        when first_prompt_at is not null and first_save_at is null and first_share_at is null then 'prompt_only'
        when first_save_at is not null and first_share_at is null then 'save_only'
        when first_share_at is not null and first_save_at is null then 'share_only'
        when first_save_at is not null and first_share_at is not null then 'save_and_share'
        else null
    end as first_activation_event,
    first_prompt_at,
    first_save_at,
    first_share_at
from activation
