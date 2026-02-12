-- Ensure no test users leak into gold layer fact tables
-- Test accounts must be excluded from all analytical tables

with test_users as (
    select user_id
    from {{ ref('stg_users') }}
    where is_test_user = 1
),

-- Check fct_user_segments
segments_violations as (
    select 'fct_user_segments' as table_name, s.user_id
    from {{ ref('fct_user_segments') }} s
    inner join test_users t on s.user_id = t.user_id
),

-- Check fct_session_outcomes
sessions_violations as (
    select 'fct_session_outcomes' as table_name, s.user_id
    from {{ ref('fct_session_outcomes') }} s
    inner join test_users t on s.user_id = t.user_id
),

-- Check fct_user_activation
activation_violations as (
    select 'fct_user_activation' as table_name, a.user_id
    from {{ ref('fct_user_activation') }} a
    inner join test_users t on a.user_id = t.user_id
),

-- Check fct_user_engagement_trajectory
trajectory_violations as (
    select 'fct_user_engagement_trajectory' as table_name, e.user_id
    from {{ ref('fct_user_engagement_trajectory') }} e
    inner join test_users t on e.user_id = t.user_id
),

-- Check fct_prompt_analysis
prompt_violations as (
    select 'fct_prompt_analysis' as table_name, p.user_id
    from {{ ref('fct_prompt_analysis') }} p
    inner join test_users t on p.user_id = t.user_id
),

-- Check fct_conversion_signals
conversion_violations as (
    select 'fct_conversion_signals' as table_name, c.user_id
    from {{ ref('fct_conversion_signals') }} c
    inner join test_users t on c.user_id = t.user_id
),

-- Check fct_viral_loop (uses sharer_user_id)
viral_violations as (
    select 'fct_viral_loop' as table_name, v.sharer_user_id as user_id
    from {{ ref('fct_viral_loop') }} v
    inner join test_users t on v.sharer_user_id = t.user_id
),

-- Check fct_user_retention
retention_violations as (
    select 'fct_user_retention' as table_name, r.user_id
    from {{ ref('fct_user_retention') }} r
    inner join test_users t on r.user_id = t.user_id
)

select * from segments_violations
union all
select * from sessions_violations
union all
select * from activation_violations
union all
select * from trajectory_violations
union all
select * from prompt_violations
union all
select * from conversion_violations
union all
select * from viral_violations
union all
select * from retention_violations
