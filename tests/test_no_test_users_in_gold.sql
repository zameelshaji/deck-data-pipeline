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
)

select * from segments_violations
union all
select * from sessions_violations
union all
select * from activation_violations
