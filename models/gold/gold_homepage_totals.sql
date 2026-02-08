{{ config(materialized='table') }}

-- Homepage totals: includes test users for all metrics EXCEPT MAU and total_activated_users

with
-- Count ALL signups from auth.users (including test users)
total_signups as (
    select count(*) as total_signups
    from {{ ref('src_users') }}
),

-- Total prompts across all eras (including test users)
total_prompts as (
    select count(*) as total_prompts
    from {{ ref('src_dextr_queries') }}
),

-- Total swipes including legacy (including test users)
total_swipes as (
    select count(*) as total_swipes from (
        -- Legacy swipes
        select 1 from {{ ref('src_user_swipes_v2') }}
        union all
        select 1 from {{ ref('src_user_liked_places') }}
        union all
        -- dextr_pack_cards swipes (pre Nov 20)
        select 1 from {{ ref('src_dextr_pack_cards') }}
        where user_action in ('left', 'right') and created_at < '2025-11-20'
        union all
        -- dextr_places swipes (Nov 20+)
        select 1 from {{ ref('src_dextr_places') }}
        where user_action in ('like', 'dislike') and created_at >= '2025-11-20'
    ) s
),

-- Total saves (including test users)
total_saves as (
    select count(*) as total_saves from (
        select 1 from {{ ref('src_core_card_actions') }} where action_type = 'saved' and timestamp < '2026-01-30'
        union all
        select 1 from {{ ref('src_app_events') }} where event_name = 'card_saved' and event_timestamp >= '2026-01-30'
    ) s
),

-- Total shares (including test users)
total_shares as (
    select count(*) as total_shares from (
        select 1 from {{ ref('src_core_card_actions') }} where action_type = 'share' and timestamp < '2026-01-30'
        union all
        select 1 from {{ ref('src_app_events') }} where event_name in ('card_shared', 'deck_shared') and event_timestamp >= '2026-01-30'
    ) s
),

-- Total places in catalog
total_places as (
    select count(*) as total_places from {{ ref('src_places') }}
),

-- Total boards created
total_boards as (
    select count(*) as total_boards from {{ ref('src_boards') }} where is_default = false
),

-- Total multiplayer sessions (only sessions with 2+ participants)
total_multiplayer as (
    select count(*) as total_multiplayer_sessions
    from (
        select sp.multiplayer_id
        from {{ ref('src_session_participants') }} sp
        group by sp.multiplayer_id
        having count(*) >= 2
    ) real_multiplayer
),

-- MAU (EXCLUDES test users, only activated users) — unique activated users with >=1 event in last 30 days
mau as (
    select count(distinct e.user_id) as mau
    from {{ ref('stg_unified_events') }} e
    inner join {{ ref('stg_users') }} u on e.user_id = u.user_id
    inner join {{ ref('fct_user_activation') }} a on e.user_id = a.user_id and a.is_activated = true
    where u.is_test_user = 0
      and e.event_timestamp >= current_date - interval '30 days'
),

-- Total activated users (EXCLUDES test users)
total_activated as (
    select count(*) as total_activated_users
    from {{ ref('fct_user_activation') }}
    where is_activated = true
)

select
    (select total_signups from total_signups) as total_signups,
    (select total_prompts from total_prompts) as total_prompts,
    (select total_swipes from total_swipes) as total_swipes,
    (select total_saves from total_saves) as total_saves,
    (select total_shares from total_shares) as total_shares,
    (select total_places from total_places) as total_places,
    (select total_boards from total_boards) as total_boards,
    (select total_multiplayer_sessions from total_multiplayer) as total_multiplayer_sessions,
    (select mau from mau) as mau,
    (select total_activated_users from total_activated) as total_activated_users,
    current_timestamp as last_refreshed_at
