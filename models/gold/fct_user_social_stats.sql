{{ config(materialized='table') }}

-- User Social Stats — per-user roll-up of social graph state
--
-- Grain: one row per user_id.
-- Answers: "Who has the most followers?"
--          "Which users have liked the most decks?"
--          "Who is being followed by whom?" (via joins)

with follows_out as (
    -- Edges where this user is the actor (following other users)
    select
        actor_user_id as user_id,
        count(*) filter (where is_active) as following_count
    from {{ ref('fct_social_graph') }}
    where target_kind = 'user'
    group by actor_user_id
),

follows_in as (
    -- Edges where this user is the target (being followed)
    select
        target_id::uuid as user_id,
        count(*) filter (where is_active) as follower_count
    from {{ ref('fct_social_graph') }}
    where target_kind = 'user'
      and target_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    group by target_id::uuid
),

decks_liked as (
    -- Edges where this user liked a deck
    select
        actor_user_id as user_id,
        count(*) filter (where is_active) as decks_liked_count
    from {{ ref('fct_social_graph') }}
    where target_kind = 'board'
    group by actor_user_id
),

likes_received as (
    -- Likes received on decks this user owns. Board → creator join via src_boards.
    select
        b.user_id as user_id,
        count(*) filter (where sg.is_active) as likes_received_count
    from {{ ref('fct_social_graph') }} sg
    inner join {{ ref('src_boards') }} b on sg.target_id::text = b.id::text
    where sg.target_kind = 'board'
    group by b.user_id
)

select
    u.user_id,
    u.username,
    u.email,
    coalesce(fo.following_count, 0) as following_count,
    coalesce(fi.follower_count, 0) as follower_count,
    coalesce(dl.decks_liked_count, 0) as decks_liked_count,
    coalesce(lr.likes_received_count, 0) as likes_received_count
from {{ ref('stg_users') }} u
left join follows_out fo on u.user_id = fo.user_id
left join follows_in fi on u.user_id = fi.user_id
left join decks_liked dl on u.user_id = dl.user_id
left join likes_received lr on u.user_id = lr.user_id
where u.is_test_user = 0
  and (coalesce(fo.following_count, 0) > 0
       or coalesce(fi.follower_count, 0) > 0
       or coalesce(dl.decks_liked_count, 0) > 0
       or coalesce(lr.likes_received_count, 0) > 0)
