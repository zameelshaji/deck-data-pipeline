{{ config(materialized='table') }}

-- Social Graph — current edge set derived from follow/unfollow and like/unlike events
--
-- Grain: one row per (actor_user_id, target_kind, target_id)
-- Represents the LATEST known state of the edge: is it active or removed?
-- Feeds fct_viral_loop (virality signals) and fct_user_social_stats.
--
-- Edge semantics:
--   target_kind='user'  → actor follows target
--   target_kind='board' → actor likes target (deck_liked)

with latest_edge_state as (
    select
        actor_user_id,
        target_kind,
        target_id,
        -- Most recent action per edge (add/remove)
        (array_agg(edge_action order by occurred_at desc))[1] as latest_action,
        (array_agg(occurred_at order by occurred_at desc))[1] as latest_event_at,
        -- History timestamps
        min(occurred_at) filter (where edge_action = 'add') as first_added_at,
        max(occurred_at) filter (where edge_action = 'add') as last_added_at,
        max(occurred_at) filter (where edge_action = 'remove') as last_removed_at,
        count(*) filter (where edge_action = 'add') as add_count,
        count(*) filter (where edge_action = 'remove') as remove_count
    from {{ ref('stg_social_graph_events') }}
    inner join {{ ref('stg_users') }} u on actor_user_id = u.user_id
    where u.is_test_user = 0
      and target_id is not null
    group by actor_user_id, target_kind, target_id
)

select
    actor_user_id,
    target_kind,
    target_id,
    latest_action,
    latest_action = 'add' as is_active,
    first_added_at,
    last_added_at,
    last_removed_at,
    latest_event_at,
    add_count,
    remove_count
from latest_edge_state
