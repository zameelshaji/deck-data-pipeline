-- Event-grain social graph model (not session-grain).
--
-- Social actions are usually cross-session — following a user or liking a
-- deck happens once and has long-lived effects on the graph. So unlike the
-- other stg_session_* models, this one is one row per event, with the
-- source and target of each edge resolved to typed columns.
--
-- Covers:
--   user_followed / user_unfollowed — directed user-to-user edges
--   deck_liked / deck_unliked       — directed user-to-board edges
--
-- Downstream: fct_social_graph folds this into the current edge set;
-- fct_viral_loop uses deck_like events as a viral signal.

with social_events as (
    select
        id,
        event_name,
        event_timestamp as occurred_at,
        user_id as actor_user_id,
        effective_session_id as session_id,
        board_id,
        -- Extracted via stg_app_events_enriched (Tier 1)
        -- followed_user_id and unfollowed_user_id carry the target user
        nullif(properties->>'followed_user_id', '') as followed_user_id,
        nullif(properties->>'unfollowed_user_id', '') as unfollowed_user_id
    from {{ ref('stg_app_events_enriched') }}
    where event_name in ('user_followed', 'user_unfollowed', 'deck_liked', 'deck_unliked')
),

normalized as (
    select
        id as event_id,
        event_name,
        occurred_at,
        actor_user_id,
        session_id,
        case
            when event_name in ('user_followed', 'user_unfollowed') then 'user'
            when event_name in ('deck_liked', 'deck_unliked') then 'board'
        end as target_kind,
        case
            when event_name = 'user_followed' then followed_user_id
            when event_name = 'user_unfollowed' then unfollowed_user_id
            when event_name in ('deck_liked', 'deck_unliked') then board_id::text
        end as target_id,
        case
            when event_name in ('user_followed', 'deck_liked') then 'add'
            when event_name in ('user_unfollowed', 'deck_unliked') then 'remove'
        end as edge_action
    from social_events
)

select * from normalized
