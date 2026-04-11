-- Per-session multiplayer engagement counts.
--
-- Three multiplayer event types are aggregated:
--   multiplayer_created — user started a new multiplayer voting session
--   multiplayer_joined  — user joined a multiplayer session another user started
--   multiplayer_voted   — user cast a vote in a multiplayer session
--
-- Typed properties (multiplayer_id, session_title, vote_type, places_count,
-- source_board_id) come through from stg_app_events_properties. This model
-- collapses them to one row per (session_id, user_id) so fct_session_outcomes
-- can cleanly join for has_multiplayer / mp_engagement columns, and so
-- fct_multiplayer_engagement has a session-grain feed.

with mp_events as (
    select
        effective_session_id as session_id,
        user_id,
        event_name,
        event_timestamp as mp_at,
        multiplayer_id,
        places_count,
        source_board_id
    from {{ ref('stg_app_events_enriched') }}
    where event_name in ('multiplayer_created', 'multiplayer_joined', 'multiplayer_voted')
      and effective_session_id is not null
),

session_multiplayer as (
    select
        session_id,
        user_id,
        count(*) filter (where event_name = 'multiplayer_created') as mp_sessions_created,
        count(*) filter (where event_name = 'multiplayer_joined') as mp_sessions_joined,
        count(*) filter (where event_name = 'multiplayer_voted') as mp_votes_cast,
        count(distinct multiplayer_id) filter (where multiplayer_id is not null) as distinct_multiplayer_ids,
        array_agg(distinct multiplayer_id) filter (where multiplayer_id is not null) as multiplayer_ids,
        min(mp_at) as first_mp_at,
        max(mp_at) as last_mp_at,
        max(places_count) filter (where event_name = 'multiplayer_created') as created_mp_places_count,
        max(source_board_id) filter (where event_name = 'multiplayer_created') as created_from_board_id
    from mp_events
    group by session_id, user_id
)

select * from session_multiplayer
