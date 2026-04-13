{{ config(materialized='table') }}

-- Per-event typed property extraction (Phase B.5, Tier 2)
--
-- Flattens app_events.properties JSONB into typed columns per event_name, so
-- downstream models don't need to re-parse JSON. Grain: one row per dedup'd
-- app_events row (matches stg_app_events_enriched).
--
-- Tier 1 universal fields (origin_surface, origin_source_id, action_type,
-- share_channel, fast_path, is_private, places_count, source_board_id,
-- save_method, task_name, multiplayer_id_prop, query_id_prop) live on
-- stg_app_events_enriched — this model carries them through and adds
-- event-specific fields on top.
--
-- See Dracon2/Dracon/Managers/TelemetryManager.swift (EventName enum +
-- per-event trackEvent call sites) for the authoritative payload schemas.

with enriched as (
    select *
    from {{ ref('stg_app_events_enriched') }}
)

select
    -- Keys + context (from stg_app_events_enriched)
    id,
    event_name,
    event_timestamp,
    user_id,
    effective_session_id,
    client_event_id,
    card_id,
    pack_id,
    board_id,
    share_link_id,
    origin_surface,
    origin_source_id,
    action_type,
    share_channel,
    fast_path,
    is_private,
    places_count,
    source_board_id,
    multiplayer_id_prop as multiplayer_id,
    query_id_prop,
    save_method,
    task_name,

    -- ------------------------------------------------------------
    -- Tier 2: per-event typed columns
    -- ------------------------------------------------------------

    -- card_saved / place_saved
    case when event_name in ('card_saved', 'place_saved')
        then nullif(properties->>'place_name', '') end as place_name,
    case when event_name in ('card_saved', 'place_saved')
        then nullif(properties->>'place_category', '') end as place_category,
    case when event_name = 'card_saved'
        then nullif(properties->>'board_name', '') end as board_name_from_save,
    case when event_name = 'card_saved'
         and properties->>'already_saved' in ('true', 'false')
        then (properties->>'already_saved')::boolean end as already_saved,
    case when event_name = 'card_saved'
         and properties->>'position_in_pack' ~ '^-?[0-9]+$'
        then (properties->>'position_in_pack')::int end as position_in_pack,

    -- card_viewed
    case when event_name = 'card_viewed'
        then nullif(properties->>'place_name', '') end as viewed_place_name,

    -- Swipes (card_swiped_right / card_swiped_left)
    case when event_name in ('card_swiped_right', 'card_swiped_left')
         and properties->>'place_id' ~ '^-?[0-9]+$'
        then (properties->>'place_id')::bigint end as swiped_place_id,
    case when event_name in ('card_swiped_right', 'card_swiped_left')
        then nullif(properties->>'source', '') end as swipe_source,

    -- dextr_query_submitted
    case when event_name = 'dextr_query_submitted'
        then nullif(properties->>'query_text', '') end as query_text,
    case when event_name = 'dextr_query_submitted'
        then nullif(properties->>'location', '') end as query_location,
    case when event_name = 'dextr_query_submitted'
         and properties->>'message_count' ~ '^-?[0-9]+$'
        then (properties->>'message_count')::int end as query_message_count,

    -- dextr_results_viewed
    case when event_name = 'dextr_results_viewed'
         and properties->>'results_count' ~ '^-?[0-9]+$'
        then (properties->>'results_count')::int end as results_count,
    case when event_name = 'dextr_results_viewed'
        then nullif(properties->>'query', '') end as results_query_text,

    -- board_created / board_viewed
    case when event_name in ('board_created', 'board_viewed')
        then nullif(properties->>'board_name', '') end as board_name,

    -- multiplayer_created
    case when event_name = 'multiplayer_created'
        then nullif(properties->>'session_title', '') end as mp_session_title,
    case when event_name = 'multiplayer_created'
        then nullif(properties->>'source_type', '') end as mp_source_type,

    -- multiplayer_joined (also carries session_title — collapsed with above)
    case when event_name = 'multiplayer_joined'
        then nullif(properties->>'session_title', '') end as mp_joined_session_title,

    -- multiplayer_voted
    case when event_name = 'multiplayer_voted'
        then nullif(properties->>'vote_type', '') end as vote_type,
    case when event_name = 'multiplayer_voted'
        then nullif(properties->>'participant_id', '') end as mp_participant_id,

    -- user_followed / user_unfollowed
    case when event_name = 'user_followed'
        then nullif(properties->>'followed_user_id', '') end as followed_user_id,
    case when event_name = 'user_unfollowed'
        then nullif(properties->>'unfollowed_user_id', '') end as unfollowed_user_id,

    -- whats_next_tapped (carries action-specific fields)
    case when event_name = 'whats_next_tapped'
        then nullif(properties->>'action_title', '') end as whats_next_action_title,
    case when event_name = 'whats_next_tapped'
         and properties->>'remaining_votes' ~ '^-?[0-9]+$'
        then (properties->>'remaining_votes')::int end as whats_next_remaining_votes,
    case when event_name = 'whats_next_tapped'
         and properties->>'total_places' ~ '^-?[0-9]+$'
        then (properties->>'total_places')::int end as whats_next_total_places,
    case when event_name = 'whats_next_tapped'
         and properties->>'voted_count' ~ '^-?[0-9]+$'
        then (properties->>'voted_count')::int end as whats_next_voted_count,

    -- session_ended
    case when event_name = 'session_ended'
        then nullif(properties->>'status', '') end as session_end_status,
    case when event_name = 'session_ended'
         and properties->>'saves_count' ~ '^-?[0-9]+$'
        then (properties->>'saves_count')::int end as session_saves_count,
    case when event_name = 'session_ended'
         and properties->>'shares_count' ~ '^-?[0-9]+$'
        then (properties->>'shares_count')::int end as session_shares_count,
    case when event_name = 'session_ended'
         and properties->>'duration_seconds' ~ '^-?[0-9]+$'
        then (properties->>'duration_seconds')::int end as session_duration_seconds,

    -- onboarding_v2_completed
    case when event_name = 'onboarding_v2_completed'
         and properties->>'places_swiped_right' ~ '^-?[0-9]+$'
        then (properties->>'places_swiped_right')::int end as onboarding_places_swiped_right,

    -- Keep raw properties for any consumer that needs additional extraction
    properties

from enriched
