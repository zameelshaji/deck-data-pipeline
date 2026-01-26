-- Unified event stream for North Star sessionization
-- Extends stg_events with multiplayer actions and board saves
-- This is the base for 10-minute gap session derivation

with base_events as (
    -- Use existing stg_events as primary source (already handles 8 event types)
    select
        user_id,
        card_id::text as card_id,
        event_source,
        event_name,
        event_category,
        source,
        source_id::text as source_id,
        event_timestamp
    from {{ ref('stg_events') }}
    where user_id is not null
),

multiplayer_create_events as (
    -- Multiplayer session creation events (not in stg_events)
    select
        user_id,
        null::text as card_id,
        'multiplayer' as event_source,
        'multiplayer_create' as event_name,
        'Social' as event_category,
        source_type as source,
        multiplayer_id::text as source_id,
        action_at as event_timestamp
    from {{ ref('int_user_multiplayer_actions') }}
    where action_type = 'multiplayer_create'
),

multiplayer_join_events as (
    -- Multiplayer session join events (not in stg_events)
    select
        user_id,
        null::text as card_id,
        'multiplayer' as event_source,
        'multiplayer_join' as event_name,
        'Social' as event_category,
        source_type as source,
        multiplayer_id::text as source_id,
        action_at as event_timestamp
    from {{ ref('int_user_multiplayer_actions') }}
    where action_type = 'multiplayer_join'
),

board_save_events as (
    -- Board saves that may not be captured in stg_events
    -- Only include board_places_v2 source to avoid duplication with direct saves
    select
        user_id,
        card_id,
        'board' as event_source,
        'board_save' as event_name,
        'Content Curation' as event_category,
        source_context as source,
        source_context as source_id,
        saved_at as event_timestamp
    from {{ ref('int_user_saves_unified') }}
    where original_source = 'board_places_v2'
),

share_events as (
    -- Card share events (ensure they're in the unified stream)
    select
        user_id,
        card_id,
        'share' as event_source,
        'card_share' as event_name,
        'Social' as event_category,
        share_surface as source,
        source_context as source_id,
        shared_at as event_timestamp
    from {{ ref('int_user_shares_unified') }}
),

combined_events as (
    select * from base_events
    union all
    select * from multiplayer_create_events
    union all
    select * from multiplayer_join_events
    union all
    select * from board_save_events
    union all
    select * from share_events
)

select
    -- Generate event_id for uniqueness
    md5(
        coalesce(user_id::text, '') || '-' ||
        coalesce(card_id, '') || '-' ||
        event_source || '-' ||
        event_name || '-' ||
        event_timestamp::text
    ) as event_id,
    user_id,
    card_id,
    event_source,
    event_name,
    event_category,
    source,
    source_id,
    event_timestamp,
    date(event_timestamp) as event_date,
    date_trunc('week', event_timestamp)::date as event_week
from combined_events
