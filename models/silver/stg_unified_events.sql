{{ config(materialized='table') }}

-- CTE 1: Queries from dextr_queries (legacy direct-insert path).
--
-- Era handoff for 'query' events:
--   - Before 2026-02-05: legacy is the only source (telemetry
--     app_events.dextr_query_submitted was rolling out Jan 30 – Feb 4 with
--     partial coverage, e.g. Feb 1 had 23 legacy vs 3 telemetry).
--   - From 2026-02-05 onward: telemetry is source of truth and is emitted
--     alongside the legacy insert from the iOS client. Counting both would
--     double the prompt count (verified: Apr 12 2026 had 83 legacy == 83
--     telemetry, landing the dashboard on 152 vs the true ~83).
--
-- Telemetry-era queries come through the telemetry_events CTE below,
-- sourced from app_events.dextr_query_submitted. Deprecating the legacy
-- write path from the iOS client is an open follow-up.
with queries as (
    select
        user_id,
        query_timestamp as event_timestamp,
        'query' as event_type,
        null::text as card_id,
        response_pack_id::text as pack_id,
        'dextr_queries' as source_table,
        case
            when query_timestamp < '2025-11-20'::timestamptz then 'card_system'
            when query_timestamp < '2026-01-30'::timestamptz then 'places_system'
            else 'telemetry'
        end as data_era,
        app_version,
        'dextr'::text as origin_surface,
        response_pack_id::text as origin_source_id
    from {{ ref('src_dextr_queries') }}
    where user_id is not null
      and query_timestamp is not null
      and query_timestamp < '2026-02-05'::timestamptz
),

-- CTE 2: Swipes from dextr_pack_cards (card system era, before Nov 20 2025)
swipes_legacy as (
    select
        dq.user_id,
        dpc.created_at as event_timestamp,
        case
            when dpc.user_action = 'right' then 'swipe_right'
            when dpc.user_action = 'left' then 'swipe_left'
            else dpc.user_action
        end as event_type,
        dpc.card_id::text as card_id,
        dpc.pack_id::text as pack_id,
        'dextr_pack_cards' as source_table,
        'card_system' as data_era,
        dq.app_version,
        'dextr'::text as origin_surface,
        dpc.pack_id::text as origin_source_id
    from {{ ref('src_dextr_pack_cards') }} dpc
    inner join {{ ref('src_dextr_queries') }} dq
        on dpc.pack_id = dq.response_pack_id
    where dpc.created_at < '2025-11-20'::timestamptz
      and dpc.user_action in ('right', 'left')
      and dq.user_id is not null
      and dpc.created_at is not null
),

-- CTE 3: Swipes from dextr_places (places_system era only: Nov 20 2025 – Jan 29 2026)
-- Telemetry-era swipes (Jan 30 2026+) come through the telemetry_events CTE below,
-- sourced from app_events.card_swiped_{right,left}. See Phase A fix.
swipes_current as (
    select
        dq.user_id,
        dp.created_at as event_timestamp,
        case
            when dp.user_action = 'like' then 'swipe_right'
            when dp.user_action = 'dislike' then 'swipe_left'
            else dp.user_action
        end as event_type,
        dp.place_deck_sku as card_id,
        dp.pack_id::text as pack_id,
        'dextr_places' as source_table,
        'places_system' as data_era,
        dq.app_version,
        'dextr'::text as origin_surface,
        dp.pack_id::text as origin_source_id
    from {{ ref('src_dextr_places') }} dp
    inner join {{ ref('src_dextr_queries') }} dq
        on dp.pack_id = dq.response_pack_id
    where dp.created_at >= '2025-11-20'::timestamptz
      and dp.created_at < '2026-01-30'::timestamptz
      and dp.user_action in ('like', 'dislike')
      and dq.user_id is not null
      and dp.created_at is not null
),

-- CTE 4: Saves from entity tables (before telemetry era)
saves_from_entity_tables as (
    select
        user_id,
        timestamp as event_timestamp,
        'save' as event_type,
        card_id::text as card_id,
        source_id::text as pack_id,
        'core_card_actions' as source_table,
        case
            when timestamp < '2025-11-20'::timestamptz then 'card_system'
            else 'places_system'
        end as data_era,
        null::text as app_version,
        null::text as origin_surface,
        source_id::text as origin_source_id
    from {{ ref('src_core_card_actions') }}
    where action_type = 'saved'
      and timestamp < '2026-01-30'::timestamptz
      and user_id is not null
      and timestamp is not null
),

-- CTE 5: Shares from entity tables (before telemetry era)
shares_from_entity_tables as (
    select
        user_id,
        timestamp as event_timestamp,
        'share' as event_type,
        card_id::text as card_id,
        source_id::text as pack_id,
        'core_card_actions' as source_table,
        case
            when timestamp < '2025-11-20'::timestamptz then 'card_system'
            else 'places_system'
        end as data_era,
        null::text as app_version,
        null::text as origin_surface,
        source_id::text as origin_source_id
    from {{ ref('src_core_card_actions') }}
    where action_type = 'share'
      and timestamp < '2026-01-30'::timestamptz
      and user_id is not null
      and timestamp is not null
),

-- CTE 6: Clicks/conversions from entity tables (before telemetry era)
clicks_from_entity_tables as (
    select
        user_id,
        timestamp as event_timestamp,
        action_type as event_type,
        card_id::text as card_id,
        source_id::text as pack_id,
        'core_card_actions' as source_table,
        case
            when timestamp < '2025-11-20'::timestamptz then 'card_system'
            else 'places_system'
        end as data_era,
        null::text as app_version,
        null::text as origin_surface,
        source_id::text as origin_source_id
    from {{ ref('src_core_card_actions') }}
    where action_type in ('opened_website', 'book_with_deck', 'click_directions', 'click_phone')
      and timestamp < '2026-01-30'::timestamptz
      and user_id is not null
      and timestamp is not null
),

-- CTE 7: Featured section actions (before telemetry era)
featured_actions as (
    select
        user_id,
        action_timestamp as event_timestamp,
        action_type as event_type,
        card_id::text as card_id,
        pack_id::text as pack_id,
        'featured_section_actions' as source_table,
        case
            when action_timestamp < '2025-11-20'::timestamptz then 'card_system'
            else 'places_system'
        end as data_era,
        null::text as app_version,
        'featured'::text as origin_surface,
        pack_id::text as origin_source_id
    from {{ ref('src_featured_section_actions') }}
    where action_timestamp < '2026-01-30'::timestamptz
      and user_id is not null
      and action_timestamp is not null
),

-- CTE 8: Telemetry events (Jan 30 2026+)
-- We join src_planning_sessions (bronze) here rather than stg_planning_sessions
-- to avoid a circular dependency: stg_unified_sessions → stg_unified_events,
-- and stg_app_events_enriched → stg_unified_sessions, so we can't route the
-- telemetry branch through stg_app_events_enriched. Bronze is cycle-free.
telemetry_deduped as (
    select distinct on (coalesce(ae.client_event_id, ae.id::text))
        ae.*,
        ps.initiation_surface as session_initiation_surface
    from {{ ref('src_app_events') }} ae
    left join {{ ref('src_planning_sessions') }} ps
        on ae.session_id = ps.id
    where ae.event_timestamp >= '2026-01-30'::timestamptz
      and ae.user_id is not null
      and ae.event_timestamp is not null
    order by coalesce(ae.client_event_id, ae.id::text), ae.event_timestamp desc
),

telemetry_events as (
    select
        user_id,
        event_timestamp,
        case
            -- Saves (place_saved is a distinct action emitted from older code paths;
            -- verified 211 of 215 occurrences are orphans, not duplicates of card_saved)
            when event_name in ('card_saved', 'place_saved') then 'save'
            when event_name = 'card_unsaved' then 'unsave'
            -- Views
            when event_name = 'card_viewed' then 'card_view'
            when event_name = 'place_detail_view_open' then 'place_detail_view'
            when event_name = 'board_viewed' then 'board_view'
            -- Swipes (iOS emits card_swiped_*; normalize to canonical)
            when event_name in ('card_swiped_right', 'swipe_right') then 'swipe_right'
            when event_name in ('card_swiped_left', 'swipe_left') then 'swipe_left'
            -- Shares (place_share is ~92% duplicate of card_shared; keep distinct
            -- so downstream aggregators can choose to count or filter)
            when event_name = 'card_shared' then 'card_share'
            when event_name = 'deck_shared' then 'deck_share'
            when event_name = 'multiplayer_shared' then 'multiplayer_share'
            when event_name = 'profile_shared' then 'profile_share'
            when event_name = 'place_share' then 'place_share'
            -- Sessions
            when event_name = 'session_started' then 'session_start'
            when event_name = 'session_ended' then 'session_end'
            -- Dextr / AI
            when event_name = 'dextr_query_submitted' then 'query'
            when event_name = 'dextr_results_viewed' then 'results_view'
            when event_name = 'place_mini_dextr' then 'mini_dextr'
            -- Board lifecycle
            when event_name = 'board_created' then 'board_create'
            -- Social graph
            when event_name = 'deck_liked' then 'deck_like'
            when event_name = 'deck_unliked' then 'deck_unlike'
            when event_name = 'user_followed' then 'user_follow'
            when event_name = 'user_unfollowed' then 'user_unfollow'
            -- Multiplayer
            when event_name = 'multiplayer_created' then 'multiplayer_create'
            when event_name = 'multiplayer_joined' then 'multiplayer_join'
            when event_name = 'multiplayer_voted' then 'multiplayer_vote'
            -- Place actions / conversions
            when event_name = 'place_opened_website' then 'opened_website'
            when event_name = 'place_book_with_deck' then 'book_with_deck'
            when event_name = 'place_click_directions' then 'click_directions'
            when event_name = 'place_click_phone' then 'click_phone'
            when event_name = 'place_book_button_click' then 'book_button_click'
            when event_name = 'place_copy_address' then 'copy_address'
            when event_name = 'place_deal_card_tap' then 'deal_card_tap'
            -- Navigation
            when event_name = 'whats_next_tapped' then 'whats_next_tap'
            -- Checklist / first-session UX
            when event_name = 'checklist_viewed' then 'checklist_view'
            when event_name = 'checklist_task_completed' then 'checklist_task_complete'
            when event_name = 'checklist_all_completed' then 'checklist_all_complete'
            when event_name = 'spin_wheel_unlocked' then 'spin_unlock'
            when event_name = 'spin_wheel_rigged_win' then 'spin_win'
            -- Permission prompts
            when event_name = 'post_spin_notification_prompt_shown' then 'notif_prompt_shown'
            when event_name = 'post_spin_notification_enabled' then 'notif_granted'
            when event_name = 'post_spin_notification_skipped' then 'notif_denied'
            when event_name = 'deferred_location_prompt_shown' then 'location_prompt_shown'
            when event_name = 'deferred_location_granted' then 'location_granted'
            when event_name = 'deferred_location_denied' then 'location_denied'
            -- Onboarding v1 + v2 events pass through; the onboarding_% prefix is
            -- used downstream (stg_onboarding_events) for domain-specific modeling
            else event_name
        end as event_type,
        card_id::text as card_id,
        pack_id::text as pack_id,
        'app_events' as source_table,
        'telemetry' as data_era,
        null::text as app_version,
        -- Coalesce iOS's inconsistent origin key into a canonical origin_surface.
        -- Priority: event-level properties.surface > properties.source > session.initiation_surface.
        -- Matches the equivalent logic in stg_app_events_enriched so the two models
        -- produce consistent surface attribution for telemetry-era events.
        coalesce(
            nullif(properties->>'surface', ''),
            nullif(properties->>'source', ''),
            session_initiation_surface
        ) as origin_surface,
        coalesce(
            nullif(properties->>'source_id', ''),
            nullif(properties->>'source_board_id', '')
        ) as origin_source_id
    from telemetry_deduped
),

-- UNION ALL
all_events as (
    select * from queries
    union all
    select * from swipes_legacy
    union all
    select * from swipes_current
    union all
    select * from saves_from_entity_tables
    union all
    select * from shares_from_entity_tables
    union all
    select * from clicks_from_entity_tables
    union all
    select * from featured_actions
    union all
    select * from telemetry_events
)

-- Add event_category and normalized origin_surface in the final wrapping CTE.
--
-- iOS emits ~24 distinct surface values with sub-variants (featured_carousel,
-- featured_spotlight, featured_category, search_tab, search_view, etc). We
-- keep origin_surface_raw for debugging and emit a normalized origin_surface
-- collapsed to the canonical top-level surfaces so gold models and tests
-- have a stable allowlist.
select
    user_id,
    event_timestamp,
    event_type,
    card_id,
    pack_id,
    source_table,
    data_era,
    app_version,
    origin_surface as origin_surface_raw,
    case
        when origin_surface is null then null
        when origin_surface in ('dextr', 'chat_recommendations') then 'dextr'
        when origin_surface in (
            'featured', 'featured_carousel', 'featured_category',
            'featured_spotlight'
        ) then 'featured'
        when origin_surface in ('search', 'search_tab', 'search_view', 'deck_search')
            then 'search'
        when origin_surface in ('mydecks', 'board_detail') then 'mydecks'
        when origin_surface in ('shared_link', 'shared_content', 'single_card')
            then 'shared_link'
        when origin_surface in ('session_voting', 'session_results', 'multiplayer')
            then 'multiplayer'
        when origin_surface in ('place_detail') then 'place_detail'
        when origin_surface in ('review') then 'review'
        when origin_surface in ('whats_next') then 'whats_next'
        when origin_surface in ('import_swipe_cards') then 'import'
        when origin_surface in ('server_cron') then 'server'
        when origin_surface in ('unknown') then 'unknown'
        when origin_surface = 'explore' then 'explore'
        when origin_surface = 'map' then 'map'
        when origin_surface in ('mini_dextr', 'miniDextr') then 'mini_dextr'
        else 'other'
    end as origin_surface,
    origin_source_id,
    case
        -- AI / Dextr
        when event_type in ('query', 'results_view', 'mini_dextr') then 'AI'
        -- Swipes
        when event_type in ('swipe_right', 'swipe_left') then 'Swipe'
        -- Saves
        when event_type in ('save', 'unsave') then 'Save'
        -- Shares
        when event_type in ('card_share', 'deck_share', 'multiplayer_share', 'profile_share', 'place_share', 'share') then 'Share'
        -- Conversions
        when event_type in (
            'opened_website', 'book_with_deck', 'click_directions', 'click_phone',
            'book_button_click', 'copy_address', 'deal_card_tap', 'conversion'
        ) then 'Conversion'
        -- Views
        when event_type in ('card_view', 'place_detail_view', 'board_view') then 'View'
        -- Session lifecycle
        when event_type in ('session_start', 'session_end') then 'Session'
        -- Board lifecycle
        when event_type = 'board_create' then 'Board'
        -- Social graph
        when event_type in ('deck_like', 'deck_unlike', 'user_follow', 'user_unfollow') then 'Social'
        -- Multiplayer
        when event_type in ('multiplayer_create', 'multiplayer_join', 'multiplayer_vote') then 'Multiplayer'
        -- Navigation
        when event_type = 'whats_next_tap' then 'Navigation'
        -- First-session UX (checklist + spin wheel)
        when event_type in (
            'checklist_view', 'checklist_task_complete', 'checklist_all_complete',
            'spin_unlock', 'spin_win'
        ) then 'FirstSession'
        -- Permission prompts
        when event_type in (
            'notif_prompt_shown', 'notif_granted', 'notif_denied',
            'location_prompt_shown', 'location_granted', 'location_denied'
        ) then 'Permission'
        -- Onboarding (catches any onboarding_* that fell through the CASE)
        when event_type like 'onboarding_%' then 'Onboarding'
        else 'Other'
    end as event_category
from all_events
