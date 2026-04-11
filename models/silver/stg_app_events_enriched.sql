with deduplicated_events as (
    select distinct on (coalesce(client_event_id, id::text))
        *
    from {{ ref('src_app_events') }}
    where event_timestamp is not null
      and user_id is not null
    order by coalesce(client_event_id, id::text), event_timestamp desc
),

enriched as (
    select
        e.id,
        e.event_name,
        e.event_timestamp,
        e.user_id,
        coalesce(e.session_id, us.session_id) as effective_session_id,
        e.session_id is not null as has_native_session_id,
        e.session_id is null as is_inferred_session,
        case when e.session_id is not null then 'native' else 'inferred' end as data_source,
        e.card_id,
        e.pack_id,
        e.board_id,
        e.share_link_id,
        e.properties,
        e.client_event_id,

        -- Session context from planning_sessions (if native)
        ps.initiation_surface,
        ps.device_type,
        coalesce(ps.started_at, us.started_at) as session_started_at,

        -- Unified session context
        us.initiation_surface as unified_initiation_surface,
        coalesce(
            us.is_genuine_planning_attempt,
            (ps.session_id is not null or ps.initiation_surface = 'dextr')
        ) as is_genuine_planning_attempt,

        -- ------------------------------------------------------------
        -- Tier 1 universal property extraction (Phase B.5)
        -- ------------------------------------------------------------
        -- iOS TelemetryManager is inconsistent about which key carries origin:
        --   * swipes + card_viewed         → properties.source
        --   * place_* + dextr_results      → properties.surface
        --   * card_saved                   → properties.surface
        --   * session_started              → planning_sessions.initiation_surface (top-level)
        -- This coalesces all three into a canonical origin_surface_raw.
        -- Falls back to the session's initiation_surface so events that never
        -- carry their own origin still inherit from their parent session.
        coalesce(
            nullif(e.properties->>'surface', ''),
            nullif(e.properties->>'source', ''),
            ps.initiation_surface,
            us.initiation_surface
        ) as origin_surface_raw,
        coalesce(
            nullif(e.properties->>'source_id', ''),
            nullif(e.properties->>'source_board_id', '')
        ) as origin_source_id,
        nullif(e.properties->>'action_type', '') as action_type,
        nullif(e.properties->>'share_channel', '') as share_channel,
        nullif(e.properties->>'fast_path', '') as fast_path,
        case
            when e.properties->>'is_private' in ('true', 'false')
                then (e.properties->>'is_private')::boolean
        end as is_private,
        case
            when e.properties->>'places_count' ~ '^-?[0-9]+$'
                then (e.properties->>'places_count')::int
        end as places_count,
        nullif(e.properties->>'source_board_id', '') as source_board_id,
        nullif(e.properties->>'multiplayer_id', '') as multiplayer_id_prop,
        nullif(e.properties->>'query_id', '') as query_id_prop,
        nullif(e.properties->>'save_method', '') as save_method,
        nullif(e.properties->>'task_name', '') as task_name

    from deduplicated_events e
    left join {{ ref('stg_planning_sessions') }} ps
        on e.session_id = ps.session_id
    left join {{ ref('stg_unified_sessions') }} us
        on e.session_id is null
        and e.user_id = us.user_id
        and us.session_source = 'inferred'
        and e.event_timestamp >= us.started_at
        and e.event_timestamp <= us.ended_at
)

-- Normalize origin_surface_raw into a canonical set. iOS emits ~24 sub-variants
-- (featured_carousel, featured_spotlight, search_tab, search_view, etc.). We
-- collapse them into the canonical top-level surfaces so gold models and
-- accepted_values tests have a stable allowlist. The raw value is preserved
-- as origin_surface_raw for debugging.
--
-- IMPORTANT: this CASE must stay in sync with stg_unified_events.sql final
-- select — both models expose the same origin_surface column and downstream
-- consumers expect identical semantics.
select
    *,
    case
        when origin_surface_raw is null then null
        when origin_surface_raw in ('dextr', 'chat_recommendations') then 'dextr'
        when origin_surface_raw in (
            'featured', 'featured_carousel', 'featured_category', 'featured_spotlight'
        ) then 'featured'
        when origin_surface_raw in ('search', 'search_tab', 'search_view', 'deck_search')
            then 'search'
        when origin_surface_raw in ('mydecks', 'board_detail') then 'mydecks'
        when origin_surface_raw in ('shared_link', 'shared_content', 'single_card')
            then 'shared_link'
        when origin_surface_raw in ('session_voting', 'session_results', 'multiplayer')
            then 'multiplayer'
        when origin_surface_raw = 'place_detail' then 'place_detail'
        when origin_surface_raw = 'review' then 'review'
        when origin_surface_raw = 'whats_next' then 'whats_next'
        when origin_surface_raw = 'import_swipe_cards' then 'import'
        when origin_surface_raw = 'server_cron' then 'server'
        when origin_surface_raw = 'unknown' then 'unknown'
        else 'other'
    end as origin_surface
from enriched
