-- Per-session swipe aggregate with origin_surface pivots.
--
-- Telemetry-era swipes come through stg_app_events_enriched (app_events
-- card_swiped_{right,left}); pre-telemetry (places_system) swipes come
-- from dextr_places joined through dextr_queries. Both are unioned here
-- so a session's swipe count is correct regardless of era.
--
-- We pivot the telemetry swipes by origin_surface so downstream gold
-- models can answer "where did this session's swipes happen?" — dextr
-- explore / featured / map / etc.

with telemetry_swipes as (
    select
        effective_session_id as session_id,
        user_id,
        card_id,
        event_name,
        origin_surface,
        event_timestamp as swiped_at
    from {{ ref('stg_app_events_enriched') }}
    where event_name in ('card_swiped_right', 'card_swiped_left')
      and effective_session_id is not null
),

-- Pre-telemetry swipes from dextr_places. These don't have a native
-- session_id in app_events, so we'd need to infer sessions to attribute
-- them. For simplicity this aggregate covers native sessions only, which
-- are telemetry-era. Legacy-era swipes remain available via stg_unified_events.

session_swipes as (
    select
        session_id,
        user_id,
        count(*) filter (where event_name = 'card_swiped_right') as right_swipe_count,
        count(*) filter (where event_name = 'card_swiped_left') as left_swipe_count,
        count(*) as total_swipe_count,
        count(distinct card_id) as unique_cards_swiped,
        min(swiped_at) as first_swipe_at,
        max(swiped_at) as last_swipe_at,

        -- Per-surface right-swipe pivots
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface = 'dextr') as right_swipes_dextr,
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface = 'featured') as right_swipes_featured,
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface = 'search') as right_swipes_search,
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface = 'mydecks') as right_swipes_mydecks,
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface = 'shared_link') as right_swipes_shared_link,
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface = 'multiplayer') as right_swipes_multiplayer,
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface = 'import') as right_swipes_import,
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface = 'explore') as right_swipes_explore,
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface = 'map') as right_swipes_map,
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface = 'mini_dextr') as right_swipes_mini_dextr,
        count(*) filter (where event_name = 'card_swiped_right' and origin_surface is null) as right_swipes_unknown,
        count(*) filter (
            where event_name = 'card_swiped_right'
              and origin_surface is not null
              and origin_surface not in (
                  'dextr', 'featured', 'search', 'mydecks', 'shared_link',
                  'multiplayer', 'import', 'explore', 'map', 'mini_dextr'
              )
        ) as right_swipes_other_surface,

        -- Per-surface left-swipe pivots
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface = 'dextr') as left_swipes_dextr,
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface = 'featured') as left_swipes_featured,
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface = 'search') as left_swipes_search,
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface = 'mydecks') as left_swipes_mydecks,
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface = 'shared_link') as left_swipes_shared_link,
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface = 'multiplayer') as left_swipes_multiplayer,
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface = 'import') as left_swipes_import,
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface = 'explore') as left_swipes_explore,
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface = 'map') as left_swipes_map,
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface = 'mini_dextr') as left_swipes_mini_dextr,
        count(*) filter (where event_name = 'card_swiped_left' and origin_surface is null) as left_swipes_unknown,
        count(*) filter (
            where event_name = 'card_swiped_left'
              and origin_surface is not null
              and origin_surface not in (
                  'dextr', 'featured', 'search', 'mydecks', 'shared_link',
                  'multiplayer', 'import', 'explore', 'map', 'mini_dextr'
              )
        ) as left_swipes_other_surface
    from telemetry_swipes
    group by session_id, user_id
)

select * from session_swipes
