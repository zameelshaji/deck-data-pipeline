-- Per-session navigation / view aggregate.
--
-- Captures the "browsing within a session" signals that aren't saves,
-- swipes, or shares: card views, board views, place detail opens,
-- What's Next taps, and onboarding-to-session transitions.
--
-- Downstream uses:
--   - fct_session_outcomes — adds has_whats_next_origin, card_view_count columns
--   - fct_session_explorer — timeline of navigation events per session
--   - fct_surface_performance — denominator for save/share rates per surface

with nav_events as (
    select
        effective_session_id as session_id,
        user_id,
        event_name,
        event_timestamp as nav_at,
        origin_surface,
        action_type as whats_next_action_type
    from {{ ref('stg_app_events_enriched') }}
    where event_name in (
        'card_viewed',
        'board_viewed',
        'place_detail_view_open',
        'whats_next_tapped'
    )
    and effective_session_id is not null
),

session_navigation as (
    select
        session_id,
        user_id,
        count(*) filter (where event_name = 'card_viewed') as card_view_count,
        count(*) filter (where event_name = 'board_viewed') as board_view_count,
        count(*) filter (where event_name = 'place_detail_view_open') as place_detail_view_count,
        count(*) filter (where event_name = 'whats_next_tapped') as whats_next_tap_count,
        -- Unique cards viewed (denominator for save rate calculations)
        count(distinct case when event_name in ('card_viewed', 'place_detail_view_open')
                            then session_id || ':' || user_id || ':' || coalesce(origin_surface, '') end) as unique_views,
        min(nav_at) filter (where event_name = 'card_viewed') as first_card_view_at,
        min(nav_at) filter (where event_name = 'whats_next_tapped') as first_whats_next_at,
        array_agg(distinct whats_next_action_type) filter (where whats_next_action_type is not null) as whats_next_action_types
    from nav_events
    group by session_id, user_id
)

select * from session_navigation
