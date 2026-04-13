-- Per-session save aggregate pivoted by origin_surface AND save_method.
--
-- Answers: "Where did this session's saves come from?" and "How are users
-- actually saving places — long-press, button tap, swipe-up, etc.?"
--
-- Complements stg_session_saves (which only gives a total count per session).
-- Downstream: fct_session_outcomes joins this for per-surface save columns,
-- fct_surface_performance uses it as the main source for save-rate-by-surface.

with saves as (
    select
        effective_session_id as session_id,
        user_id,
        card_id,
        origin_surface,
        save_method,
        event_timestamp as saved_at
    from {{ ref('stg_app_events_enriched') }}
    where event_name in ('card_saved', 'place_saved')
      and effective_session_id is not null
),

session_saves_by_surface as (
    select
        session_id,
        user_id,
        count(*) as save_count,

        -- Per-surface save pivots
        count(*) filter (where origin_surface = 'dextr') as saves_dextr,
        count(*) filter (where origin_surface = 'featured') as saves_featured,
        count(*) filter (where origin_surface = 'search') as saves_search,
        count(*) filter (where origin_surface = 'mydecks') as saves_mydecks,
        count(*) filter (where origin_surface = 'shared_link') as saves_shared_link,
        count(*) filter (where origin_surface = 'multiplayer') as saves_multiplayer,
        count(*) filter (where origin_surface = 'whats_next') as saves_whats_next,
        count(*) filter (where origin_surface = 'place_detail') as saves_place_detail,
        count(*) filter (where origin_surface = 'import') as saves_import,
        count(*) filter (where origin_surface = 'explore') as saves_explore,
        count(*) filter (where origin_surface = 'map') as saves_map,
        count(*) filter (where origin_surface = 'mini_dextr') as saves_mini_dextr,
        count(*) filter (where origin_surface is null) as saves_unknown_surface,
        -- Catch-all for surfaces not explicitly pivoted (review, server,
        -- unknown, other). Keeps save_count = sum(pivots) invariant.
        count(*) filter (where origin_surface is not null and origin_surface not in (
            'dextr', 'featured', 'search', 'mydecks', 'shared_link',
            'multiplayer', 'whats_next', 'place_detail', 'import',
            'explore', 'map', 'mini_dextr'
        )) as saves_other_surface,

        -- Save-method pivots (how the user physically triggered the save)
        count(*) filter (where save_method = 'button_tap') as saves_button_tap,
        count(*) filter (where save_method = 'long_press') as saves_long_press,
        count(*) filter (where save_method = 'swipe_up') as saves_swipe_up,
        count(*) filter (where save_method = 'board_selector') as saves_board_selector,
        count(*) filter (where save_method = 'auto_save') as saves_auto_save,
        count(*) filter (where save_method is null) as saves_unknown_method,

        -- Mode calculations for primary_save_surface are cleaner at a
        -- separate grain; computed in a follow-up CTE below.
        min(saved_at) as first_save_at,
        max(saved_at) as last_save_at
    from saves
    group by session_id, user_id
),

primary_surface as (
    -- "Primary surface" = the surface with the most saves in the session.
    -- Ties broken alphabetically (row_number over count desc then surface asc).
    select session_id, user_id, origin_surface as primary_save_surface
    from (
        select
            session_id,
            user_id,
            origin_surface,
            row_number() over (
                partition by session_id, user_id
                order by count(*) desc, origin_surface asc
            ) as rn
        from saves
        where origin_surface is not null
        group by session_id, user_id, origin_surface
    ) ranked
    where rn = 1
)

select
    s.*,
    p.primary_save_surface
from session_saves_by_surface s
left join primary_surface p using (session_id, user_id)
