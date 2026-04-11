-- Phase C regression guard: stg_session_saves_by_surface must preserve
-- total save count across its per-surface pivots. If a save lands in an
-- origin_surface we haven't pivoted on, the pivot columns won't sum to
-- save_count and we'll silently undercount per-surface saves.
--
-- The test returns any session where the pivots don't reconcile.

select
    session_id,
    user_id,
    save_count,
    saves_dextr + saves_featured + saves_search + saves_mydecks
      + saves_shared_link + saves_multiplayer + saves_whats_next
      + saves_place_detail + saves_import + saves_unknown_surface
      + saves_other_surface as pivot_total
from {{ ref('stg_session_saves_by_surface') }}
where save_count <> (
    saves_dextr + saves_featured + saves_search + saves_mydecks
      + saves_shared_link + saves_multiplayer + saves_whats_next
      + saves_place_detail + saves_import + saves_unknown_surface
      + saves_other_surface
)
