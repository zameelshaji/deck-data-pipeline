-- Phase B.5 regression guard: critical events must carry a non-null
-- origin_surface. These are events for which iOS is expected to always
-- attach a surface property (either directly, or by virtue of flowing
-- through a native session). A NULL here means either:
--   (a) iOS added a new call site that forgot to attach surface, or
--   (b) the properties extraction in stg_app_events_enriched regressed.
--
-- Events NOT checked (legitimately NULL): query, unsave, onboarding_*,
-- multiplayer_*, board_*, checklist_*, spin_*, notif_*, location_*,
-- session_*, whats_next_tap, user_follow, user_unfollow, deck_like,
-- deck_unlike. These don't carry a surface concept.

select event_type, count(*) as null_count
from {{ ref('stg_unified_events') }}
where data_era = 'telemetry'
  and origin_surface is null
  and event_type in (
      'save',
      'swipe_right',
      'swipe_left',
      'card_view',
      'place_detail_view',
      'results_view'
  )
group by 1
having count(*) > 0
