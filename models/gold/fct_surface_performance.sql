{{ config(materialized='table') }}

-- Surface Performance — Save/share/PSR metrics sliced by origin_surface
--
-- Grain: one row per (metric_date, origin_surface)
-- Answers: "Which parts of the app actually drive the North Star?"
--          "What's the PSR_broad of dextr-initiated sessions vs mydecks?"
--          "Is Featured converting at the same rate as Search?"
--
-- Scope: telemetry era only (2026-01-30+), since pre-telemetry events
-- don't carry origin_surface consistently. Pre-telemetry per-surface
-- analysis would need backfill work — out of scope here.
--
-- Feeds a new Streamlit dashboard widget on the North Star / Engagement
-- page — bar chart of PSR_broad by surface, plus a save-rate × volume
-- bubble chart showing which surfaces have the best conversion.

with event_base as (
    -- Every telemetry event, keyed by user+day+surface for the denominator
    select
        date(event_timestamp) as metric_date,
        origin_surface,
        user_id,
        effective_session_id as session_id,
        event_name
    from {{ ref('stg_app_events_enriched') }}
    inner join {{ ref('stg_users') }} using (user_id)
    where event_timestamp >= '2026-01-30'::timestamptz
      and is_test_user = 0
      and origin_surface is not null
),

-- Per-(date, surface) event tallies
daily_surface_events as (
    select
        metric_date,
        origin_surface,
        count(*) as total_events,
        count(distinct user_id) as distinct_users,
        count(distinct session_id) as distinct_sessions,
        count(*) filter (where event_name in ('card_saved', 'place_saved')) as saves,
        count(*) filter (where event_name in ('card_shared', 'deck_shared', 'multiplayer_shared', 'profile_shared')) as shares,
        count(*) filter (where event_name in ('card_swiped_right', 'card_swiped_left')) as swipes,
        count(*) filter (where event_name = 'card_swiped_right') as swipe_rights,
        count(*) filter (where event_name in ('card_viewed', 'place_detail_view_open')) as views
    from event_base
    group by metric_date, origin_surface
),

-- Sessions that initiated from each surface, for PSR calculations.
-- A session is attributed to its initiation_surface (not the surface of
-- individual events within it). Gold models already use this convention.
sessions_by_init_surface as (
    select
        so.session_date as metric_date,
        so.initiation_surface as origin_surface,
        count(*) as total_sessions,
        count(*) filter (where so.has_save) as sessions_with_save,
        count(*) filter (where so.has_share) as sessions_with_share,
        count(*) filter (where so.meets_psr_broad) as sessions_with_psr_broad,
        count(*) filter (where so.meets_psr_strict) as sessions_with_psr_strict,
        count(*) filter (where so.has_3plus_swipes) as sessions_with_3plus_swipes
    from {{ ref('fct_session_outcomes') }} so
    where so.session_date >= '2026-01-30'
      and so.has_native_session_id
      and so.initiation_surface is not null
    group by so.session_date, so.initiation_surface
),

-- Sessions that contained at least one event from each surface. This is
-- different from sessions_by_init_surface: a single session can touch
-- multiple surfaces (e.g. starts in dextr, then user navigates to mydecks).
sessions_by_event_surface as (
    select
        metric_date,
        origin_surface,
        count(distinct session_id) filter (where session_id is not null) as sessions_touching_surface
    from event_base
    group by metric_date, origin_surface
)

select
    coalesce(e.metric_date, s.metric_date) as metric_date,
    coalesce(e.origin_surface, s.origin_surface) as origin_surface,

    -- Event-level
    coalesce(e.total_events, 0) as total_events,
    coalesce(e.distinct_users, 0) as distinct_users,
    coalesce(e.distinct_sessions, 0) as distinct_sessions,
    coalesce(e.saves, 0) as saves,
    coalesce(e.shares, 0) as shares,
    coalesce(e.swipes, 0) as swipes,
    coalesce(e.swipe_rights, 0) as swipe_rights,
    coalesce(e.views, 0) as views,

    -- Sessions where THIS surface was the initiation surface
    coalesce(s.total_sessions, 0) as initiated_sessions,
    coalesce(s.sessions_with_save, 0) as initiated_sessions_with_save,
    coalesce(s.sessions_with_share, 0) as initiated_sessions_with_share,
    coalesce(s.sessions_with_psr_broad, 0) as initiated_sessions_with_psr_broad,
    coalesce(s.sessions_with_psr_strict, 0) as initiated_sessions_with_psr_strict,
    coalesce(s.sessions_with_3plus_swipes, 0) as initiated_sessions_with_3plus_swipes,

    -- Sessions that touched THIS surface at least once (multi-surface sessions counted per surface)
    coalesce(t.sessions_touching_surface, 0) as sessions_touching_surface,

    -- Rates (NULL when denominator is 0 to avoid misleading zeros)
    case when coalesce(s.total_sessions, 0) > 0
         then s.sessions_with_save::numeric / s.total_sessions end as ssr_initiated,
    case when coalesce(s.total_sessions, 0) > 0
         then s.sessions_with_share::numeric / s.total_sessions end as shr_initiated,
    case when coalesce(s.total_sessions, 0) > 0
         then s.sessions_with_psr_broad::numeric / s.total_sessions end as psr_broad_initiated,
    case when coalesce(s.total_sessions, 0) > 0
         then s.sessions_with_psr_strict::numeric / s.total_sessions end as psr_strict_initiated,

    -- Swipe-right rate per surface (numerator = right swipes, denominator = all swipes on that surface)
    case when coalesce(e.swipes, 0) > 0
         then e.swipe_rights::numeric / e.swipes end as right_swipe_rate,

    -- Saves-per-view (engagement depth per surface)
    case when coalesce(e.views, 0) > 0
         then e.saves::numeric / e.views end as save_per_view_rate

from daily_surface_events e
full outer join sessions_by_init_surface s
    on e.metric_date = s.metric_date
   and e.origin_surface = s.origin_surface
left join sessions_by_event_surface t
    on coalesce(e.metric_date, s.metric_date) = t.metric_date
   and coalesce(e.origin_surface, s.origin_surface) = t.origin_surface
