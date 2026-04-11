with sessions_base as (
    select
        session_id,
        user_id,
        session_date,
        session_week,
        started_at,
        ended_at,
        session_duration_seconds,
        initiation_surface,
        device_type,
        app_version,
        has_native_session_id,
        not has_native_session_id as is_inferred_session,
        session_source as data_source,
        is_genuine_planning_attempt,
        -- Inline counts from unified sessions (used for inferred sessions)
        save_count as inline_save_count,
        share_count as inline_share_count,
        unique_cards_interacted as inline_unique_cards_interacted,
        query_count as inline_query_count,
        swipe_count as inline_swipe_count
    from {{ ref('stg_unified_sessions') }}
),

-- Native session saves (from app_events via stg_session_saves)
native_saves as (
    select session_id, user_id, save_count, first_save_at, last_save_at
    from {{ ref('stg_session_saves') }}
),

-- Native session shares (from app_events via stg_session_shares)
native_shares as (
    select session_id, user_id, share_count, first_share_at, 'high' as share_attribution_confidence
    from {{ ref('stg_session_shares') }}
),

-- Phase D.1: per-surface save pivots (stg_session_saves_by_surface)
surface_saves as (
    select
        session_id,
        user_id,
        saves_dextr, saves_featured, saves_search, saves_mydecks,
        saves_shared_link, saves_multiplayer, saves_whats_next,
        saves_place_detail, saves_import, saves_unknown_surface, saves_other_surface,
        saves_button_tap, saves_long_press, saves_swipe_up,
        saves_board_selector, saves_auto_save,
        primary_save_surface
    from {{ ref('stg_session_saves_by_surface') }}
),

-- Phase D.1: per-session swipe aggregates (telemetry era)
session_swipes as (
    select
        session_id,
        user_id,
        total_swipe_count as native_swipe_count,
        right_swipe_count as native_right_swipe_count,
        left_swipe_count as native_left_swipe_count,
        right_swipes_dextr, right_swipes_featured, right_swipes_search,
        right_swipes_mydecks, right_swipes_multiplayer, right_swipes_import,
        right_swipes_shared_link, right_swipes_unknown, right_swipes_other_surface
    from {{ ref('stg_session_swipes') }}
),

-- Phase D.1: session-level multiplayer, dextr, navigation flags
session_mp as (
    select session_id, user_id,
        (mp_sessions_created + mp_sessions_joined + mp_votes_cast) > 0 as has_multiplayer,
        mp_sessions_created, mp_sessions_joined, mp_votes_cast
    from {{ ref('stg_session_multiplayer') }}
),
session_dextr as (
    select session_id, user_id,
        results_views > 0 as has_dextr_result_view,
        queries_submitted as query_count_native,
        results_views as results_view_count
    from {{ ref('stg_session_dextr') }}
),
session_nav as (
    select session_id, user_id,
        whats_next_tap_count > 0 as has_whats_next_tap,
        card_view_count,
        place_detail_view_count,
        board_view_count,
        whats_next_tap_count
    from {{ ref('stg_session_navigation') }}
),

-- Post-share interactions per session (within 24h, non-sharer)
post_share_interactions as (
    select
        si.session_id,
        count(*) as interaction_count
    from {{ ref('stg_share_interactions_clean') }} si
    where si.time_since_share_minutes <= 1440
    group by si.session_id
),

-- Check if session had a dextr prompt (for native sessions)
prompt_sessions as (
    select distinct effective_session_id as session_id
    from {{ ref('stg_app_events_enriched') }}
    where event_name = 'session_started'
      and initiation_surface = 'dextr'
),

version_lookup as (
    select
        app_version::text as app_version,
        release_date::date as release_date,
        release_date_end::date as release_date_end
    from {{ ref('app_version_releases') }}
),

outcomes as (
    select
        s.session_id,
        s.user_id,
        u.username,
        u.email,
        s.session_date,
        s.session_week,
        s.started_at,
        s.ended_at,
        s.session_duration_seconds,
        s.initiation_surface,
        s.device_type,
        s.app_version,
        coalesce(s.app_version, vl.app_version) as effective_app_version,
        s.has_native_session_id,
        s.is_inferred_session,
        s.data_source,

        -- Outcome flags: use native saves/shares for native sessions, inline counts for inferred
        coalesce(sv.save_count, s.inline_save_count, 0) > 0 as has_save,
        coalesce(sh.share_count, s.inline_share_count, 0) > 0 as has_share,
        coalesce(psi.interaction_count, 0) > 0 as has_post_share_interaction,
        coalesce(sv.save_count, s.inline_save_count, 0) as save_count,
        coalesce(sh.share_count, s.inline_share_count, 0) as share_count,
        coalesce(s.inline_swipe_count, 0) as swipe_count,

        -- PSR ladder flags
        coalesce(s.inline_swipe_count, 0) >= 3 as has_3plus_swipes,
        coalesce(sv.save_count, s.inline_save_count, 0) > 0 as meets_ssr,
        coalesce(sh.share_count, s.inline_share_count, 0) > 0 as meets_shr,
        (coalesce(sv.save_count, s.inline_save_count, 0) > 0 and coalesce(sh.share_count, s.inline_share_count, 0) > 0) as meets_psr_broad,
        (coalesce(sv.save_count, s.inline_save_count, 0) > 0 and coalesce(sh.share_count, s.inline_share_count, 0) > 0 and coalesce(psi.interaction_count, 0) > 0) as meets_psr_strict,
        (coalesce(sv.save_count, s.inline_save_count, 0) = 0 and coalesce(sh.share_count, s.inline_share_count, 0) = 0) as is_no_value_session,

        -- Timing metrics
        case
            when sv.first_save_at is not null and s.started_at is not null
                then extract(epoch from (sv.first_save_at - s.started_at))::integer
            else null
        end as time_to_first_save_seconds,
        case
            when sh.first_share_at is not null and s.started_at is not null
                then extract(epoch from (sh.first_share_at - s.started_at))::integer
            else null
        end as time_to_first_share_seconds,

        -- Intent classification
        case
            when s.initiation_surface in ('dextr', 'search') then 'strong'
            else 'weak'
        end as intent_strength,
        (ps.session_id is not null or s.initiation_surface = 'dextr' or s.inline_query_count > 0) as is_prompt_session,

        -- Attribution confidence
        coalesce(sh.share_attribution_confidence, case when s.has_native_session_id then 'high' else 'medium' end) as share_attribution_confidence,

        -- Genuine planning attempt
        coalesce(
            s.is_genuine_planning_attempt,
            (ps.session_id is not null or s.initiation_surface = 'dextr'
             or coalesce(sv.save_count, s.inline_save_count, 0) > 0
             or coalesce(sh.share_count, s.inline_share_count, 0) > 0)
        ) as is_genuine_planning_attempt,

        -- ------------------------------------------------------------
        -- Phase D.1: per-surface save/swipe breakdown + new domain flags
        -- ------------------------------------------------------------
        -- Save breakdowns (NULL for inferred/legacy sessions, 0+ for telemetry natives)
        ss.saves_dextr,
        ss.saves_featured,
        ss.saves_search,
        ss.saves_mydecks,
        ss.saves_shared_link,
        ss.saves_multiplayer,
        ss.saves_whats_next,
        ss.saves_place_detail,
        ss.saves_import,
        ss.saves_unknown_surface,
        ss.saves_other_surface,

        -- Save-method breakdown — how the user physically triggered saves
        ss.saves_button_tap,
        ss.saves_long_press,
        ss.saves_swipe_up,
        ss.saves_board_selector,
        ss.saves_auto_save,

        -- Primary save surface (the surface with the most saves in the session)
        ss.primary_save_surface,

        -- Swipe-by-surface breakdown (telemetry era only; NULL for legacy/inferred)
        ssw.native_swipe_count,
        ssw.right_swipes_dextr,
        ssw.right_swipes_featured,
        ssw.right_swipes_search,
        ssw.right_swipes_mydecks,
        ssw.right_swipes_multiplayer,
        ssw.right_swipes_shared_link,
        ssw.right_swipes_import,
        ssw.right_swipes_unknown,
        ssw.right_swipes_other_surface,

        -- New domain flags (FALSE for sessions without these signals)
        coalesce(smp.has_multiplayer, false) as has_multiplayer,
        coalesce(smp.mp_sessions_created, 0) as mp_sessions_created,
        coalesce(smp.mp_sessions_joined, 0) as mp_sessions_joined,
        coalesce(smp.mp_votes_cast, 0) as mp_votes_cast,

        coalesce(sd.has_dextr_result_view, false) as has_dextr_result_view,
        coalesce(sd.query_count_native, 0) as query_count_native,
        coalesce(sd.results_view_count, 0) as results_view_count,

        coalesce(sn.has_whats_next_tap, false) as has_whats_next_tap,
        coalesce(sn.card_view_count, 0) as card_view_count,
        coalesce(sn.place_detail_view_count, 0) as place_detail_view_count,
        coalesce(sn.board_view_count, 0) as board_view_count,
        coalesce(sn.whats_next_tap_count, 0) as whats_next_tap_count

    from sessions_base s
    inner join {{ ref('stg_users') }} u on s.user_id = u.user_id
    left join native_saves sv on s.session_id = sv.session_id
    left join native_shares sh on s.session_id = sh.session_id
    left join post_share_interactions psi on s.session_id = psi.session_id
    left join prompt_sessions ps on s.session_id = ps.session_id
    left join version_lookup vl on s.session_date between vl.release_date and vl.release_date_end
    left join surface_saves ss on s.session_id = ss.session_id
    left join session_swipes ssw on s.session_id = ssw.session_id
    left join session_mp smp on s.session_id = smp.session_id
    left join session_dextr sd on s.session_id = sd.session_id
    left join session_nav sn on s.session_id = sn.session_id
    -- CRITICAL: Exclude test users
    where u.is_test_user = 0
)

select * from outcomes
