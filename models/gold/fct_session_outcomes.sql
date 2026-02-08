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
        query_count as inline_query_count
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

        -- PSR ladder flags
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
        ) as is_genuine_planning_attempt

    from sessions_base s
    inner join {{ ref('stg_users') }} u on s.user_id = u.user_id
    left join native_saves sv on s.session_id = sv.session_id
    left join native_shares sh on s.session_id = sh.session_id
    left join post_share_interactions psi on s.session_id = psi.session_id
    left join prompt_sessions ps on s.session_id = ps.session_id
    left join version_lookup vl on s.session_date between vl.release_date and vl.release_date_end
    -- CRITICAL: Exclude test users
    where u.is_test_user = 0
)

select * from outcomes
