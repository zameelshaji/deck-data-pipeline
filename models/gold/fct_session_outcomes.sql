with sessions_base as (
    -- Native sessions from planning_sessions table
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
        null::text as app_version,
        true as has_native_session_id,
        false as is_inferred_session,
        'native' as data_source,
        null::boolean as is_genuine_planning_attempt,
        0 as inferred_save_count,
        0 as inferred_share_count,
        0 as inferred_unique_cards_interacted
    from {{ ref('stg_planning_sessions') }}

    union all

    -- Inferred sessions from stg_inferred_sessions
    select
        inferred_session_id as session_id,
        user_id,
        session_date,
        session_week,
        session_started_at as started_at,
        session_ended_at as ended_at,
        session_duration_seconds,
        inferred_initiation_surface as initiation_surface,
        null::text as device_type,
        null::text as app_version,
        false as has_native_session_id,
        true as is_inferred_session,
        'inferred' as data_source,
        is_genuine_planning_attempt,
        save_count as inferred_save_count,
        share_count as inferred_share_count,
        unique_cards_interacted as inferred_unique_cards_interacted
    from {{ ref('stg_inferred_sessions') }}
),

-- Deduplicate: prefer native sessions
sessions_deduped as (
    select distinct on (session_id)
        *
    from sessions_base
    order by session_id, has_native_session_id desc
),

-- Native session saves
all_saves as (
    select session_id, user_id, save_count, unique_cards_saved, first_save_at, last_save_at
    from {{ ref('stg_session_saves') }}
),

-- Native session shares
all_shares as (
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

-- Check if session had a dextr prompt
prompt_sessions as (
    select distinct effective_session_id as session_id
    from {{ ref('stg_app_events_enriched') }}
    where event_name = 'session_started'
      and initiation_surface = 'dextr'
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
        s.has_native_session_id,
        s.is_inferred_session,
        s.data_source,

        -- Outcome flags (native saves/shares from joins, inferred from stg_inferred_sessions)
        coalesce(sv.save_count, s.inferred_save_count, 0) > 0 as has_save,
        coalesce(sh.share_count, s.inferred_share_count, 0) > 0 as has_share,
        coalesce(psi.interaction_count, 0) > 0 as has_post_share_interaction,
        coalesce(sv.save_count, s.inferred_save_count, 0) as save_count,
        coalesce(sv.unique_cards_saved, s.inferred_unique_cards_interacted, 0) as unique_cards_saved,
        coalesce(sh.share_count, s.inferred_share_count, 0) as share_count,

        -- Derived metric flags
        coalesce(sv.save_count, s.inferred_save_count, 0) > 0 as meets_ssr,
        coalesce(sh.share_count, s.inferred_share_count, 0) > 0 as meets_shr,
        (coalesce(sv.save_count, s.inferred_save_count, 0) > 0 and coalesce(sh.share_count, s.inferred_share_count, 0) > 0) as meets_psr_broad,
        (coalesce(sv.save_count, s.inferred_save_count, 0) > 0 and coalesce(sh.share_count, s.inferred_share_count, 0) > 0 and coalesce(psi.interaction_count, 0) > 0) as meets_psr_strict,
        coalesce(sv.unique_cards_saved, s.inferred_unique_cards_interacted, 0) >= 3 as meets_scr3,
        (coalesce(sv.save_count, s.inferred_save_count, 0) = 0 and coalesce(sh.share_count, s.inferred_share_count, 0) = 0) as is_no_value_session,

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
        (ps.session_id is not null or s.initiation_surface = 'dextr') as is_prompt_session,

        -- Attribution confidence
        coalesce(sh.share_attribution_confidence, 'high') as share_attribution_confidence,

        -- Genuine planning attempt
        coalesce(
            s.is_genuine_planning_attempt,
            (ps.session_id is not null or s.initiation_surface = 'dextr'
             or coalesce(sv.save_count, s.inferred_save_count, 0) > 0
             or coalesce(sh.share_count, s.inferred_share_count, 0) > 0)
        ) as is_genuine_planning_attempt

    from sessions_deduped s
    left join {{ ref('stg_users') }} u on s.user_id = u.user_id
    left join all_saves sv on s.session_id = sv.session_id
    left join all_shares sh on s.session_id = sh.session_id
    left join post_share_interactions psi on s.session_id = psi.session_id
    left join prompt_sessions ps on s.session_id = ps.session_id
)

select * from outcomes
