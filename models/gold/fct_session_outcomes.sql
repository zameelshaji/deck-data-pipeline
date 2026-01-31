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
        true as has_native_session_id,
        false as is_inferred_session,
        'native' as data_source
    from {{ ref('stg_planning_sessions') }}

    union all

    -- Inferred sessions from app_events (only those without native session)
    select
        effective_session_id as session_id,
        user_id,
        date(min(event_timestamp)) as session_date,
        date_trunc('week', min(event_timestamp))::date as session_week,
        min(event_timestamp) as started_at,
        max(event_timestamp) as ended_at,
        extract(epoch from (max(event_timestamp) - min(event_timestamp)))::integer as session_duration_seconds,
        max(initiation_surface) as initiation_surface,
        max(device_type) as device_type,
        false as has_native_session_id,
        true as is_inferred_session,
        'inferred' as data_source
    from {{ ref('stg_app_events_enriched') }}
    where is_inferred_session = true
    group by effective_session_id, user_id
),

-- Deduplicate: prefer native sessions
sessions_deduped as (
    select distinct on (session_id)
        *
    from sessions_base
    order by session_id, has_native_session_id desc
),

-- Post-share interactions per session (within 24h, non-sharer)
post_share_interactions as (
    select
        si.session_id,
        count(*) as interaction_count
    from {{ ref('stg_share_interactions_clean') }} si
    where si.time_since_share_minutes <= 1440  -- 24 hours
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
        s.session_date,
        s.session_week,
        s.started_at,
        s.ended_at,
        s.session_duration_seconds,
        s.initiation_surface,
        s.device_type,
        s.has_native_session_id,
        s.is_inferred_session,
        s.data_source,

        -- Outcome flags
        coalesce(sv.save_count, 0) > 0 as has_save,
        coalesce(sh.share_count, 0) > 0 as has_share,
        coalesce(psi.interaction_count, 0) > 0 as has_post_share_interaction,
        coalesce(sv.save_count, 0) as save_count,
        coalesce(sv.unique_cards_saved, 0) as unique_cards_saved,
        coalesce(sh.share_count, 0) as share_count,

        -- Derived metric flags
        coalesce(sv.save_count, 0) > 0 as meets_ssr,
        coalesce(sh.share_count, 0) > 0 as meets_shr,
        (coalesce(sv.save_count, 0) > 0 and coalesce(sh.share_count, 0) > 0) as meets_psr_broad,
        (coalesce(sv.save_count, 0) > 0 and coalesce(sh.share_count, 0) > 0 and coalesce(psi.interaction_count, 0) > 0) as meets_psr_strict,
        coalesce(sv.unique_cards_saved, 0) >= 3 as meets_scr3,
        (coalesce(sv.save_count, 0) = 0 and coalesce(sh.share_count, 0) = 0) as is_no_value_session,

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
        coalesce(ps.session_id is not null, s.initiation_surface = 'dextr') as is_prompt_session

    from sessions_deduped s
    left join {{ ref('stg_session_saves') }} sv on s.session_id = sv.session_id
    left join {{ ref('stg_session_shares') }} sh on s.session_id = sh.session_id
    left join post_share_interactions psi on s.session_id = psi.session_id
    left join prompt_sessions ps on s.session_id = ps.session_id
)

select * from outcomes
