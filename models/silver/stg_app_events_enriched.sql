with deduplicated_events as (
    select distinct on (coalesce(client_event_id, id::text))
        *
    from {{ ref('src_app_events') }}
    where event_timestamp is not null
      and user_id is not null
    order by coalesce(client_event_id, id::text), event_timestamp desc
),

enriched as (
    select
        e.id,
        e.event_name,
        e.event_timestamp,
        e.user_id,
        coalesce(e.session_id, us.session_id) as effective_session_id,
        e.session_id is not null as has_native_session_id,
        e.session_id is null as is_inferred_session,
        case when e.session_id is not null then 'native' else 'inferred' end as data_source,
        e.card_id,
        e.pack_id,
        e.board_id,
        e.share_link_id,
        e.properties,
        e.client_event_id,

        -- Session context from planning_sessions (if native)
        ps.initiation_surface,
        ps.device_type,
        coalesce(ps.started_at, us.started_at) as session_started_at,

        -- Unified session context
        us.initiation_surface as unified_initiation_surface,
        coalesce(
            us.is_genuine_planning_attempt,
            (ps.session_id is not null or ps.initiation_surface = 'dextr')
        ) as is_genuine_planning_attempt

    from deduplicated_events e
    left join {{ ref('stg_planning_sessions') }} ps
        on e.session_id = ps.session_id
    left join {{ ref('stg_unified_sessions') }} us
        on e.session_id is null
        and e.user_id = us.user_id
        and us.session_source = 'inferred'
        and e.event_timestamp >= us.started_at
        and e.event_timestamp <= us.ended_at
)

select * from enriched
