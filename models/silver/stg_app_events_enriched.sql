with deduplicated_events as (
    select distinct on (coalesce(client_event_id, id::text))
        *
    from {{ ref('src_app_events') }}
    where event_timestamp is not null
      and user_id is not null
    order by coalesce(client_event_id, id::text), event_timestamp desc
),

events_with_gaps as (
    select
        *,
        lag(event_timestamp) over (
            partition by user_id
            order by event_timestamp
        ) as prev_event_timestamp,
        extract(epoch from (
            event_timestamp - lag(event_timestamp) over (
                partition by user_id order by event_timestamp
            )
        )) / 60.0 as minutes_since_prev_event
    from deduplicated_events
),

events_with_session_breaks as (
    select
        *,
        case
            when minutes_since_prev_event is null then 1
            when minutes_since_prev_event > 5 then 1
            else 0
        end as is_session_start
    from events_with_gaps
),

events_with_inferred_session as (
    select
        *,
        sum(is_session_start) over (
            partition by user_id
            order by event_timestamp
            rows unbounded preceding
        ) as inferred_session_number,
        md5(
            user_id::text || '_' ||
            sum(is_session_start) over (
                partition by user_id
                order by event_timestamp
                rows unbounded preceding
            )::text
        )::uuid as inferred_session_id
    from events_with_session_breaks
),

enriched as (
    select
        e.id,
        e.event_name,
        e.event_timestamp,
        e.user_id,
        coalesce(e.session_id, e.inferred_session_id) as effective_session_id,
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
        ps.started_at as session_started_at
    from events_with_inferred_session e
    left join {{ ref('stg_planning_sessions') }} ps
        on e.session_id = ps.session_id
)

select * from enriched
