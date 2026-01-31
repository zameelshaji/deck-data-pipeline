with sessions as (
    select
        id as session_id,
        user_id,
        started_at,
        ended_at,
        session_status,
        initiation_surface,
        initiation_source_id,
        device_type,
        app_version,
        date(started_at) as session_date,
        date_trunc('week', started_at)::date as session_week,

        -- Computed fields
        case
            when ended_at is not null
                then extract(epoch from (ended_at - started_at))::integer
            else null
        end as session_duration_seconds,

        case
            when session_status = 'completed' then true
            when ended_at is not null then true
            else false
        end as is_completed

    from {{ ref('src_planning_sessions') }}
    where started_at is not null
      and user_id is not null
)

select * from sessions
