select
    id,
    user_id,
    started_at,
    ended_at,
    session_status,
    initiation_surface,
    initiation_source_id,
    device_type,
    app_version
from {{ source('public', 'planning_sessions') }}
