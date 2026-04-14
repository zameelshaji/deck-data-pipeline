select
    id,
    group_id,
    invited_by,
    invited_user_id,
    status,
    created_at,
    expires_at
from {{ source('public', 'group_invites') }}
