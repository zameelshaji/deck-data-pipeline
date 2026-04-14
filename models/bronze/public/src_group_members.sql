select
    id,
    group_id,
    user_id,
    role,
    joined_at
from {{ source('public', 'group_members') }}
