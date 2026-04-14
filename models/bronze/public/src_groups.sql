select
    id,
    name,
    description,
    avatar_url,
    creator_id,
    invite_code,
    member_count,
    created_at,
    updated_at
from {{ source('public', 'groups') }}
