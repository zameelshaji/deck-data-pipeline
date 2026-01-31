select
    id,
    share_link_id,
    viewer_user_id,
    viewer_anon_id,
    interaction_type,
    interaction_timestamp,
    is_sharer
from {{ source('public', 'share_interactions') }}
