select
    id,
    sharer_user_id,
    session_id,
    share_type,
    board_id,
    card_id,
    multiplayer_id,
    share_channel,
    short_code,
    created_at
from {{ source('public', 'share_links') }}
