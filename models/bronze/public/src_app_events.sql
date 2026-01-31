select
    id,
    event_name,
    event_timestamp,
    user_id,
    session_id,
    card_id,
    pack_id,
    board_id,
    share_link_id,
    properties,
    client_event_id
from {{ source('public', 'app_events') }}
