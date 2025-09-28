select
    id as multiplayer_id,
    creator_id,
    title,
    status,
    max_participants,
    created_at,
    updated_at,
    expires_at,
    source_type,
    source_board_id,
    ai_prompt
from {{ source("public", "multiplayer_sessions") }}
