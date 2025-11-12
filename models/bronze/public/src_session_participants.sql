select session_id as multiplayer_id, is_creator, joined_at, last_active, user_id
from {{ source("public", "session_participants") }}
