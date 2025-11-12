select
    session_id as multiplayer_id,
    participant_id as user_id,
    place_id,
    vote_type,
    voted_at
from {{ source("public", "session_votes") }}
