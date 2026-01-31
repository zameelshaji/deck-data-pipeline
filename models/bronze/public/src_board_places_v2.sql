select 
    board_id, 
    place_id, 
    added_by,
    added_at,
    updated_at,
    session_id
from {{ source("public", "board_places_v2") }}
