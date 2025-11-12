select board_id, place_id, created_at from {{ source("public", "board_places") }}
