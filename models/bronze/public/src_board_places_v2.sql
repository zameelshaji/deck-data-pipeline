-- Board places v2 with enhanced schema including added_by for collaborative boards
select
    id,
    board_id,
    place_id,
    place_deck_sku,
    added_by,
    notes,
    added_at,
    updated_at
from {{ source("public", "board_places_v2") }}
