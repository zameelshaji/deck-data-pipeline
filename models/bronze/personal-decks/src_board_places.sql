SELECT 
    board_id,
    place_id,
    created_at
FROM {{ source('public', 'board_places') }}