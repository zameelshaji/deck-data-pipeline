SELECT 
    session_id as multiplayer_id,
    place_data::jsonb ->> 'id' as card_id,
    order_index,
    created_at
FROM {{ source('public', 'session_places') }}