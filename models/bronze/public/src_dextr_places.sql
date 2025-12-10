select 
    id, 
    place_id, 
    pack_id, 
    user_action,
    created_at, 
    updated_at, 
    place_deck_sku
from {{source('public', 'dextr_places')}}