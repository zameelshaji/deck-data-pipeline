select
    id,
    place_id,
    pack_id,
    user_action,
    created_at,
    updated_at,
    place_deck_sku,
    batch_number,
    card_position_in_batch,
    was_refined,
    refinement_score,
    swipe_duration_ms
from {{ source('public', 'dextr_places') }}
