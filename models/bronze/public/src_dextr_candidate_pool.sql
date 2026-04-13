select
    id,
    pack_id,
    place_id,
    original_rank,
    similarity_score,
    served,
    batch_number,
    created_at
from {{ source('public', 'dextr_candidate_pool') }}
