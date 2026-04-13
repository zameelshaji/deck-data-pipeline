select
    id,
    pack_id,
    batch_number,
    was_refined,
    place_ids,
    refinement_scores,
    centroid_place_ids,
    candidates_remaining,
    latency_ms,
    created_at
from {{ source('public', 'dextr_refine_log') }}
