select
    pack_id,
    pack_name,
    generated_timestamp,
    expiry_timestamp,
    pack_context::jsonb ->> 'location' as location,
    pack_context::jsonb ->> 'featured_cards' as featured_cards_count,
    pack_context::jsonb ->> 'experience_cards' as experience_cards_count,
    total_cards,
    created_at
from {{ source("public", "dextr_packs") }}
