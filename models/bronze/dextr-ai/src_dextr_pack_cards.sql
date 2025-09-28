select
    pack_id,
    coalesce(featured_place_id, card_id) as card_id,
    case when featured_place_id is not null then 1 else 0 end as is_featured,
    card_order,
    shown_to_user,
    user_action,
    created_at
from {{ source("public", "dextr_pack_cards") }}
