select id, place_id, category_id, created_at
from {{ source("public", "featured_cards_category_mapping") }}
