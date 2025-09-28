select
    id,
    name,

    types,
    rating,
    price_level,
    website,
    booking_url,
    formatted_phone_number,
    formatted_address,
    is_spotlight,

    location_lat,
    location_lng,
    reservable,
    takeout,
    dine_in,
    serves_beer,
    serves_dinner,
    serves_lunch,
    serves_wine,
    serves_brunch,
    serves_vegetarian_food,
    wheelchair_accessible_entrance,
    serves_breakfast,
    created_at,
    updated_at
from {{ source("public", "featured_cards") }}
