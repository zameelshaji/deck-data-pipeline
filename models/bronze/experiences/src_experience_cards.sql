select 
    card_id,
    name,
    category,
    rating,
    price_level,
    location_lat,
    location_lng,
    formatted_address,

    business_status,
    opening_hours,

    serves_vegetarian_food,
    wheelchair_accessible_entrance,
    reservable,
    takeout,
    dine_in,
    serves_beer,
    serves_dinner,
    serves_lunch,
    serves_wine,
    serves_brunch,
    serves_breakfast,
    source_type,
    source_id,
    google_place_data,
    card_metadata,
    created_at,
    user_ratings_total,
    types
from
    {{ source('public', 'experience_cards') }}