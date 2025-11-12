select
    id,
    name,
    description,
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
    updated_at,
    
    -- Check each category individually
    CASE WHEN description ILIKE ANY (ARRAY['% hike %','% trail %','% climb %','% climbing %','% boulder %','% go ape %','% zip %','% outdoor %','% park %','% adventure %','% cycling%','% bike %','% kayak %','% canoe %','% surf %','% rafting %','% go-kart %','% paintball %','% farm %','% zoo %','% wildlife %','% treetop %']) THEN true ELSE false END as is_adventure,
    
    CASE WHEN description ILIKE ANY (ARRAY['% museum %','% gallery %','% art %','% historic %','% heritage %','% landmark %','% monument%','% temple %','% cathedral %','% church %','% festival %','% exhibition %','% concert hall %','% opera %','% cultural %']) THEN true ELSE false END as is_culture,
    
    CASE WHEN description ILIKE ANY (ARRAY['% restauran t%','% eatery %','% brunch %','% breakfast %','% lunch %','% dinner%','% cafe %','% café %','% coffee %','% bakery %','% doughnut %','% patisserie %','% pizza %','% burger %','% sushi %','% bistro %','% food %','% fine dining %']) THEN true ELSE false END as is_dining,
    
    CASE WHEN description ILIKE ANY (ARRAY['% cinema %','% movie %','%bowling%','% arcade %','% karaoke %','% live music%','% comedy %','% jazz %','% club %','% theatre %','% theater%','% show %','% performance %','% event %','% party %','% nightclub %','% escape room %']) THEN true ELSE false END as is_entertainment,
    
    CASE WHEN description ILIKE ANY (ARRAY['% gym%','% yoga %','% wellness%','% spa %','% pilates%','% fitness %','% clinic %','% massage %','% meditation %','% sauna %','% therapy %','% rehab %','% holistic %']) THEN true ELSE false END as is_health,
    
    CASE WHEN description ILIKE ANY (ARRAY['% bar %','% pub %','% brewery %','% taproom %','% wine %','% cocktail %','% speakeasy %','% gin %','% beer %','% drinks %','% whisky %','% rum %','% vodka %','% tasting %']) THEN true ELSE false END as is_drinks 

from {{ source("public", "featured_cards") }}
