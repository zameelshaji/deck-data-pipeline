-- Unified content catalog
with
    experience_cards as (
        select
            card_id::text,
            'experience' as card_type,
            0::boolean as is_featured,
            name::text,
            category,
            rating,
            price_level,
            location_lat,
            location_lng,
            formatted_address,
            source_type,
            created_at,
            business_status,
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
            user_ratings_total,
            types,
            is_adventure,
            is_culture,
            is_dining,
            is_entertainment,
            is_health,
            is_drinks

        from {{ ref("src_experience_cards") }}
    ),

    featured_cards as (
        select
            id::text as card_id,
            'featured' as card_type,
            1::boolean as is_featured,
            name::text,
            null as category,
            rating,
            price_level,
            location_lat,
            location_lng,
            formatted_address,
            'partnership' as source_type,
            created_at,
            -- Business attributes
            null as business_status,
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
            null::int as user_ratings_total,
            types,
            is_adventure,
            is_culture,
            is_dining,
            is_entertainment,
            is_health,
            is_drinks

        from {{ ref("src_featured_cards") }}
    ), 

    
    places as (
    select
        deck_sku as card_id,  -- text
        'post-gemini' as card_type,  -- ✅ Added 'as'
        is_featured,
        name,  -- text
        null::character varying as category,  -- match experience_cards type
        rating,  -- numeric(3,2) → compatible with numeric
        price_level,  -- integer
        location_lat,  -- numeric (already cast in src_places)
        location_lng,  -- numeric (already cast in src_places)
        formatted_address,  -- text
        source_type,  -- text
        created_at,  -- timestamp with time zone
        business_status,  -- text
        serves_vegetarian_food,  -- boolean
        wheelchair_accessible_entrance,  -- boolean
        reservable,  -- boolean
        takeout,  -- boolean
        dine_in,  -- boolean
        serves_beer,  -- boolean
        serves_dinner,  -- boolean
        serves_lunch,  -- boolean
        serves_wine,  -- boolean
        serves_brunch,  -- boolean
        serves_breakfast,  -- boolean
        user_ratings_total,  -- integer
        types,  -- text[]
        -- Category flags based on categories array
        'Adventure' = ANY(categories) as is_adventure,
        'Culture' = ANY(categories) as is_culture,
        'Dining' = ANY(categories) as is_dining,
        'Entertainment' = ANY(categories) as is_entertainment,
        'Health' = ANY(categories) as is_health,
        'Drinks' = ANY(categories) as is_drinks
    from {{ ref("src_places") }}
)

select *
from experience_cards
union all
select *
from featured_cards
union all
select *
from places