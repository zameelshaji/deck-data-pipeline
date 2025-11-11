-- Unified content catalog
with
    experience_cards as (
        select
            card_id,
            'experience' as card_type,
            name,
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
            id as card_id,
            'featured' as card_type,
            name,
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
    )

select *
from experience_cards
union all
select *
from featured_cards
