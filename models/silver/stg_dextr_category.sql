with
    pack_cards as (
        select 
            pack_id, 
            coalesce(featured_place_id, card_id) as card_id, 
            case when featured_place_id is not null then 1 else 0 end as is_featured, 
            card_order, 
            shown_to_user, 
            user_action, 
            created_at as card_created_at
        from {{ source("public", "dextr_pack_cards") }}
    ),

    queries as (
        select 
            query_id, 
            user_id, 
            query_text, 
            query_timestamp, 
            response_pack_id, 
            processing_time, 
            query_context::jsonb ->> 'location' as location, 
            query_context::jsonb ->> 'app_version' as app_version 
        from {{ source("public", "dextr_queries") }}
    ),

    packs as (
        select
            pack_id,
            pack_name,
            generated_timestamp,
            expiry_timestamp,
            pack_context::jsonb ->> 'location' as pack_location,
            (pack_context::jsonb ->> 'featured_cards')::int as featured_cards_count,
            (pack_context::jsonb ->> 'experience_cards')::int as experience_cards_count,
            total_cards,
            created_at as pack_created_at
        from {{ source("public", "dextr_packs") }}
    ),

    -- Just reference your existing union table
    all_cards as (
        select 
            card_id,
            card_type,
            name,
            category,
            rating,
            price_level,
            location_lat,
            location_lng,
            formatted_address,
            is_adventure,
            is_culture,
            is_dining,
            is_entertainment,
            is_health,
            is_drinks
        from {{ ref("stg_cards") }}  
    ),

    -- Join everything together
    user_pack_cards as (
        select
            q.query_id,
            q.user_id,
            q.query_text,
            q.query_timestamp,
            q.processing_time,
            q.location as query_location,
            q.app_version,
            
            p.pack_id,
            p.pack_name,
            p.generated_timestamp,
            p.expiry_timestamp,
            p.pack_location,
            p.featured_cards_count,
            p.experience_cards_count,
            p.total_cards,
            p.pack_created_at,
            
            pc.card_id,
            pc.is_featured,
            pc.card_order,
            pc.shown_to_user,
            pc.user_action,
            pc.card_created_at,
            
            -- Card details from unified catalog
            c.card_type,
            c.name,
            c.category,
            c.rating,
            c.price_level,
            c.location_lat,
            c.location_lng,
            c.formatted_address,
            c.is_adventure,
            c.is_culture,
            c.is_dining,
            c.is_entertainment,
            c.is_health,
            c.is_drinks
            
        from queries q
        inner join packs p on q.response_pack_id = p.pack_id
        inner join pack_cards pc on p.pack_id = pc.pack_id
        left join all_cards c on pc.card_id = c.card_id
    )

select 
    query_id, 
    user_id, 
    pack_id,
    array_agg(distinct card_id order by card_id) as card_ids,

    -- Category presence 
    bool_or(is_adventure) as has_adventure,
    bool_or(is_culture) as has_culture,
    bool_or(is_dining) as has_dining,
    bool_or(is_entertainment) as has_entertainment,
    bool_or(is_health) as has_health,
    bool_or(is_drinks) as has_drinks,
    
    -- Category counts
    sum(case when is_adventure = true then 1 else 0 end) as adventure_count,
    sum(case when is_culture = true then 1 else 0 end) as culture_count,
    sum(case when is_dining = true then 1 else 0 end) as dining_count,
    sum(case when is_entertainment = true then 1 else 0 end) as entertainment_count,
    sum(case when is_health = true then 1 else 0 end) as health_count,
    sum(case when is_drinks = true then 1 else 0 end) as drinks_count
    
    
from user_pack_cards
group by query_id, user_id, pack_id
order by query_id, pack_id