{{
    config(
        materialized='table',
        schema='gold',
        tags=['ml', 'recommendation', 'phase3']
    )
}}

-- =============================================================================
-- Gold Recommendation Training Set
-- One row per (prompt_context, candidate_place, swipe_outcome) tuple
-- Used to train LightGBM pointwise classifier and LambdaRank models
-- =============================================================================

with test_users as (
    select id from {{ ref('test_accounts') }}
),

-- =============================================================================
-- CTE 1: base_swipes — Union current + legacy swipe data
-- =============================================================================
current_swipes as (
    select
        dp.pack_id,
        dp.place_id,
        dp.place_deck_sku,
        dp.user_action,
        dp.created_at as swipe_timestamp,
        'current' as data_source
    from {{ ref('src_dextr_places') }} dp
    where dp.user_action in ('like', 'dislike')
),

legacy_swipes as (
    select
        dpc.pack_id,
        dpc.card_id,
        dpc.is_featured as legacy_is_featured,
        dpc.user_action,
        dpc.created_at as swipe_timestamp
    from {{ ref('src_dextr_pack_cards') }} dpc
    where dpc.user_action in ('like', 'dislike', 'left', 'right')
      and dpc.shown_to_user = true
),

-- Resolve legacy card_ids to place_ids
legacy_swipes_resolved as (
    select
        ls.pack_id,
        coalesce(
            case when ls.legacy_is_featured = 1 then ls.card_id else null end,
            ipr.resolved_place_id
        ) as place_id,
        p.deck_sku as place_deck_sku,
        case
            when ls.user_action = 'right' then 'like'
            when ls.user_action = 'left' then 'dislike'
            else ls.user_action
        end as user_action,
        ls.swipe_timestamp,
        'legacy' as data_source
    from legacy_swipes ls
    left join {{ ref('int_place_resolver') }} ipr
        on ls.card_id::text = ipr.original_card_id
        and ls.legacy_is_featured = 0
    left join {{ ref('src_places') }} p
        on coalesce(
            case when ls.legacy_is_featured = 1 then ls.card_id else null end,
            ipr.resolved_place_id
        ) = p.place_id
),

base_swipes as (
    select pack_id, place_id, place_deck_sku, user_action, swipe_timestamp, data_source
    from current_swipes

    union all

    select pack_id, place_id, place_deck_sku, user_action, swipe_timestamp, data_source
    from legacy_swipes_resolved
    where user_action in ('like', 'dislike')
),

-- =============================================================================
-- CTE 2: swipes_with_context — Join to queries for user_id and prompt text
-- =============================================================================
swipes_with_context as (
    select
        bs.pack_id,
        bs.place_id,
        bs.place_deck_sku,
        bs.user_action,
        bs.swipe_timestamp,
        bs.data_source,
        dq.user_id,
        dq.query_text
    from base_swipes bs
    inner join {{ ref('src_dextr_queries') }} dq
        on bs.pack_id = dq.response_pack_id
    where dq.user_id::text not in (select id from test_users)
),

-- =============================================================================
-- CTE 3: swipes_with_places — Join to places for candidate features
-- =============================================================================
swipes_with_places as (
    select
        sc.pack_id,
        sc.place_id,
        sc.place_deck_sku,
        sc.user_action,
        sc.swipe_timestamp,
        sc.data_source,
        sc.user_id,
        sc.query_text,
        -- Label
        case when sc.user_action = 'like' then 1 else 0 end as label,
        -- Candidate features (with NULL handling)
        coalesce(p.rating, -1) as rating,
        coalesce(p.price_level, -1) as price_level,
        coalesce(ln(p.user_ratings_total + 1), 0) as user_ratings_total,
        raw_p.categories[1] as primary_category,
        coalesce(array_length(raw_p.categories, 1), 0) as num_categories,
        raw_p.types[1] as primary_type,
        coalesce(array_length(raw_p.types, 1), 0) as num_types,
        coalesce(p.reservable::int, 0) as is_reservable,
        coalesce(p.dine_in::int, 0) as is_dine_in,
        coalesce(p.serves_brunch::int, 0) as serves_brunch,
        coalesce(p.serves_beer::int, 0) as serves_beer,
        coalesce(p.serves_wine::int, 0) as serves_wine,
        p.location_lat as latitude,
        p.location_lng as longitude,
        -- Raw values needed for downstream session/interaction features
        p.rating as raw_rating,
        p.price_level as raw_price_level
    from swipes_with_context sc
    left join {{ ref('src_places') }} p
        on sc.place_id = p.place_id
    left join {{ source('public', 'places') }} raw_p
        on sc.place_id = raw_p.id
),

-- =============================================================================
-- CTE 4: place_global_stats — Leak-free historical like rate per place
-- =============================================================================
place_global_stats as (
    select
        place_id,
        count(*) as total_swipes,
        count(case when user_action = 'like' then 1 end) as total_likes
    from swipes_with_context
    where place_id is not null
    group by place_id
),

-- =============================================================================
-- CTE 5: Add all features via window functions
-- Session context features use ONLY preceding rows to prevent future leakage
-- =============================================================================
with_all_features as (
    select
        sp.*,
        -- Historical like rate (leak-free: exclude current row)
        coalesce(
            (pgs.total_likes - sp.label)::numeric
            / nullif(pgs.total_swipes - 1, 0),
            0.5
        ) as historical_like_rate,

        -- === Prompt Context Features ===
        coalesce(char_length(sp.query_text), 0) as prompt_length,
        coalesce(array_length(string_to_array(trim(sp.query_text), ' '), 1), 0) as prompt_word_count,
        least(
            coalesce(array_length(string_to_array(trim(sp.query_text), ' '), 1), 0)::numeric / 5.0,
            1.0
        ) as prompt_specificity_score,
        case
            when lower(sp.query_text) ~ '(restaurant|food|eat|dinner|lunch|breakfast|brunch|cuisine)'
                 and lower(sp.query_text) ~ '(bar|pub|cocktail|drinks|museum|gallery|theatre|show)'
                then 'mixed'
            when lower(sp.query_text) ~ '(restaurant|food|eat|dinner|lunch|breakfast|brunch|cuisine|sushi|pizza|burger|pasta|steak|indian|chinese|thai|mexican|italian|japanese|korean|vietnamese|mediterranean|vegan|vegetarian)'
                then 'dining'
            when lower(sp.query_text) ~ '(bar|pub|cocktail|wine bar|beer|drinks|drinking|rooftop bar|speakeasy|happy hour)'
                then 'drinks'
            when lower(sp.query_text) ~ '(museum|gallery|theatre|theater|show|exhibition|concert|comedy|cinema|bowling|karaoke|escape room|adventure|hiking|climbing|spa|gym|yoga|wellness|park|garden)'
                then 'activity'
            else 'unknown'
        end as extracted_intent,

        -- === Session Context Features (window functions, preceding only) ===
        coalesce(
            count(*) filter (where sp.user_action = 'like')
                over (partition by sp.pack_id order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as likes_so_far,

        coalesce(
            count(*) filter (where sp.user_action = 'dislike')
                over (partition by sp.pack_id order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as dislikes_so_far,

        coalesce(
            count(*)
                over (partition by sp.pack_id order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as cards_seen_count,

        -- Running average rating of liked places so far
        sum(case when sp.user_action = 'like' then sp.raw_rating else 0 end)
            over (partition by sp.pack_id order by sp.swipe_timestamp
                  rows between unbounded preceding and 1 preceding)
        / nullif(
            count(*) filter (where sp.user_action = 'like' and sp.raw_rating is not null)
                over (partition by sp.pack_id order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as avg_liked_rating,

        -- Running average price of liked places so far
        sum(case when sp.user_action = 'like' and sp.raw_price_level is not null then sp.raw_price_level else 0 end)
            over (partition by sp.pack_id order by sp.swipe_timestamp
                  rows between unbounded preceding and 1 preceding)
        / nullif(
            count(*) filter (where sp.user_action = 'like' and sp.raw_price_level is not null)
                over (partition by sp.pack_id order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as avg_liked_price,

        -- Running centroid lat/lng of liked places (for Haversine distance)
        sum(case when sp.user_action = 'like' and sp.latitude is not null then sp.latitude else 0 end)
            over (partition by sp.pack_id order by sp.swipe_timestamp
                  rows between unbounded preceding and 1 preceding)
        / nullif(
            count(*) filter (where sp.user_action = 'like' and sp.latitude is not null)
                over (partition by sp.pack_id order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as centroid_lat,

        sum(case when sp.user_action = 'like' and sp.longitude is not null then sp.longitude else 0 end)
            over (partition by sp.pack_id order by sp.swipe_timestamp
                  rows between unbounded preceding and 1 preceding)
        / nullif(
            count(*) filter (where sp.user_action = 'like' and sp.longitude is not null)
                over (partition by sp.pack_id order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as centroid_lng,

        -- Liked primary categories so far (aggregated as comma-separated string for overlap check)
        string_agg(
            case when sp.user_action = 'like' then sp.primary_category end, '||'
        ) over (partition by sp.pack_id order by sp.swipe_timestamp
                rows between unbounded preceding and 1 preceding
        ) as liked_categories_str

    from swipes_with_places sp
    left join place_global_stats pgs
        on sp.place_id = pgs.place_id
),

-- =============================================================================
-- CTE 6: Final assembly — compute derived interaction features
-- =============================================================================
final_training_set as (
    select
        -- === Identification & Metadata ===
        label,
        pack_id,
        user_id,
        place_deck_sku,
        place_id,
        query_text,
        swipe_timestamp,
        data_source,

        -- === Candidate Features ===
        rating,
        price_level,
        user_ratings_total,
        primary_category,
        num_categories,
        primary_type,
        num_types,
        is_reservable,
        is_dine_in,
        serves_brunch,
        serves_beer,
        serves_wine,
        latitude,
        longitude,
        historical_like_rate,

        -- === Prompt Context Features ===
        prompt_length,
        prompt_word_count,
        prompt_specificity_score,
        extracted_intent,

        -- === Session Context Features ===
        likes_so_far,
        dislikes_so_far,
        case
            when (likes_so_far + dislikes_so_far) > 0
            then likes_so_far::numeric / (likes_so_far + dislikes_so_far)
            else 0.5
        end as like_ratio_so_far,
        cards_seen_count,
        avg_liked_rating,
        avg_liked_price,

        -- === Interaction Features ===
        -- Category overlap: 1 if this place's primary_category appeared in any liked place so far
        case
            when primary_category is not null
                 and liked_categories_str is not null
                 and liked_categories_str like '%' || primary_category || '%'
            then 1
            else 0
        end as category_overlap_with_likes,

        -- Price delta from average liked price
        case
            when raw_price_level is not null and avg_liked_price is not null
            then abs(raw_price_level - avg_liked_price)
            else null
        end as price_delta_from_like_avg,

        -- Distance from liked centroid (Haversine in km)
        case
            when latitude is not null and centroid_lat is not null
            then 6371 * acos(
                least(1.0, greatest(-1.0,
                    cos(radians(latitude)) * cos(radians(centroid_lat)) *
                    cos(radians(centroid_lng) - radians(longitude)) +
                    sin(radians(latitude)) * sin(radians(centroid_lat))
                ))
            )
            else null
        end as distance_from_like_centroid_km

    from with_all_features
)

select * from final_training_set
