{{
    config(
        materialized='table',
        schema='gold',
        tags=['ml', 'recommendation', 'phase3']
    )
}}

-- =============================================================================
-- Gold Recommendation Training Set (v2 — Multi-Surface)
-- One row per (context, candidate_place, swipe_outcome) tuple
-- Sources: Dextr AI packs, explore/featured, shared decks, board review
-- Used to train LightGBM pointwise classifier and LambdaRank models
-- =============================================================================

with test_users as (
    select id from {{ ref('test_accounts') }}
),

-- =============================================================================
-- CTE 1: Dextr swipes from dextr_places (Nov 20 2025+, including telemetry era)
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

-- =============================================================================
-- CTE 2: Legacy swipes from dextr_pack_cards (pre-Nov 20 2025)
-- =============================================================================
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

-- =============================================================================
-- CTE 3: Dextr swipes with context (from dextr_queries for user_id + query_text)
-- =============================================================================
dextr_base_swipes as (
    select pack_id, place_id, place_deck_sku, user_action, swipe_timestamp, data_source
    from current_swipes

    union all

    select pack_id, place_id, place_deck_sku, user_action, swipe_timestamp, data_source
    from legacy_swipes_resolved
    where user_action in ('like', 'dislike')
),

dextr_swipes_with_context as (
    select
        bs.pack_id,
        bs.place_id,
        bs.place_deck_sku,
        bs.user_action,
        bs.swipe_timestamp,
        bs.data_source,
        dq.user_id::text as user_id,
        dq.query_text,
        'dextr' as origin_surface,
        bs.pack_id::text as group_key
    from dextr_base_swipes bs
    inner join {{ ref('src_dextr_queries') }} dq
        on bs.pack_id = dq.response_pack_id
    where dq.user_id::text not in (select id from test_users)
),

-- =============================================================================
-- CTE 4: Non-Dextr telemetry swipes (explore, shared decks, board review, etc.)
-- These go through app_events but NOT dextr_places (no pack_id association)
-- =============================================================================
non_dextr_swipes_deduped as (
    select distinct on (coalesce(ae.client_event_id, ae.id::text))
        ae.id,
        ae.card_id,
        ae.event_name,
        ae.event_timestamp,
        ae.user_id,
        ae.session_id,
        ae.properties
    from {{ ref('src_app_events') }} ae
    where ae.event_name in ('card_swiped_right', 'card_swiped_left')
      and ae.event_timestamp >= '2026-01-30'::timestamptz
      and ae.card_id is not null
      and ae.user_id is not null
      and (ae.pack_id is null or ae.pack_id = '0')  -- Non-Dextr swipes only
    order by coalesce(ae.client_event_id, ae.id::text), ae.event_timestamp desc
),

non_dextr_swipes_with_context as (
    select
        null::int as pack_id,
        ae.card_id::int as place_id,
        null::text as place_deck_sku,
        case when ae.event_name = 'card_swiped_right' then 'like' else 'dislike' end as user_action,
        ae.event_timestamp as swipe_timestamp,
        'telemetry' as data_source,
        ae.user_id::text as user_id,
        null::text as query_text,
        -- Normalize surface using same logic as stg_unified_events
        case
            when coalesce(nullif(ae.properties->>'surface', ''), nullif(ae.properties->>'source', ''))
                in ('featured', 'featured_carousel', 'featured_category', 'featured_spotlight')
                then 'featured'
            when coalesce(nullif(ae.properties->>'surface', ''), nullif(ae.properties->>'source', ''))
                in ('shared_link', 'shared_content', 'single_card')
                then 'shared_link'
            when coalesce(nullif(ae.properties->>'surface', ''), nullif(ae.properties->>'source', ''))
                in ('mydecks', 'board_detail')
                then 'mydecks'
            when coalesce(nullif(ae.properties->>'surface', ''), nullif(ae.properties->>'source', ''))
                in ('search', 'search_tab', 'search_view', 'deck_search')
                then 'search'
            when coalesce(nullif(ae.properties->>'surface', ''), nullif(ae.properties->>'source', ''))
                in ('import_swipe_cards')
                then 'import'
            else 'other'
        end as origin_surface,
        coalesce(ae.session_id::text, 'solo_' || ae.user_id::text) as group_key
    from non_dextr_swipes_deduped ae
    where ae.user_id::text not in (select id from test_users)
),

-- =============================================================================
-- CTE 5: Union all swipes with context
-- =============================================================================
swipes_with_context as (
    select pack_id, place_id, place_deck_sku, user_action, swipe_timestamp, data_source,
           user_id, query_text, origin_surface, group_key
    from dextr_swipes_with_context

    union all

    select pack_id, place_id, place_deck_sku, user_action, swipe_timestamp, data_source,
           user_id, query_text, origin_surface, group_key
    from non_dextr_swipes_with_context
),

-- =============================================================================
-- CTE 6: Join to places for candidate features
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
        sc.origin_surface,
        sc.group_key,
        -- Label
        case when sc.user_action = 'like' then 1 else 0 end as label,
        -- Candidate features (with NULL handling)
        coalesce(p.rating, -1) as rating,
        coalesce(p.price_level, -1) as price_level,
        coalesce(ln(p.user_ratings_total + 1), 0) as user_ratings_total,
        raw_p.categories[1] as primary_category,
        coalesce(array_length(raw_p.categories, 1), 0) as num_categories,
        lower(
            coalesce(
                (select t from unnest(raw_p.types) as t
                 where lower(t) not in ('point_of_interest', 'establishment', 'food',
                                        'store', 'health', 'place_of_worship', 'place')
                 limit 1),
                raw_p.types[1]
            )
        ) as primary_type,
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
-- CTE 7: Place global stats — Leak-free historical like rate per place
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
-- CTE 8: User save stats (from board_places_v2 — all-time aggregates)
-- =============================================================================
user_save_stats as (
    select
        bp.added_by::text as user_id,
        count(*) as user_total_saves,
        avg(p.rating) as user_preferred_rating,
        avg(p.price_level::numeric) as user_preferred_price
    from {{ ref('src_board_places_v2') }} bp
    left join {{ ref('src_places') }} p on bp.place_id = p.place_id
    where bp.added_by is not null
    group by bp.added_by
),

-- =============================================================================
-- CTE 9: User share stats (from app_events — all-time aggregate)
-- =============================================================================
user_share_stats as (
    select
        ae.user_id::text as user_id,
        count(*) as user_total_shares
    from {{ ref('src_app_events') }} ae
    where ae.event_name in ('card_shared', 'deck_shared', 'multiplayer_shared', 'place_share')
      and ae.user_id is not null
    group by ae.user_id
),

-- =============================================================================
-- CTE 10: Place popularity (from board_places_v2 — how many users saved each place)
-- =============================================================================
place_popularity as (
    select
        bp.place_id,
        count(distinct bp.added_by) as place_save_count
    from {{ ref('src_board_places_v2') }} bp
    where bp.place_id is not null
      and bp.added_by is not null
    group by bp.place_id
),

-- =============================================================================
-- CTE 11: Add all features via window functions
-- Session context features use ONLY preceding rows to prevent future leakage
-- Window partitions use group_key (pack_id for dextr, session_id for non-dextr)
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
            -- Mixed: matches 2+ categories (dining+drinks, dining+activity, or drinks+activity)
            when
                (lower(sp.query_text) ~ '(restaurant|food|eat|dinner|lunch|breakfast|brunch|cuisine|sushi|pizza|burger|pasta|steak|indian|chinese|thai|mexican|italian|japanese|korean|vietnamese|mediterranean|vegan|vegetarian|ramen|noodle|noodles|coffee|cafe|café|tea|cake|dessert|desserts|sweet|sweets|roast|hotpot|hot pot|bistro|diner|bakery|patisserie|dim sum|tapas|bbq|grill|seafood|chicken|wings|dumplings|pancakes|waffles|ice cream|gelato|chai|meal)'
                 and lower(sp.query_text) ~ '(bar|pub|cocktail|cocktails|wine bar|beer|drinks|drinking|rooftop bar|speakeasy|happy hour|pint|nightlife|clubbing|club|clubs|boozy|shots|gin|rum|whisky|whiskey|brewery)')
                or
                (lower(sp.query_text) ~ '(restaurant|food|eat|dinner|lunch|breakfast|brunch|cuisine|sushi|pizza|burger|pasta|steak|indian|chinese|thai|mexican|italian|japanese|korean|vietnamese|mediterranean|vegan|vegetarian|ramen|noodle|noodles|coffee|cafe|café|tea|cake|dessert|desserts|sweet|sweets|roast|hotpot|hot pot|bistro|diner|bakery|patisserie|dim sum|tapas|bbq|grill|seafood|chicken|wings|dumplings|pancakes|waffles|ice cream|gelato|chai|meal)'
                 and lower(sp.query_text) ~ '(museum|gallery|theatre|theater|show|exhibition|concert|comedy|cinema|bowling|karaoke|escape room|adventure|hiking|climbing|spa|gym|yoga|wellness|park|garden|zoo|aquarium|pottery|painting|craft|crafts|go kart|go-kart|trampoline|axe throwing|mini golf|crazy golf|skating|ice skating|roller|arcade|laser|paintball|dance|dancing|bachata|salsa|class|classes|workshop|tour|market|markets|fireworks|christmas|carnival|festival|fair|event|events|activities|activity|fun things|things to do|day out|team building)')
                or
                (lower(sp.query_text) ~ '(bar|pub|cocktail|cocktails|wine bar|beer|drinks|drinking|rooftop bar|speakeasy|happy hour|pint|nightlife|clubbing|club|clubs|boozy|shots|gin|rum|whisky|whiskey|brewery)'
                 and lower(sp.query_text) ~ '(museum|gallery|theatre|theater|show|exhibition|concert|comedy|cinema|bowling|karaoke|escape room|adventure|hiking|climbing|spa|gym|yoga|wellness|park|garden|zoo|aquarium|pottery|painting|craft|crafts|go kart|go-kart|trampoline|axe throwing|mini golf|crazy golf|skating|ice skating|roller|arcade|laser|paintball|dance|dancing|bachata|salsa|class|classes|workshop|tour|market|markets|fireworks|christmas|carnival|festival|fair|event|events|activities|activity|fun things|things to do|day out|team building)')
            then 'mixed'
            -- Dining
            when lower(sp.query_text) ~ '(restaurant|food|eat|dinner|lunch|breakfast|brunch|cuisine|sushi|pizza|burger|pasta|steak|indian|chinese|thai|mexican|italian|japanese|korean|vietnamese|mediterranean|vegan|vegetarian|ramen|noodle|noodles|coffee|cafe|café|tea|cake|dessert|desserts|sweet|sweets|roast|hotpot|hot pot|bistro|diner|bakery|patisserie|dim sum|tapas|bbq|grill|seafood|chicken|wings|dumplings|pancakes|waffles|ice cream|gelato|chai|meal)'
            then 'dining'
            -- Drinks
            when lower(sp.query_text) ~ '(bar|pub|cocktail|cocktails|wine bar|beer|drinks|drinking|rooftop bar|speakeasy|happy hour|pint|nightlife|clubbing|club|clubs|boozy|shots|gin|rum|whisky|whiskey|brewery)'
            then 'drinks'
            -- Activity
            when lower(sp.query_text) ~ '(museum|gallery|theatre|theater|show|exhibition|concert|comedy|cinema|bowling|karaoke|escape room|adventure|hiking|climbing|spa|gym|yoga|wellness|park|garden|zoo|aquarium|pottery|painting|craft|crafts|go kart|go-kart|trampoline|axe throwing|mini golf|crazy golf|skating|ice skating|roller|arcade|laser|paintball|dance|dancing|bachata|salsa|class|classes|workshop|tour|market|markets|fireworks|christmas|carnival|festival|fair|event|events|activities|activity|fun things|things to do|day out|team building|nails|hair|beauty|massage)'
            then 'activity'
            else 'unknown'
        end as extracted_intent,

        -- === Session Context Features (window functions, preceding only) ===
        -- Partitioned by group_key (pack_id for dextr, session_id for non-dextr)
        coalesce(
            count(*) filter (where sp.user_action = 'like')
                over (partition by sp.group_key order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as likes_so_far,

        coalesce(
            count(*) filter (where sp.user_action = 'dislike')
                over (partition by sp.group_key order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as dislikes_so_far,

        coalesce(
            count(*)
                over (partition by sp.group_key order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as cards_seen_count,

        -- Running average rating of liked places so far
        sum(case when sp.user_action = 'like' then sp.raw_rating else 0 end)
            over (partition by sp.group_key order by sp.swipe_timestamp
                  rows between unbounded preceding and 1 preceding)
        / nullif(
            count(*) filter (where sp.user_action = 'like' and sp.raw_rating is not null)
                over (partition by sp.group_key order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as avg_liked_rating,

        -- Running average price of liked places so far (use numeric to avoid integer truncation)
        sum(case when sp.user_action = 'like' and sp.raw_price_level is not null then sp.raw_price_level::numeric else 0.0 end)
            over (partition by sp.group_key order by sp.swipe_timestamp
                  rows between unbounded preceding and 1 preceding)
        / nullif(
            count(*) filter (where sp.user_action = 'like' and sp.raw_price_level is not null)
                over (partition by sp.group_key order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as avg_liked_price,

        -- Running centroid lat/lng of liked places (for Haversine distance)
        sum(case when sp.user_action = 'like' and sp.latitude is not null then sp.latitude else 0 end)
            over (partition by sp.group_key order by sp.swipe_timestamp
                  rows between unbounded preceding and 1 preceding)
        / nullif(
            count(*) filter (where sp.user_action = 'like' and sp.latitude is not null)
                over (partition by sp.group_key order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as centroid_lat,

        sum(case when sp.user_action = 'like' and sp.longitude is not null then sp.longitude else 0 end)
            over (partition by sp.group_key order by sp.swipe_timestamp
                  rows between unbounded preceding and 1 preceding)
        / nullif(
            count(*) filter (where sp.user_action = 'like' and sp.longitude is not null)
                over (partition by sp.group_key order by sp.swipe_timestamp
                      rows between unbounded preceding and 1 preceding),
            0
        ) as centroid_lng,

        -- Liked primary categories so far (aggregated as comma-separated string for overlap check)
        string_agg(
            case when sp.user_action = 'like' then sp.primary_category end, '||'
        ) over (partition by sp.group_key order by sp.swipe_timestamp
                rows between unbounded preceding and 1 preceding
        ) as liked_categories_str,

        -- === User-level features (from board saves + shares) ===
        coalesce(uss.user_total_saves, 0) as user_total_saves,
        coalesce(ushs.user_total_shares, 0) as user_total_shares,
        uss.user_preferred_rating,
        uss.user_preferred_price,

        -- === Place-level popularity feature ===
        coalesce(pp.place_save_count, 0) as place_save_count

    from swipes_with_places sp
    left join place_global_stats pgs
        on sp.place_id = pgs.place_id
    left join user_save_stats uss
        on sp.user_id = uss.user_id
    left join user_share_stats ushs
        on sp.user_id = ushs.user_id
    left join place_popularity pp
        on sp.place_id = pp.place_id
),

-- =============================================================================
-- CTE 12: Final assembly — compute derived interaction features
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
        origin_surface,
        group_key,

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

        -- Price delta from average liked price (numeric for decimal precision)
        case
            when raw_price_level is not null and avg_liked_price is not null
            then round(abs(raw_price_level::numeric - avg_liked_price), 2)
            else null
        end as price_delta_from_like_avg,

        -- Distance from liked centroid (Haversine in km, capped at 50km)
        case
            when latitude is not null and centroid_lat is not null
            then least(
                6371 * acos(
                    least(1.0, greatest(-1.0,
                        cos(radians(latitude)) * cos(radians(centroid_lat)) *
                        cos(radians(centroid_lng) - radians(longitude)) +
                        sin(radians(latitude)) * sin(radians(centroid_lat))
                    ))
                ),
                50.0
            )
            else null
        end as distance_from_like_centroid_km,

        -- === User-Level Features ===
        user_total_saves,
        user_total_shares,
        user_preferred_rating,
        user_preferred_price,

        -- === Place-Level Features ===
        place_save_count,

        -- Per-user sample cap: keep first 50 swipes per user to prevent overfitting
        row_number() over (partition by user_id order by swipe_timestamp) as user_row_num

    from with_all_features
)

select * from final_training_set
where user_row_num <= 50
