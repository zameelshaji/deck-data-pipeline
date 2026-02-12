{{ config(materialized='table') }}

-- Place/Card Performance
-- Grain: one row per place (card_id from stg_cards, with resolved_place_id from int_place_resolver)
-- Answers: "Which packs have highest save rate?", "Which cards get shown but never saved?",
--          "Are restaurants or bars performing better?", "Cards with viral potential?"
--
-- Uses int_place_resolver to map card_id → places.place_id for enriched metadata

with card_events as (
    select
        e.card_id,
        pr.resolved_place_id,
        e.user_id,
        e.event_type,
        e.event_category,
        e.event_timestamp,
        e.data_era
    from {{ ref('stg_unified_events') }} e
    inner join {{ ref('stg_users') }} u on e.user_id = u.user_id
    left join {{ ref('int_place_resolver') }} pr on e.card_id = pr.original_card_id
    where e.card_id is not null
      and u.is_test_user = 0
),

card_aggregates as (
    select
        ce.card_id,
        max(ce.resolved_place_id) as resolved_place_id,

        -- Counts
        count(*) filter (where ce.event_type in ('swipe_right', 'swipe_left')) as total_impressions,
        count(*) filter (where ce.event_type = 'swipe_right') as total_right_swipes,
        count(*) filter (where ce.event_type = 'swipe_left') as total_left_swipes,
        count(*) filter (where ce.event_type in ('save', 'saved')) as total_saves,
        count(*) filter (where ce.event_category = 'Share') as total_shares,
        count(*) filter (
            where ce.event_type in ('place_detail_view_open', 'detail_view')
        ) as total_detail_views,
        count(*) filter (
            where ce.event_type in ('book_button_click', 'book_with_deck')
        ) as total_book_clicks,
        count(*) filter (where ce.event_category = 'Conversion') as total_conversions,

        -- Unique users
        count(distinct ce.user_id) filter (
            where ce.event_type in ('swipe_right', 'swipe_left')
        ) as unique_viewers,
        count(distinct ce.user_id) filter (
            where ce.event_type in ('save', 'saved')
        ) as unique_savers,
        count(distinct ce.user_id) filter (
            where ce.event_category = 'Share'
        ) as unique_sharers,

        -- Timing
        min(date(ce.event_timestamp)) as first_seen_date,
        max(date(ce.event_timestamp)) as last_interaction_date,

        -- Recent activity (last 7 days)
        count(*) filter (
            where ce.event_type in ('swipe_right', 'swipe_left')
              and ce.event_timestamp >= current_date - 7
        ) as impressions_last_7d,
        count(*) filter (
            where ce.event_type in ('save', 'saved')
              and ce.event_timestamp >= current_date - 7
        ) as saves_last_7d,
        count(*) filter (
            where ce.event_category = 'Share'
              and ce.event_timestamp >= current_date - 7
        ) as shares_last_7d,

        -- Recent activity (last 30 days)
        count(*) filter (
            where ce.event_type in ('swipe_right', 'swipe_left')
              and ce.event_timestamp >= current_date - 30
        ) as impressions_last_30d,
        count(*) filter (
            where ce.event_type in ('save', 'saved')
              and ce.event_timestamp >= current_date - 30
        ) as saves_last_30d,
        count(*) filter (
            where ce.event_category = 'Share'
              and ce.event_timestamp >= current_date - 30
        ) as shares_last_30d

    from card_events ce
    group by ce.card_id
),

-- Pack appearance count
card_packs as (
    select
        e.card_id,
        count(distinct e.pack_id) as packs_appeared_in
    from {{ ref('stg_unified_events') }} e
    where e.card_id is not null
      and e.pack_id is not null
    group by e.card_id
),

-- Places metadata for enrichment via resolved_place_id
places_meta as (
    select
        place_id,
        name as place_name,
        formatted_address,
        split_part(formatted_address, ',', 1) as neighborhood,
        rating as places_rating,
        price_level as places_price_level,
        source_type as places_source_type,
        is_featured as places_is_featured
    from {{ ref('src_places') }}
)

select
    c.card_id,
    ca.resolved_place_id as place_id,
    coalesce(pm.place_name, c.name) as place_name,
    c.card_type,
    c.category,
    coalesce(pm.places_price_level, c.price_level) as price_level,
    coalesce(pm.places_rating, c.rating) as rating,
    pm.neighborhood,
    coalesce(pm.formatted_address, c.formatted_address) as formatted_address,
    coalesce(pm.places_source_type, c.source_type) as source_type,
    coalesce(pm.places_is_featured, c.is_featured::boolean) as is_featured,

    -- Volume metrics
    coalesce(ca.total_impressions, 0) as total_impressions,
    coalesce(ca.total_right_swipes, 0) as total_right_swipes,
    coalesce(ca.total_left_swipes, 0) as total_left_swipes,
    coalesce(ca.total_saves, 0) as total_saves,
    coalesce(ca.total_shares, 0) as total_shares,
    coalesce(ca.total_detail_views, 0) as total_detail_views,
    coalesce(ca.total_book_clicks, 0) as total_book_clicks,
    coalesce(ca.total_conversions, 0) as total_conversions,

    -- Unique users
    coalesce(ca.unique_viewers, 0) as unique_viewers,
    coalesce(ca.unique_savers, 0) as unique_savers,
    coalesce(ca.unique_sharers, 0) as unique_sharers,

    -- Rates
    case
        when coalesce(ca.total_right_swipes, 0) + coalesce(ca.total_left_swipes, 0) > 0
        then round(ca.total_right_swipes::numeric
                   / (ca.total_right_swipes + ca.total_left_swipes), 4)
        else null
    end as right_swipe_rate,
    case
        when coalesce(ca.total_impressions, 0) > 0
        then round(coalesce(ca.total_saves, 0)::numeric / ca.total_impressions, 4)
        else null
    end as save_rate,
    case
        when coalesce(ca.total_impressions, 0) > 0
        then round(coalesce(ca.total_shares, 0)::numeric / ca.total_impressions, 4)
        else null
    end as share_rate,
    case
        when coalesce(ca.total_impressions, 0) > 0
        then round(coalesce(ca.total_conversions, 0)::numeric / ca.total_impressions, 4)
        else null
    end as conversion_rate,

    -- Viral score: cards that are both saved AND shared
    case
        when coalesce(ca.total_saves, 0) > 0 and coalesce(ca.total_shares, 0) > 0
        then round(ca.total_shares::numeric / ca.total_saves, 4)
        else 0
    end as viral_score,

    -- Pack context
    coalesce(cp.packs_appeared_in, 0) as packs_appeared_in,

    -- Timing
    ca.first_seen_date,
    ca.last_interaction_date,

    -- Recency
    coalesce(ca.impressions_last_7d, 0) as impressions_last_7d,
    coalesce(ca.saves_last_7d, 0) as saves_last_7d,
    coalesce(ca.shares_last_7d, 0) as shares_last_7d,
    coalesce(ca.impressions_last_30d, 0) as impressions_last_30d,
    coalesce(ca.saves_last_30d, 0) as saves_last_30d,
    coalesce(ca.shares_last_30d, 0) as shares_last_30d

from {{ ref('stg_cards') }} c
left join card_aggregates ca on c.card_id = ca.card_id
left join card_packs cp on c.card_id = cp.card_id
left join places_meta pm on ca.resolved_place_id = pm.place_id
