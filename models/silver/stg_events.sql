-- Fixed Unified event stream with proper type casting
with
    card_actions as (
        select
            'core_card_action' as event_source,
            user_id,
            card_id,
            action_type as event_name,
            source::text as source,
            source_id::text as source_id,
            timestamp as event_timestamp
        from {{ ref("src_core_card_actions") }}
    ),

    featured_actions as (
        select
            'featured_section' as event_source,
            user_id,
            card_id,
            action_type as event_name,
            pack_type::text as source,
            pack_id::text as source_id,
            action_timestamp as event_timestamp
        from {{ ref("src_featured_section_actions") }}
    ),

    ai_queries as (
        select
            'dextr' as event_source,
            user_id,
            null as card_id,
            'dextr_query' as event_name,
            'dextr'::text as source,
            response_pack_id::text as source_id,
            query_timestamp as event_timestamp
        from {{ ref("src_dextr_queries") }}
    ),

    carousel_impressions as (
        select
            'featured_carousel' as event_source,
            user_id,
            card_id,
            action_type as event_name,
            pack_type::text as source,
            pack_id::text as source_id,
            action_timestamp as event_timestamp
        from {{ ref("src_featured_carousel_impressions") }}
    ),

    dextr_pack_swipes as (
        select
            'dextr_pack' as event_source,
            dq.user_id,
            dpc.card_id::text as card_id,
            case
                when dpc.user_action = 'left'
                then 'swipe_left'
                when dpc.user_action = 'right'
                then 'swipe_right'
            end as event_name,
            'dextr'::text as source,
            dpc.pack_id::text as source_id,
            dpc.created_at as event_timestamp
        from {{ ref("src_dextr_pack_cards") }} as dpc
        left join
            {{ ref("src_dextr_queries") }} as dq on dpc.pack_id = dq.response_pack_id
        where dpc.user_action in ('left', 'right')
    ),

    multiplayer_sessions as (
        select
            'multiplayer' as event_source,
            sv.user_id,
            sp.card_id::text as card_id,
            case
                when vote_type = 'like'
                then 'swipe_right'
                when vote_type = 'pass'
                then 'swipe_left'
            end as event_name,
            'multiplayer_session'::text as source,
            sv.multiplayer_id::text as source_id,
            sv.voted_at as event_timestamp
        from {{ ref("src_session_votes") }} as sv
        left join {{ ref("src_session_places") }} as sp on sp.id = sv.place_id
    ),

    combined as (
        select *
        from card_actions
        union all
        select *
        from featured_actions
        union all
        select *
        from ai_queries
        union all
        select *
        from carousel_impressions
        union all
        select *
        from dextr_pack_swipes
        union all
        select *
        from multiplayer_sessions
    )

select
    *,
    -- -- Standardized categorization
    case
        when event_name in ('saved', 'share', 'swipe_right', 'swipe_left')
        then 'Content Curation'
        when
            event_name in (
                'opened_website',
                'book_button_click',
                'click_directions',
                'book_with_deck',
                'click_phone'
            )
        then 'Conversion Action'
        when event_name in ('dextr_query', 'mini_dextr')
        then 'AI Interaction'
        when
            event_name in (
                'Impression',
                'category_clicked',
                'detail_view_open',
                'detail_open',
                'click',
                'spotlight_click',
                'impression'
            )
        then 'Content Discovery'
        else 'Other'
    end as event_category

-- -- Conversion value scoring
-- CASE 
-- WHEN event_name = 'book_with_deck' THEN 10
-- WHEN event_name = 'opened_website' THEN 8
-- WHEN event_name = 'click_directions' THEN 7
-- WHEN event_name = 'saved' THEN 5
-- WHEN event_name = 'share' THEN 4
-- WHEN event_name = 'dextr_query' THEN 3
-- WHEN event_name = 'swipe_right' THEN 2
-- WHEN event_name IN ('mini_dextr', 'play_tiktok') THEN 2
-- WHEN event_name = 'Impression' THEN 1
-- WHEN event_name = 'swipe_left' THEN 0
-- ELSE 0
-- END as conversion_value,
-- Temporal attributes
-- DATE(event_timestamp) as event_date,
-- EXTRACT(hour FROM event_timestamp) as event_hour,
-- EXTRACT(dow FROM event_timestamp) as day_of_week,
-- DATE_TRUNC('week', event_timestamp) as event_week,
-- DATE_TRUNC('month', event_timestamp) as event_month
from combined
