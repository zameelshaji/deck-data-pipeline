{{ config(materialized='table') }}

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
                when dpc.user_action = 'left'  then 'swipe_left'
                when dpc.user_action = 'right' then 'swipe_right'
            end as event_name,
            'dextr'::text as source,
            dpc.pack_id::text as source_id,
            dpc.created_at as event_timestamp
        from {{ ref("src_dextr_pack_cards") }} as dpc
        left join {{ ref("src_dextr_queries") }} as dq
            on dpc.pack_id = dq.response_pack_id
        where dpc.user_action in ('left', 'right')
    ),

    multiplayer_sessions as (
        select
            'multiplayer' as event_source,
            sv.user_id,
            sp.card_id::text as card_id,
            case
                when vote_type = 'like' then 'swipe_right'
                when vote_type = 'pass' then 'swipe_left'
            end as event_name,
            'multiplayer_session'::text as source,
            sv.multiplayer_id::text as source_id,
            sv.voted_at as event_timestamp
        from {{ ref("src_session_votes") }} as sv
        left join {{ ref("src_session_places") }} as sp
            on sp.id = sv.place_id
    ),

    legacy as (
        select 
            'legacy' as event_source,
            user_id,
            place_id::text as card_id,
            case 
                when direction in ('right', 'up') then 'swipe_right'
                when direction = 'left'          then 'swipe_left'
            end as event_name, 
            null as source,
            null as source_id,
            created_at as event_timestamp
        from {{ ref("src_user_swipes_v2") }}

        union all 

        select 
            'legacy' as event_source,
            user_id,
            place_id::text as card_id,
            'swipe_right' as event_name, 
            null as source,
            null as source_id,
            created_at as event_timestamp
        from {{ ref("src_user_liked_places") }}
    ),

    combined as (
        select * from card_actions
        union all
        select * from featured_actions
        union all
        select * from ai_queries
        union all
        select * from carousel_impressions
        union all
        select * from dextr_pack_swipes
        union all
        select * from multiplayer_sessions
        union all 
        select * from legacy
    )

select
    c.*,

    -- High-level event category (UX bucket)
    case
        when c.event_name in ('saved', 'share', 'swipe_right', 'swipe_left')
            then 'Content Curation'
        when c.event_name in (
                'opened_website',
                'book_button_click',
                'click_directions',
                'book_with_deck',
                'click_phone'
            )
            then 'Conversion Action'
        when c.event_name in ('dextr_query', 'mini_dextr')
            then 'AI Interaction'
        when c.event_name in (
                'Impression',       -- handle both cases
                'impression',
                'category_clicked',
                'detail_view_open',
                'detail_open',
                'click',
                'spotlight_click'
            )
            then 'Content Discovery'
        else 'Other'
    end as event_category,

    --Intent stage: Interest vs Conversion vs Other
    case
        -- Interest-level signals
        when c.event_name in (
            'detail_open',
            'detail_view_open',
            'category_clicked',
            'spotlight_click',
            'saved',
            'share',
            'swipe_right',
            'click',
            'Impression',
            'impression'
        )
            then 'Interest'

        -- Conversion-level signals
        when c.event_name in (
            'opened_website',
            'click_directions',
            'click_phone',
            'book_button_click',
            'book_with_deck'
        )
            then 'Conversion'

        else 'Other'
    end as intent_stage

from combined c
inner join {{ ref('src_user_profiles') }} p
    on p.user_id = c.user_id
