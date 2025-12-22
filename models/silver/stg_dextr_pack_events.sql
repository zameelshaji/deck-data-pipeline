with legacy as ( 
        select
            'dextr_pack_legacy' as event_source,
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

dextr_post_gemini as (
        select 
            'dextr_pack' as event_source, 
            dq.user_id,
            p.deck_sku as card_id,
            case
                when dpc.user_action = 'dislike'
                then 'swipe_left'
                when dpc.user_action = 'like'
                then 'swipe_right'
            end as event_name,
            'dextr'::text as source,
            dpc.pack_id::text as source_id,
            dpc.created_at as event_timestamp
        from {{ ref('src_dextr_places')}} as dpc
        left join {{ ref("src_dextr_queries") }} as dq on dpc.pack_id = dq.response_pack_id
        left join {{ ref('src_places')}} as p on p.place_id = dpc.place_id
        where dpc.user_action in ('like', 'dislike')
)

select * from legacy 
union all 
select * from dextr_post_gemini
