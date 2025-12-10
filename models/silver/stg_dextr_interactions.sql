-- AI system performance and user satisfaction analytics
with
    query_pack_performance as (
        select
            dq.query_id,
            dq.user_id,
            dq.query_text,
            dq.query_timestamp,
            dq.location,
            dq.app_version,
            dq.processing_time / 1000 as processing_time_secs,

            -- Pack details
            dp.pack_id,
            dp.pack_name,
            dp.total_cards,
            dp.featured_cards_count::int as featured_cards_count,
            dp.experience_cards_count::int as experience_cards_count

        from {{ ref("src_dextr_queries") }} dq
        left join {{ ref("src_dextr_packs") }} dp on dq.response_pack_id = dp.pack_id
    ),

    pack_engagement as (
        select
            pack_id,
            count(*) as total_cards_in_pack,
            count(case when shown_to_user then 1 end) as cards_shown,
            count(case when user_action = 'right' then 1 end) as cards_liked,
            count(case when user_action = 'left' then 1 end) as cards_disliked,
            count(case when user_action is not null then 1 end) as cards_acted_upon
        from {{ ref("src_dextr_pack_cards") }}
        group by pack_id

        union all 
    
        -- post gemini
        select 
            pack_id, 
            count(*) as total_cards_in_pack,
            null as cards_shown, 
            count(case when user_action = 'like' then 1 end) as cards_liked, 
            count(case when user_action = 'dislike' then 1 end) as cards_disliked, 
            count(case when user_action is not null then 1 end) as cards_acted_upon
        from {{ref('src_dextr_places')}}
        group by pack_id
    )



select
    qpp.*,

    -- Engagement metrics
    coalesce(pe.total_cards_in_pack, 0) as total_cards_in_pack,
    coalesce(pe.cards_shown, 0) as cards_shown,
    coalesce(pe.cards_liked, 0) as cards_liked,
    coalesce(pe.cards_disliked, 0) as cards_disliked,
    coalesce(pe.cards_acted_upon, 0) as cards_acted_upon,

    -- Performance calculations
    case
        when pe.total_cards_in_pack > 0
        then round(100.0 * pe.cards_shown / pe.total_cards_in_pack, 2)
        else 0
    end as pack_completion_rate_percent,
    case
        when pe.cards_shown > 0
        then round(100.0 * pe.cards_acted_upon / pe.cards_shown, 2)
        else 0
    end as engagement_rate_percent,
    case
        when pe.cards_acted_upon > 0
        then round(100.0 * pe.cards_liked / pe.cards_acted_upon, 2)
        else 0
    end as like_rate_percent

from query_pack_performance qpp
left join pack_engagement pe on qpp.pack_id = pe.pack_id
inner join {{ref ('src_user_profiles')}} p on p.user_id = qpp.user_id
