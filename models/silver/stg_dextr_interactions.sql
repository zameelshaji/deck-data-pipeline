-- AI system performance and user satisfaction analytics
WITH query_pack_performance AS (
    SELECT 
        dq.query_id,
        dq.user_id,
        dq.query_text,
        dq.query_timestamp,
        dq.location,
        dq.app_version,
        dq.processing_time/1000 as processing_time_secs,
        
        -- Pack details
        dp.pack_id,
        dp.pack_name,
        dp.total_cards,
        dp.featured_cards_count::int as featured_cards_count,
        dp.experience_cards_count::int as experience_cards_count
        
    FROM {{ ref('src_dextr_queries') }} dq
    LEFT JOIN {{ ref('src_dextr_packs') }} dp 
        ON dq.response_pack_id = dp.pack_id
),

pack_engagement AS (
    SELECT 
        pack_id,
        COUNT(*) as total_cards_in_pack,
        COUNT(CASE WHEN shown_to_user THEN 1 END) as cards_shown,
        COUNT(CASE WHEN user_action = 'right' THEN 1 END) as cards_liked,
        COUNT(CASE WHEN user_action = 'left' THEN 1 END) as cards_disliked,
        COUNT(CASE WHEN user_action IS NOT NULL THEN 1 END) as cards_acted_upon
    FROM {{ ref('src_dextr_pack_cards') }}
    GROUP BY pack_id
)

SELECT 
    qpp.*,
    
    -- Engagement metrics
    COALESCE(pe.total_cards_in_pack, 0) as total_cards_in_pack,
    COALESCE(pe.cards_shown, 0) as cards_shown,
    COALESCE(pe.cards_liked, 0) as cards_liked,
    COALESCE(pe.cards_disliked, 0) as cards_disliked,
    COALESCE(pe.cards_acted_upon, 0) as cards_acted_upon,
    
    -- Performance calculations
    CASE WHEN pe.total_cards_in_pack > 0 THEN ROUND(100.0 * pe.cards_shown / pe.total_cards_in_pack, 2) ELSE 0 END AS pack_completion_rate_percent,     
    CASE WHEN pe.cards_shown > 0 THEN ROUND(100.0 * pe.cards_acted_upon / pe.cards_shown, 2)ELSE 0 END as engagement_rate_percent, 
    CASE WHEN pe.cards_acted_upon > 0 THEN ROUND(100.0 * pe.cards_liked / pe.cards_acted_upon, 2)ELSE 0 END as like_rate_percent

FROM query_pack_performance qpp
LEFT JOIN pack_engagement pe ON qpp.pack_id = pe.pack_id