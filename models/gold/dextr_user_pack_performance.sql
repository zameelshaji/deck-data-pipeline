select 
    dq.*,
    dp.card_id,
    c.name as place_name,
    c.rating,
    c.user_ratings_total as no_of_ratings,
    c.price_level,
    dp.event_name
from {{ ref('dextr_query_user_performance')}} dq 
left join {{ ref('stg_dextr_pack_events')}} dp
    on dp.source_id = dq.pack_id::text
left join {{ ref('stg_cards')}} c
    on dp.card_id = c.card_id