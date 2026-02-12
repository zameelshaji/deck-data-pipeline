-- Verify int_place_resolver has unique card_ids (no duplicates)
select
    original_card_id,
    count(*) as cnt
from {{ ref('int_place_resolver') }}
group by original_card_id
having count(*) > 1
