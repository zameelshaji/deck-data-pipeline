select * 
from {{ source("public", "giveaway_claims") }}
where user_id::text not in (select id from {{ref('src_test_accounts')}} )