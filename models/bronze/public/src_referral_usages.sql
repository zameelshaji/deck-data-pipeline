select 
    id as usage_id,
    code_id,
    user_id,
    used_at
from {{ source("public", "referral_usages") }}
where user_id::text not in (select id from {{ref('src_test_accounts')}} )