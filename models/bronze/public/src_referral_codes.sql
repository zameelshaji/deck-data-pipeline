select 
    id as code_id,
    user_id,
    code,
    created_at,
    used_count
from {{ source("public", "referral_codes") }}
where user_id::text not in (select id from {{ref('src_test_accounts')}} )