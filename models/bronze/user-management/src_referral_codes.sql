select 
    id as code_id,
    user_id,
    code,
    created_at,
    used_count
from {{ source("public", "referral_codes") }}