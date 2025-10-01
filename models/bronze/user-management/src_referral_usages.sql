select 
    id as usage_id,
    code_id,
    user_id as referred_user_id,
    used_at
from {{ source("public", "referral_usages") }}