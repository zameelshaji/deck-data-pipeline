select 
    referrer_user_id, 
    referrer_username,
    referrer_email,
    count(*) as referrals_used
from {{ ref('stg_referral_relationships')}}
group by 1,2,3

