{{ config(enabled=false) }}
with referral_relationships as (
    select * from {{ ref('stg_referral_relationships') }}
)

select
    date(referral_used_at) as referral_date,
    referrer_user_id,
    count(distinct referred_user_id) as total_referrals,
    rank() over (partition by date(referral_used_at) order by count(distinct referred_user_id) desc) as daily_rank
from referral_relationships
group by date(referral_used_at), referrer_user_id
order by referral_date desc, total_referrals desc