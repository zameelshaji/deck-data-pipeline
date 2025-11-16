{{ config(enabled=false) }}
with referral_relationships as (
    select * from {{ ref('stg_referral_relationships') }}
)

select
    date(referral_used_at) as referral_date,
    count(distinct code_id) as codes_used,
    count(distinct referrer_user_id) as active_referrers,
    count(distinct referred_user_id) as new_referred_users,
    avg(extract(epoch from (referral_used_at - code_created_at)) / 86400.0) as avg_days_to_use
from referral_relationships
group by date(referral_used_at)
order by referral_date desc