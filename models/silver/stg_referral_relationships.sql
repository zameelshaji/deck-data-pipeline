
with referred as (
    select 
        ru.user_id as referred_user_id,
        rc.user_id as referrer_user_id,
        ru.used_at
    from {{ ref('src_referral_usages') }} ru
    left join {{ ref('src_referral_codes') }} rc on rc.code_id = ru.code_id
)

select
    r.referrer_user_id,
    u1.username as referrer_username,
    u1.email as referrer_email,
    r.referred_user_id,
    u2.username as referred_username,
    u2.email as referred_email,
    r.used_at,
    date(r.used_at) as used_date
from referred r
left join {{ ref('stg_users')}} u1 on r.referrer_user_id = u1.user_id
left join {{ ref('stg_users')}} u2 on r.referred_user_id = u2.user_id

