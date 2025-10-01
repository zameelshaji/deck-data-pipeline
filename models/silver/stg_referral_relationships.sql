with referral_codes as (
    select * from {{ ref('src_referral_codes') }}
),

referral_usages as (
    select * from {{ ref('src_referral_usages') }}
)

select
    rc.code_id,
    rc.user_id as referrer_user_id,
    ru.referred_user_id,
    rc.code,
    rc.created_at as code_created_at,
    ru.used_at as referral_used_at
from referral_codes rc
inner join referral_usages ru
    on rc.code_id = ru.code_id