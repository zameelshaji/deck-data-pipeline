
with test_users as (
    select 
        id,
        1 as is_test_user 
    from {{ ref('test_accounts')}}
)
-- User master table with all attributes
select 
    -- Core identity
    u.user_id,
    u.email,
    u.created_at,

    up.username,
    up.full_name,
    up.onboarding_completed,
    coalesce(t.is_test_user,0) as is_test_user,
    
    -- Preference data
    upr.preferences_completed,
    upr.travel_preference_selected,
    upr.likes_adventure,
    upr.likes_dining,
    upr.likes_drinks,
    upr.likes_culture,
    upr.likes_entertainment,
    upr.likes_health,
    upr.prefers_solo,
    upr.prefers_friends,
    upr.prefers_family,
    upr.prefers_date
from 
    {{ref('src_users')}} as u
left join 
    {{ ref('src_user_profiles')}} as up
    on u.user_id = up.user_id
left join 
    {{ ref('src_user_preferences')}} as upr
    on u.user_id = upr.user_id
left join 
    test_users as t 
    on u.user_id::text = t.id
where u.created_at is not null
