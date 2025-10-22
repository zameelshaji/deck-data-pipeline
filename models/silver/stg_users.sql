-- User master table with all attributes
select 
    -- Core identity
    u.user_id,
    u.email,
    up.username,
    up.full_name,
    up.created_at,
    up.onboarding_completed,
    
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
    {{ref ('src_users')}} as u
left join 
    {{ ref ('src_user_profiles')}} as up
    on u.user_id = up.user_id
left join 
    {{ ref ('src_user_preferences')}} as upr
    on u.user_id = upr.user_id
