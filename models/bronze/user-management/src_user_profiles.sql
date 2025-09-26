select 
    id,
    email,
    username,
    full_name,
    created_at,
    onboarding_completed,
    follower_count,
    following_count
from 
    {{ source('public', 'user_profiles_with_stats') }}