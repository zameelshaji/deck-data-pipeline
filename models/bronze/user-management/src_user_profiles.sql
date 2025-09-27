select 
    id,
    email,
    username,
    full_name,
    created_at,
    onboarding_completed
from 
    {{ source('public', 'user_profiles') }}