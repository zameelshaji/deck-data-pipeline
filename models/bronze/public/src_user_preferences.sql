select
    user_id,
    updated_at,
    preferences::jsonb ->> 'experiencePreferences' as experience_preferences_raw,
    preferences::jsonb ->> 'companionPreferences' as companion_preferences_raw,
    preferences::jsonb ->> 'travelPreference' as travel_preference_selected,
    case
        when
            preferences::jsonb -> 'experiencePreferences' is not null
            and preferences::jsonb -> 'companionPreferences' is not null
            and preferences::jsonb -> 'travelPreference' is not null
        then true
        else false
    end as preferences_completed,
    preferences::jsonb -> 'experiencePreferences' ? 'Adventure' as likes_adventure,
    preferences::jsonb -> 'experiencePreferences' ? 'Dining' as likes_dining,
    preferences::jsonb -> 'experiencePreferences' ? 'Drinks' as likes_drinks,
    preferences::jsonb -> 'experiencePreferences' ? 'Culture' as likes_culture,
    preferences::jsonb
    -> 'experiencePreferences'
    ? 'Entertainment' as likes_entertainment,
    preferences::jsonb -> 'experiencePreferences' ? 'Health' as likes_health,
    preferences::jsonb -> 'companionPreferences' ? 'Solo' as prefers_solo,
    preferences::jsonb -> 'companionPreferences' ? 'Friends' as prefers_friends,
    preferences::jsonb -> 'companionPreferences' ? 'Family' as prefers_family,
    preferences::jsonb -> 'companionPreferences' ? 'Date' as prefers_date
from {{ source("public", "user_preferences") }}
