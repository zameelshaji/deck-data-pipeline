SELECT 
    card_id,
    user_id,
    source,
    source_id,
    action_type,
    timestamp,
    created_at
FROM {{ source('public', 'core_card_actions') }}