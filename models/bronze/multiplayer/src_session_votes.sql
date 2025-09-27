SELECT 
    session_id as multiplayer_id,
    participant_id as user_id,
    place_id,
    vote_type,
    voted_at
FROM {{ source('public', 'session_votes') }}