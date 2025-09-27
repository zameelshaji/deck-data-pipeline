select 
    action_id,
    user_id,
    session_id,

    action_context::jsonb ->> 'featuredPlaceId' as card_id,
    action_context::jsonb ->> 'position' as position,
    action_type,

    pack_id,
    pack_type,
    action_timestamp,
    created_at
from     
    {{ source('public', 'featured_section_actions') }}
where
    action_type <> 'carousel_cycle_complete'