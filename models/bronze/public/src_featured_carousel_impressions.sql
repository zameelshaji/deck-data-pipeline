with
    carousel_cycles as (
        select
            action_id,
            user_id,
            session_id,
            action_context::jsonb -> 'cardsViewedInCycle' as card_ids_json,
            'Impression' as action_type,
            pack_id,
            pack_type,
            action_timestamp,
            created_at
        from {{ source("public", "featured_section_actions") }}
        where action_type = 'carousel_cycle_complete'
    )

select
    action_id,
    user_id,
    session_id,
    unnest(array(select jsonb_array_elements_text(card_ids_json))) as card_id,
    action_type,
    pack_id,
    pack_type,
    action_timestamp,
    created_at
from carousel_cycles
where card_ids_json is not null
