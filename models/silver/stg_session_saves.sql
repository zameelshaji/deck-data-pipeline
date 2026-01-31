with app_event_saves as (
    select
        effective_session_id as session_id,
        user_id,
        card_id,
        event_timestamp as saved_at
    from {{ ref('stg_app_events_enriched') }}
    where event_name = 'card_saved'
),

session_saves as (
    select
        session_id,
        user_id,
        count(*) as save_count,
        count(distinct card_id) as unique_cards_saved,
        min(saved_at) as first_save_at,
        max(saved_at) as last_save_at
    from app_event_saves
    where session_id is not null
    group by session_id, user_id
)

select * from session_saves
