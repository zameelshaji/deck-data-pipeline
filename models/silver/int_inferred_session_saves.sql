{{ config(materialized='table') }}

with save_events as (
    select
        user_id,
        event_timestamp,
        card_id
    from {{ ref('int_inferred_session_events') }}
    where event_type = 'save'
),

saves_with_sessions as (
    select
        s.session_id,
        e.user_id,
        e.card_id,
        e.event_timestamp as saved_at
    from save_events e
    inner join {{ ref('int_inferred_sessions') }} s
        on e.user_id = s.user_id
        and e.event_timestamp between s.started_at and s.ended_at
)

select
    session_id,
    user_id,
    count(*) as save_count,
    count(distinct card_id) as unique_cards_saved,
    min(saved_at) as first_save_at,
    max(saved_at) as last_save_at
from saves_with_sessions
group by session_id, user_id
