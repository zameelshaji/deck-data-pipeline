with app_event_saves as (
    select
        effective_session_id as session_id,
        user_id,
        card_id,
        event_timestamp as saved_at
    from {{ ref('stg_app_events_enriched') }}
    where event_name = 'card_saved'
),

board_saves as (
    select
        b.source_session_id as session_id,
        bp.added_by as user_id,
        bp.place_id::text as card_id,
        bp.added_at as saved_at
    from {{ ref('src_board_places') }} bp
    left join {{ ref('src_boards') }} b on bp.board_id = b.id
    where bp.added_by is not null
      and b.source_session_id is not null
),

all_saves as (
    select * from app_event_saves
    union all
    select * from board_saves
),

session_saves as (
    select
        session_id,
        user_id,
        count(*) as save_count,
        count(distinct card_id) as unique_cards_saved,
        min(saved_at) as first_save_at,
        max(saved_at) as last_save_at
    from all_saves
    where session_id is not null
    group by session_id, user_id
)

select * from session_saves
