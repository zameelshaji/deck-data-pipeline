{{ config(materialized='table') }}

-- Board Lifecycle — creation, views, likes, shares, unsaves per board
--
-- Grain: one row per board_id.
-- Answers: "Which boards get the most views per save?"
--          "What's the net save count (saves - unsaves)?"
--          "How often do boards get shared or liked?"
--          "What's the board creation → first-share lead time?"

with boards as (
    select
        b.id as board_id,
        b.user_id as creator_user_id,
        b.created_at,
        date(b.created_at) as created_date,
        b.name as board_name
    from {{ ref('src_boards') }} b
    inner join {{ ref('stg_users') }} u on b.user_id = u.user_id
    where u.is_test_user = 0
),

-- Board creation events from app_events (for is_private + verification)
board_create_events as (
    select
        board_id::text as board_id,
        user_id as creator_user_id_from_event,
        is_private,
        event_timestamp as creation_event_at
    from {{ ref('stg_app_events_enriched') }}
    where event_name = 'board_created'
),

-- Board views
board_views as (
    select
        board_id::text as board_id,
        count(*) as total_views,
        count(distinct user_id) as distinct_viewers,
        count(*) filter (where event_timestamp >= now() - interval '7 days') as views_7d,
        count(*) filter (where event_timestamp >= now() - interval '30 days') as views_30d,
        min(event_timestamp) as first_viewed_at
    from {{ ref('stg_app_events_enriched') }}
    where event_name = 'board_viewed'
      and board_id is not null
    group by board_id::text
),

-- Board likes (deck_liked events)
board_likes as (
    select
        target_id as board_id,
        count(*) filter (where edge_action = 'add') as total_likes,
        count(*) filter (where edge_action = 'remove') as total_unlikes,
        count(distinct actor_user_id) filter (where edge_action = 'add') as distinct_likers,
        min(occurred_at) filter (where edge_action = 'add') as first_liked_at
    from {{ ref('stg_social_graph_events') }}
    where target_kind = 'board'
    group by target_id
),

-- Board shares
board_shares as (
    select
        board_id::text as board_id,
        count(*) as total_shares,
        count(distinct sharer_user_id) as distinct_sharers,
        min(created_at) as first_shared_at
    from {{ ref('src_share_links') }}
    where share_type = 'deck'
      and board_id is not null
    group by board_id::text
),

-- Places saved to this board (event-level)
board_saves as (
    select
        board_id::text as board_id,
        count(*) as total_saves,
        count(distinct card_id) as unique_cards_saved
    from {{ ref('stg_app_events_enriched') }}
    where event_name in ('card_saved', 'place_saved')
      and board_id is not null
    group by board_id::text
),

-- Unsaves from this board
board_unsaves as (
    select
        board_id::text as board_id,
        count(*) as total_unsaves
    from {{ ref('stg_app_events_enriched') }}
    where event_name = 'card_unsaved'
      and board_id is not null
    group by board_id::text
)

select
    b.board_id,
    b.creator_user_id,
    b.board_name,
    b.created_at,
    b.created_date,
    bce.is_private,

    -- Views
    coalesce(bv.total_views, 0) as total_views,
    coalesce(bv.distinct_viewers, 0) as distinct_viewers,
    coalesce(bv.views_7d, 0) as views_7d,
    coalesce(bv.views_30d, 0) as views_30d,
    bv.first_viewed_at,

    -- Likes
    coalesce(bl.total_likes, 0) as total_likes,
    coalesce(bl.total_unlikes, 0) as total_unlikes,
    coalesce(bl.total_likes, 0) - coalesce(bl.total_unlikes, 0) as net_likes,
    coalesce(bl.distinct_likers, 0) as distinct_likers,
    bl.first_liked_at,

    -- Shares
    coalesce(bsh.total_shares, 0) as total_shares,
    coalesce(bsh.distinct_sharers, 0) as distinct_sharers,
    bsh.first_shared_at,
    case
        when bsh.first_shared_at is not null
        then extract(epoch from (bsh.first_shared_at - b.created_at)) / 3600.0
    end as hours_created_to_first_share,

    -- Saves/unsaves — net saves
    coalesce(bsv.total_saves, 0) as total_saves,
    coalesce(bsv.unique_cards_saved, 0) as unique_cards_saved,
    coalesce(bun.total_unsaves, 0) as total_unsaves,
    coalesce(bsv.total_saves, 0) - coalesce(bun.total_unsaves, 0) as net_saves,

    -- Engagement flags
    coalesce(bv.total_views, 0) > 0 as has_been_viewed,
    coalesce(bl.total_likes, 0) > 0 as has_been_liked,
    coalesce(bsh.total_shares, 0) > 0 as has_been_shared

from boards b
left join board_create_events bce on b.board_id::text = bce.board_id
left join board_views bv on b.board_id::text = bv.board_id
left join board_likes bl on b.board_id::text = bl.board_id
left join board_shares bsh on b.board_id::text = bsh.board_id
left join board_saves bsv on b.board_id::text = bsv.board_id
left join board_unsaves bun on b.board_id::text = bun.board_id
