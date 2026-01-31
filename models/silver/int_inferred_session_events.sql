{{ config(materialized='table') }}

with cutover as (
    select coalesce(min(created_at)::date, '9999-12-31'::date) as cutover_date
    from {{ ref('src_dextr_places') }}
    where created_at is not null
),

-- Queries (session anchors)
queries as (
    select
        user_id,
        query_timestamp as event_timestamp,
        'query' as event_type,
        response_pack_id::text as pack_id,
        null::text as card_id,
        app_version,
        'dextr_queries' as source_table
    from {{ ref('src_dextr_queries') }}
    where user_id is not null
      and query_timestamp is not null
),

-- Swipes from dextr_places (new table, post-cutover)
swipes_new as (
    select
        dq.user_id,
        dp.created_at as event_timestamp,
        case
            when dp.user_action = 'like' then 'swipe_right'
            when dp.user_action = 'dislike' then 'swipe_left'
        end as event_type,
        dp.pack_id::text as pack_id,
        dp.place_deck_sku as card_id,
        dq.app_version,
        'dextr_places' as source_table
    from {{ ref('src_dextr_places') }} dp
    inner join {{ ref('src_dextr_queries') }} dq
        on dp.pack_id = dq.response_pack_id
    where dp.user_action in ('like', 'dislike')
      and dp.created_at is not null
),

-- Swipes from dextr_pack_cards (legacy, pre-cutover)
swipes_legacy as (
    select
        dq.user_id,
        dpc.created_at as event_timestamp,
        case
            when dpc.user_action = 'right' then 'swipe_right'
            when dpc.user_action = 'left' then 'swipe_left'
        end as event_type,
        dpc.pack_id::text as pack_id,
        dpc.card_id::text as card_id,
        dq.app_version,
        'dextr_pack_cards' as source_table
    from {{ ref('src_dextr_pack_cards') }} dpc
    inner join {{ ref('src_dextr_queries') }} dq
        on dpc.pack_id = dq.response_pack_id
    cross join cutover c
    where dpc.user_action in ('left', 'right')
      and dpc.created_at is not null
      and dpc.created_at < c.cutover_date
),

-- Saves with pack attribution
saves as (
    select
        user_id,
        timestamp as event_timestamp,
        'save' as event_type,
        source_id as pack_id,
        card_id::text as card_id,
        null::text as app_version,
        'core_card_actions' as source_table
    from {{ ref('src_core_card_actions') }}
    where action_type = 'saved'
      and source = 'dextr'
      and source_id is not null
      and timestamp is not null
      and user_id is not null
),

-- Card shares with pack attribution
card_shares as (
    select
        user_id,
        timestamp as event_timestamp,
        'card_share' as event_type,
        source_id as pack_id,
        card_id::text as card_id,
        null::text as app_version,
        'core_card_actions' as source_table
    from {{ ref('src_core_card_actions') }}
    where action_type = 'share'
      and source_id is not null
      and timestamp is not null
      and user_id is not null
),

-- Featured section events
featured as (
    select
        user_id,
        action_timestamp as event_timestamp,
        action_type as event_type,
        pack_id::text as pack_id,
        card_id::text as card_id,
        null::text as app_version,
        'featured_section_actions' as source_table
    from {{ ref('src_featured_section_actions') }}
    where user_id is not null
      and action_timestamp is not null
)

select * from queries
union all
select * from swipes_new
union all
select * from swipes_legacy
union all
select * from saves
union all
select * from card_shares
union all
select * from featured
