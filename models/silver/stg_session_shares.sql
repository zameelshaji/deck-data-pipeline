with app_event_shares as (
    select
        effective_session_id as session_id,
        user_id,
        event_name as share_type,
        null::text as share_channel,
        share_link_id,
        event_timestamp as shared_at
    from {{ ref('stg_app_events_enriched') }}
    where event_name in ('card_shared', 'deck_shared')
),

link_shares as (
    select
        session_id,
        sharer_user_id as user_id,
        share_type,
        share_channel,
        id as share_link_id,
        created_at as shared_at
    from {{ ref('src_share_links') }}
    where sharer_user_id is not null
      and session_id is not null
),

all_shares as (
    select * from app_event_shares
    union all
    select * from link_shares
),

session_shares as (
    select
        session_id,
        user_id,
        count(*) as share_count,
        array_agg(distinct share_type) filter (where share_type is not null) as share_types,
        array_agg(distinct share_channel) filter (where share_channel is not null) as share_channels,
        min(shared_at) as first_share_at
    from all_shares
    where session_id is not null
    group by session_id, user_id
)

select * from session_shares
