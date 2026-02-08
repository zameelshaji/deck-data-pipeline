{{ config(materialized='table') }}

-- CTE 1: Queries (all eras)
with queries as (
    select
        user_id,
        query_timestamp as event_timestamp,
        'query' as event_type,
        null::text as card_id,
        response_pack_id::text as pack_id,
        'dextr_queries' as source_table,
        case
            when query_timestamp < '2025-11-20'::timestamptz then 'card_system'
            when query_timestamp < '2026-01-30'::timestamptz then 'places_system'
            else 'telemetry'
        end as data_era,
        app_version
    from {{ ref('src_dextr_queries') }}
    where user_id is not null and query_timestamp is not null
),

-- CTE 2: Swipes from dextr_pack_cards (card system era, before Nov 20 2025)
swipes_legacy as (
    select
        dq.user_id,
        dpc.created_at as event_timestamp,
        case
            when dpc.user_action = 'right' then 'swipe_right'
            when dpc.user_action = 'left' then 'swipe_left'
            else dpc.user_action
        end as event_type,
        dpc.card_id::text as card_id,
        dpc.pack_id::text as pack_id,
        'dextr_pack_cards' as source_table,
        'card_system' as data_era,
        dq.app_version
    from {{ ref('src_dextr_pack_cards') }} dpc
    inner join {{ ref('src_dextr_queries') }} dq
        on dpc.pack_id = dq.response_pack_id
    where dpc.created_at < '2025-11-20'::timestamptz
      and dpc.user_action in ('right', 'left')
      and dq.user_id is not null
      and dpc.created_at is not null
),

-- CTE 3: Swipes from dextr_places (places system + telemetry era, Nov 20 2025+)
swipes_current as (
    select
        dq.user_id,
        dp.created_at as event_timestamp,
        case
            when dp.user_action = 'like' then 'swipe_right'
            when dp.user_action = 'dislike' then 'swipe_left'
            else dp.user_action
        end as event_type,
        dp.place_deck_sku as card_id,
        dp.pack_id::text as pack_id,
        'dextr_places' as source_table,
        case
            when dp.created_at < '2026-01-30'::timestamptz then 'places_system'
            else 'telemetry'
        end as data_era,
        dq.app_version
    from {{ ref('src_dextr_places') }} dp
    inner join {{ ref('src_dextr_queries') }} dq
        on dp.pack_id = dq.response_pack_id
    where dp.created_at >= '2025-11-20'::timestamptz
      and dp.user_action in ('like', 'dislike')
      and dq.user_id is not null
      and dp.created_at is not null
),

-- CTE 4: Saves from entity tables (before telemetry era)
saves_from_entity_tables as (
    select
        user_id,
        timestamp as event_timestamp,
        'save' as event_type,
        card_id::text as card_id,
        source_id::text as pack_id,
        'core_card_actions' as source_table,
        case
            when timestamp < '2025-11-20'::timestamptz then 'card_system'
            else 'places_system'
        end as data_era,
        null::text as app_version
    from {{ ref('src_core_card_actions') }}
    where action_type = 'saved'
      and timestamp < '2026-01-30'::timestamptz
      and user_id is not null
      and timestamp is not null
),

-- CTE 5: Shares from entity tables (before telemetry era)
shares_from_entity_tables as (
    select
        user_id,
        timestamp as event_timestamp,
        'share' as event_type,
        card_id::text as card_id,
        source_id::text as pack_id,
        'core_card_actions' as source_table,
        case
            when timestamp < '2025-11-20'::timestamptz then 'card_system'
            else 'places_system'
        end as data_era,
        null::text as app_version
    from {{ ref('src_core_card_actions') }}
    where action_type = 'share'
      and timestamp < '2026-01-30'::timestamptz
      and user_id is not null
      and timestamp is not null
),

-- CTE 6: Clicks/conversions from entity tables (before telemetry era)
clicks_from_entity_tables as (
    select
        user_id,
        timestamp as event_timestamp,
        'conversion' as event_type,
        card_id::text as card_id,
        source_id::text as pack_id,
        'core_card_actions' as source_table,
        case
            when timestamp < '2025-11-20'::timestamptz then 'card_system'
            else 'places_system'
        end as data_era,
        null::text as app_version
    from {{ ref('src_core_card_actions') }}
    where action_type in ('opened_website', 'book_with_deck', 'click_directions', 'click_phone')
      and timestamp < '2026-01-30'::timestamptz
      and user_id is not null
      and timestamp is not null
),

-- CTE 7: Featured section actions (before telemetry era)
featured_actions as (
    select
        user_id,
        action_timestamp as event_timestamp,
        action_type as event_type,
        card_id::text as card_id,
        pack_id::text as pack_id,
        'featured_section_actions' as source_table,
        case
            when action_timestamp < '2025-11-20'::timestamptz then 'card_system'
            else 'places_system'
        end as data_era,
        null::text as app_version
    from {{ ref('src_featured_section_actions') }}
    where action_timestamp < '2026-01-30'::timestamptz
      and user_id is not null
      and action_timestamp is not null
),

-- CTE 8: Telemetry events (Jan 30 2026+)
telemetry_deduped as (
    select distinct on (coalesce(client_event_id, id::text))
        *
    from {{ ref('src_app_events') }}
    where event_timestamp >= '2026-01-30'::timestamptz
      and user_id is not null
      and event_timestamp is not null
    order by coalesce(client_event_id, id::text), event_timestamp desc
),

telemetry_events as (
    select
        user_id,
        event_timestamp,
        case
            when event_name = 'card_saved' then 'save'
            when event_name = 'card_shared' then 'card_share'
            when event_name = 'deck_shared' then 'deck_share'
            when event_name = 'swipe_right' then 'swipe_right'
            when event_name = 'swipe_left' then 'swipe_left'
            else event_name
        end as event_type,
        card_id::text as card_id,
        pack_id::text as pack_id,
        'app_events' as source_table,
        'telemetry' as data_era,
        null::text as app_version
    from telemetry_deduped
),

-- UNION ALL
all_events as (
    select * from queries
    union all
    select * from swipes_legacy
    union all
    select * from swipes_current
    union all
    select * from saves_from_entity_tables
    union all
    select * from shares_from_entity_tables
    union all
    select * from clicks_from_entity_tables
    union all
    select * from featured_actions
    union all
    select * from telemetry_events
)

-- Add event_category in the final wrapping CTE
select
    user_id,
    event_timestamp,
    event_type,
    card_id,
    pack_id,
    source_table,
    data_era,
    app_version,
    case
        when event_type = 'query' then 'AI'
        when event_type in ('swipe_right', 'swipe_left') then 'Swipe'
        when event_type = 'save' then 'Save'
        when event_type in ('card_share', 'deck_share', 'share') then 'Share'
        when event_type = 'conversion' then 'Conversion'
        when event_type in ('opened_website', 'book_with_deck', 'click_directions', 'click_phone') then 'Conversion'
        else 'Other'
    end as event_category
from all_events
