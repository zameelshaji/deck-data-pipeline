{{ config(materialized='table') }}

with sessions_base as (
    select * from {{ ref('fct_session_outcomes') }}
),

-- Link prompts to sessions via user_id + timestamp within session window
session_prompts as (
    select
        sb.session_id,
        jsonb_agg(
            jsonb_build_object(
                'query_text', q.query_text,
                'timestamp', q.query_timestamp,
                'pack_name', p.pack_name,
                'location', q.location,
                'total_cards_in_pack', p.total_cards
            ) order by q.query_timestamp
        ) as prompts_detail,
        count(distinct q.query_id) as prompt_count
    from {{ ref('src_dextr_queries') }} q
    inner join sessions_base sb
        on q.user_id = sb.user_id
        and q.query_timestamp between sb.started_at and sb.ended_at
    left join {{ ref('src_dextr_packs') }} p
        on q.response_pack_id = p.pack_id
    group by sb.session_id
),

-- Map packs to sessions via prompts (response_pack_id) and app_events (pack_id)
pack_sessions as (
    -- From prompts: query timestamp falls within session
    select distinct
        sb.session_id,
        q.response_pack_id::text as pack_id
    from {{ ref('src_dextr_queries') }} q
    inner join sessions_base sb
        on q.user_id = sb.user_id
        and q.query_timestamp between sb.started_at and sb.ended_at
    where q.response_pack_id is not null

    union

    -- From app_events: events that carry pack_id
    select distinct
        effective_session_id as session_id,
        pack_id
    from {{ ref('stg_app_events_enriched') }}
    where pack_id is not null
      and effective_session_id is not null
),

-- Unify legacy and post-Gemini card interactions with resolved card details
unified_pack_cards as (
    -- Legacy cards (right/left) — card_id maps directly to stg_cards
    select
        dpc.pack_id::text as pack_id,
        dpc.card_id::text as card_id,
        c.name as card_name,
        c.category,
        c.rating,
        dpc.card_order,
        case
            when dpc.user_action = 'right' then 'liked'
            when dpc.user_action = 'left' then 'disliked'
            else 'unswiped'
        end as user_action
    from {{ ref('src_dextr_pack_cards') }} dpc
    left join {{ ref('stg_cards') }} c on dpc.card_id::text = c.card_id

    union all

    -- Post-Gemini places (like/dislike) — join via place_id to src_places for name,
    -- and use deck_sku to match stg_cards for category/rating
    select
        dp.pack_id::text as pack_id,
        coalesce(dp.place_deck_sku, pl.deck_sku, dp.place_id::text) as card_id,
        pl.name as card_name,
        c.category,
        coalesce(c.rating, pl.rating) as rating,
        null::integer as card_order,
        case
            when dp.user_action = 'like' then 'liked'
            when dp.user_action = 'dislike' then 'disliked'
            else 'unswiped'
        end as user_action
    from {{ ref('src_dextr_places') }} dp
    left join {{ ref('src_places') }} pl on dp.place_id = pl.place_id
    left join {{ ref('stg_cards') }} c on coalesce(dp.place_deck_sku, pl.deck_sku) = c.card_id
),

session_cards as (
    select
        ps.session_id,
        jsonb_agg(
            jsonb_build_object(
                'card_name', uc.card_name,
                'card_id', uc.card_id,
                'category', uc.category,
                'rating', uc.rating,
                'action', uc.user_action,
                'card_order', uc.card_order
            ) order by uc.card_order nulls last
        ) as cards_generated_detail,
        -- Separate card lists by action
        jsonb_agg(
            jsonb_build_object('card_id', uc.card_id, 'card_name', uc.card_name)
            order by uc.card_order nulls last
        ) as cards_generated,
        jsonb_agg(
            jsonb_build_object('card_id', uc.card_id, 'card_name', uc.card_name)
            order by uc.card_order nulls last
        ) filter (where uc.user_action = 'liked') as cards_liked_list,
        jsonb_agg(
            jsonb_build_object('card_id', uc.card_id, 'card_name', uc.card_name)
            order by uc.card_order nulls last
        ) filter (where uc.user_action = 'disliked') as cards_disliked_list,
        count(*) as total_cards_generated,
        count(*) filter (where uc.user_action = 'liked') as cards_liked,
        count(*) filter (where uc.user_action = 'disliked') as cards_disliked
    from unified_pack_cards uc
    inner join pack_sessions ps on uc.pack_id = ps.pack_id
    group by ps.session_id
),

-- Saves: resolve place_id → src_places for name, then to stg_cards via deck_sku
session_saves as (
    select
        coalesce(bp.session_id, sb.session_id) as session_id,
        jsonb_agg(
            jsonb_build_object(
                'card_name', pl.name,
                'card_id', coalesce(pl.deck_sku, bp.place_id::text),
                'category', c.category,
                'board_name', coalesce(b.name, 'Default Board'),
                'saved_at', bp.added_at
            ) order by bp.added_at
        ) as saves_detail,
        jsonb_agg(
            jsonb_build_object('card_id', coalesce(pl.deck_sku, bp.place_id::text), 'card_name', pl.name)
            order by bp.added_at
        ) as cards_saved_list
    from {{ ref('src_board_places_v2') }} bp
    left join {{ ref('src_places') }} pl on bp.place_id = pl.place_id
    left join {{ ref('stg_cards') }} c on coalesce(pl.deck_sku, bp.place_id::text) = c.card_id
    left join {{ ref('src_boards') }} b on bp.board_id = b.id
    left join sessions_base sb
        on bp.session_id is null
        and bp.added_by = sb.user_id
        and bp.added_at between sb.started_at and sb.ended_at
    where coalesce(bp.session_id, sb.session_id) is not null
    group by coalesce(bp.session_id, sb.session_id)
),

-- Pre-aggregate share viewer counts
share_viewer_counts as (
    select
        share_link_id,
        count(distinct viewer_user_id) as unique_viewers,
        count(*) as total_interactions
    from {{ ref('stg_share_interactions_clean') }}
    where time_since_share_minutes <= 1440
    group by share_link_id
),

session_shares as (
    select
        coalesce(sl.session_id, sb.session_id) as session_id,
        jsonb_agg(
            jsonb_build_object(
                'share_type', sl.share_type,
                'share_channel', sl.share_channel,
                'shared_at', sl.created_at,
                'board_name', b.name,
                'card_name', pl.name,
                'unique_viewers', coalesce(sv.unique_viewers, 0),
                'total_interactions', coalesce(sv.total_interactions, 0)
            ) order by sl.created_at
        ) as shares_detail,
        jsonb_agg(
            distinct jsonb_build_object('card_id', coalesce(pl.deck_sku, sl.card_id::text), 'card_name', pl.name)
        ) filter (where sl.card_id is not null) as cards_shared_list,
        sum(coalesce(sv.unique_viewers, 0)) as total_share_viewers
    from {{ ref('src_share_links') }} sl
    left join share_viewer_counts sv on sl.id = sv.share_link_id
    left join {{ ref('src_boards') }} b on sl.board_id = b.id
    left join {{ ref('src_places') }} pl on sl.card_id = pl.place_id
    left join sessions_base sb
        on sl.session_id is null
        and sl.sharer_user_id = sb.user_id
        and sl.created_at between sb.started_at and sb.ended_at
    where coalesce(sl.session_id, sb.session_id) is not null
    group by coalesce(sl.session_id, sb.session_id)
),

-- Chronological event log — card_id in app_events may be a place_id (UUID),
-- so resolve via src_places, falling back to stg_cards for legacy card_ids
session_events_timeline as (
    select
        e.effective_session_id as session_id,
        jsonb_agg(
            jsonb_build_object(
                'event', e.event_name,
                'timestamp', e.event_timestamp,
                'card_name', coalesce(pl.name, c.name),
                'card_id', e.card_id
            ) order by e.event_timestamp
        ) as event_timeline,
        count(*) as total_events
    from {{ ref('stg_app_events_enriched') }} e
    left join {{ ref('src_places') }} pl on e.card_id = pl.place_id::text
    left join {{ ref('stg_cards') }} c on e.card_id::text = c.card_id
    where e.effective_session_id is not null
    group by e.effective_session_id
)

select
    sb.*,

    -- Prompts
    sp.prompts_detail,
    coalesce(sp.prompt_count, 0) as explorer_prompt_count,

    -- Cards generated
    sc.cards_generated_detail,
    sc.cards_generated,
    sc.cards_liked_list,
    sc.cards_disliked_list,
    coalesce(sc.total_cards_generated, 0) as total_cards_generated,
    coalesce(sc.cards_liked, 0) as cards_liked,
    coalesce(sc.cards_disliked, 0) as cards_disliked,

    -- Saves
    ss.saves_detail,
    ss.cards_saved_list,

    -- Shares
    sh.shares_detail,
    sh.cards_shared_list,
    coalesce(sh.total_share_viewers, 0) as total_share_viewers,

    -- Event timeline
    se.event_timeline,
    coalesce(se.total_events, 0) as total_events

from sessions_base sb
left join session_prompts sp on sb.session_id = sp.session_id
left join session_cards sc on sb.session_id = sc.session_id
left join session_saves ss on sb.session_id = ss.session_id
left join session_shares sh on sb.session_id = sh.session_id
left join session_events_timeline se on sb.session_id = se.session_id
