{{ config(materialized='table') }}

with sessions_base as (
    select * from {{ ref('fct_session_outcomes') }}
),

-- Link prompts to sessions via app_events (dextr_query events carry pack_id)
session_prompts as (
    select
        e.effective_session_id as session_id,
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
    inner join {{ ref('stg_app_events_enriched') }} e
        on e.user_id = q.user_id
        and e.event_name = 'dextr_query'
        and e.pack_id = q.response_pack_id::text
    left join {{ ref('src_dextr_packs') }} p
        on q.response_pack_id = p.pack_id
    where e.effective_session_id is not null
    group by e.effective_session_id
),

-- Map packs to sessions
pack_sessions as (
    select distinct
        effective_session_id as session_id,
        pack_id
    from {{ ref('stg_app_events_enriched') }}
    where pack_id is not null
      and effective_session_id is not null
),

-- Unify legacy and post-Gemini card interactions
unified_pack_cards as (
    -- Legacy cards (right/left)
    select
        pack_id::text as pack_id,
        card_id::text as card_id,
        card_order,
        case
            when user_action = 'right' then 'liked'
            when user_action = 'left' then 'disliked'
            else 'unswiped'
        end as user_action
    from {{ ref('src_dextr_pack_cards') }}

    union all

    -- Post-Gemini places (like/dislike)
    select
        pack_id::text as pack_id,
        place_deck_sku as card_id,
        null::integer as card_order,
        case
            when user_action = 'like' then 'liked'
            when user_action = 'dislike' then 'disliked'
            else 'unswiped'
        end as user_action
    from {{ ref('src_dextr_places') }}
),

session_cards as (
    select
        ps.session_id,
        jsonb_agg(
            jsonb_build_object(
                'card_name', c.name,
                'card_id', uc.card_id,
                'category', c.category,
                'rating', c.rating,
                'action', uc.user_action,
                'card_order', uc.card_order
            ) order by uc.card_order nulls last
        ) as cards_generated_detail,
        count(*) as total_cards_generated,
        count(*) filter (where uc.user_action = 'liked') as cards_liked,
        count(*) filter (where uc.user_action = 'disliked') as cards_disliked
    from unified_pack_cards uc
    inner join pack_sessions ps on uc.pack_id = ps.pack_id
    left join {{ ref('stg_cards') }} c on uc.card_id = c.card_id
    group by ps.session_id
),

-- Saves: native sessions have session_id, inferred use timestamp overlap
session_saves as (
    select
        coalesce(bp.session_id, sb.session_id) as session_id,
        jsonb_agg(
            jsonb_build_object(
                'card_name', c.name,
                'card_id', bp.place_id,
                'category', c.category,
                'board_name', coalesce(b.name, 'Default Board'),
                'saved_at', bp.added_at
            ) order by bp.added_at
        ) as saves_detail
    from {{ ref('src_board_places_v2') }} bp
    left join {{ ref('stg_cards') }} c on bp.place_id::text = c.card_id
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
                'card_name', c.name,
                'unique_viewers', coalesce(sv.unique_viewers, 0),
                'total_interactions', coalesce(sv.total_interactions, 0)
            ) order by sl.created_at
        ) as shares_detail,
        sum(coalesce(sv.unique_viewers, 0)) as total_share_viewers
    from {{ ref('src_share_links') }} sl
    left join share_viewer_counts sv on sl.id = sv.share_link_id
    left join {{ ref('src_boards') }} b on sl.board_id = b.id
    left join {{ ref('stg_cards') }} c on sl.card_id::text = c.card_id
    left join sessions_base sb
        on sl.session_id is null
        and sl.sharer_user_id = sb.user_id
        and sl.created_at between sb.started_at and sb.ended_at
    where coalesce(sl.session_id, sb.session_id) is not null
    group by coalesce(sl.session_id, sb.session_id)
),

-- Chronological event log (meaningful events only)
session_events_timeline as (
    select
        e.effective_session_id as session_id,
        jsonb_agg(
            jsonb_build_object(
                'event', e.event_name,
                'timestamp', e.event_timestamp,
                'card_name', c.name,
                'card_id', e.card_id
            ) order by e.event_timestamp
        ) as event_timeline,
        count(*) as total_events
    from {{ ref('stg_app_events_enriched') }} e
    left join {{ ref('stg_cards') }} c on e.card_id::text = c.card_id
    where e.effective_session_id is not null
      and e.event_name in (
          'session_started', 'dextr_query',
          'swipe_right', 'swipe_left', 'saved', 'card_saved',
          'share', 'card_shared', 'deck_shared',
          'detail_view_open', 'detail_open',
          'opened_website', 'book_button_click', 'book_with_deck'
      )
    group by e.effective_session_id
)

select
    sb.*,

    -- Prompts
    sp.prompts_detail,
    coalesce(sp.prompt_count, 0) as explorer_prompt_count,

    -- Cards generated
    sc.cards_generated_detail,
    coalesce(sc.total_cards_generated, 0) as total_cards_generated,
    coalesce(sc.cards_liked, 0) as cards_liked,
    coalesce(sc.cards_disliked, 0) as cards_disliked,

    -- Saves
    ss.saves_detail,

    -- Shares
    sh.shares_detail,
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
