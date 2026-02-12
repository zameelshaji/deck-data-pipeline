{{ config(materialized='table') }}

-- Dextr Pack Analytics
-- Grain: one row per pack_id
-- Answers: "Which packs lead to shortlist completions?",
--          "Which prompt types lead to highest save rates?",
--          "Average cards generated per prompt — more or fewer better?"

with pack_queries as (
    select
        di.pack_id,
        di.query_id,
        di.user_id,
        di.query_text,
        date(di.query_timestamp) as query_date,
        di.query_timestamp,
        di.location,
        di.app_version,
        di.processing_time_secs,
        di.total_cards,
        di.cards_shown,
        di.cards_liked,
        di.cards_disliked,
        di.cards_acted_upon
    from {{ ref('stg_dextr_interactions') }} di
    inner join {{ ref('stg_users') }} u on di.user_id = u.user_id
    where u.is_test_user = 0
      and di.pack_id is not null
),

-- Map packs to sessions
pack_sessions as (
    select
        pq.pack_id,
        s.session_id,
        s.has_save as session_has_save,
        s.has_share as session_has_share,
        s.has_post_share_interaction as session_has_post_share
    from pack_queries pq
    inner join {{ ref('fct_session_outcomes') }} s
        on pq.user_id = s.user_id
        and pq.query_timestamp between s.started_at and s.ended_at
),

-- Count saves per pack from board_places_v2
-- Legacy packs: card_id in dextr_pack_cards
-- Current packs: place_deck_sku in dextr_places
pack_saves_legacy as (
    select
        dpc.pack_id,
        count(distinct bp.place_id) as cards_saved
    from {{ ref('src_dextr_pack_cards') }} dpc
    inner join {{ ref('src_board_places_v2') }} bp
        on bp.place_id::text = dpc.card_id::text
        and bp.added_by = (
            select dq.user_id from {{ ref('src_dextr_queries') }} dq
            where dq.response_pack_id = dpc.pack_id limit 1
        )
    group by dpc.pack_id
),

pack_saves_current as (
    select
        dp.pack_id,
        count(distinct bp.place_id) as cards_saved
    from {{ ref('src_dextr_places') }} dp
    inner join {{ ref('src_places') }} pl on dp.place_id = pl.place_id
    inner join {{ ref('src_board_places_v2') }} bp
        on bp.place_id::text = pl.place_id::text
    inner join {{ ref('src_dextr_queries') }} dq
        on dp.pack_id = dq.response_pack_id
        and bp.added_by = dq.user_id
    group by dp.pack_id
),

pack_saves_combined as (
    select pack_id, cards_saved from pack_saves_legacy
    union all
    select pack_id, cards_saved from pack_saves_current
)

select
    pq.pack_id,
    pq.query_id,
    pq.user_id,
    pq.query_text,
    pq.query_date,
    pq.query_timestamp,
    pq.location,
    pq.app_version,
    pq.processing_time_secs,

    -- Pack content
    coalesce(pq.total_cards, 0) as total_cards_generated,
    coalesce(pq.cards_shown, 0) as cards_shown,
    coalesce(pq.cards_acted_upon, 0) as cards_swiped,
    coalesce(pq.cards_liked, 0) as cards_liked,
    coalesce(pq.cards_disliked, 0) as cards_disliked,
    coalesce(ps_c.cards_saved, 0) as cards_saved,

    -- Rates
    case
        when coalesce(pq.total_cards, 0) > 0
        then round(coalesce(pq.cards_shown, 0)::numeric / pq.total_cards, 4)
        else null
    end as completion_rate,
    case
        when coalesce(pq.cards_acted_upon, 0) > 0
        then round(pq.cards_liked::numeric / pq.cards_acted_upon, 4)
        else null
    end as like_rate,
    case
        when coalesce(pq.cards_shown, 0) > 0
        then round(coalesce(ps_c.cards_saved, 0)::numeric / pq.cards_shown, 4)
        else null
    end as save_rate,

    -- Shortlist flag (saved >= 3 cards)
    coalesce(ps_c.cards_saved, 0) >= 3 as has_shortlist,

    -- Session-level outcomes
    ps.session_id,
    coalesce(ps.session_has_save, false) as led_to_save,
    coalesce(ps.session_has_share, false) as led_to_share,
    coalesce(ps.session_has_post_share, false) as led_to_post_share

from pack_queries pq
left join pack_sessions ps on pq.pack_id = ps.pack_id
left join pack_saves_combined ps_c on pq.pack_id = ps_c.pack_id
