-- Dextr Query Performance by User
-- Analyzes how each Dextr query performs with detailed card-level interactions
-- One row per query (pack) showing all user interactions with that pack

with
    query_details as (
        select
            query_id,
            user_id,
            query_text,
            query_timestamp,
            response_pack_id as pack_id,
            processing_time,
            query_context,
            date(query_timestamp) as query_date

        from {{ source("public", "dextr_queries") }}
    ),

    pack_card_interactions as (
        select
            dpc.pack_id,

            -- Cards viewed (shown to user)
            count(case when dpc.shown_to_user = true then 1 end) as cards_viewed,

            -- Cards liked (swiped right)
            count(case when dpc.user_action = 'right' then 1 end) as cards_liked,

            -- Cards disliked (swiped left)
            count(case when dpc.user_action = 'left' then 1 end) as cards_disliked,

            -- Total cards in pack
            count(*) as total_cards_in_pack

        from {{ ref("src_dextr_pack_cards") }} dpc
        group by dpc.pack_id
    ),

    pack_saves as (
        select
            cca.source_id::bigint as pack_id,
            count(distinct cca.card_id) as cards_saved

        from {{ ref("src_core_card_actions") }} cca
        where
            cca.action_type = 'saved'
            and cca.source = 'dextr'
            and cca.source_id ~ '^[0-9]+$'  -- Only numeric values
        group by cca.source_id::bigint
    ),

    user_info as (
        select
            user_id,
            username,
            email,
            created_at as user_created_at
        from {{ ref("stg_users") }}
    ),

    user_segments as (
        select
            user_id,
            user_type
        from {{ ref("user_segmentation") }}
    )

select
    -- Date dimension
    qd.query_date as date,

    -- User identity
    qd.user_id,
    ui.username as user_name,
    ui.email as user_email,

    -- User segment
    coalesce(us.user_type, 'Passenger') as user_type,

    -- Query details
    qd.query_id,
    qd.query_text,
    qd.query_timestamp,
    qd.processing_time,

    -- Query context (stored as JSONB for flexibility)
    qd.query_context,
    qd.query_context ->> 'location' as location,
    qd.query_context ->> 'app_version' as app_version,
    qd.query_context ->> 'session_id' as session_id,

    -- Pack details
    qd.pack_id,
    coalesce(pci.total_cards_in_pack, 0) as total_cards_in_pack,

    -- Card interactions
    coalesce(pci.cards_viewed, 0) as cards_viewed,
    coalesce(pci.cards_liked, 0) as cards_liked,
    coalesce(pci.cards_disliked, 0) as cards_disliked,
    coalesce(ps.cards_saved, 0) as cards_saved,

    -- Performance metrics
    case
        when pci.cards_viewed > 0
        then round(100.0 * pci.cards_liked / pci.cards_viewed, 2)
        else 0
    end as like_rate,

    case
        when pci.cards_viewed > 0
        then round(100.0 * ps.cards_saved / pci.cards_viewed, 2)
        else 0
    end as save_rate,

    case
        when pci.total_cards_in_pack > 0
        then round(100.0 * pci.cards_viewed / pci.total_cards_in_pack, 2)
        else 0
    end as view_completion_rate,

    -- Engagement flags
    case when pci.cards_viewed > 0 then 1 else 0 end as query_engaged,
    case when pci.cards_liked > 0 then 1 else 0 end as query_had_likes,
    case when ps.cards_saved > 0 then 1 else 0 end as query_had_saves

from query_details qd
left join pack_card_interactions pci on qd.pack_id = pci.pack_id
left join pack_saves ps on qd.pack_id = ps.pack_id
left join user_info ui on qd.user_id = ui.user_id
left join user_segments us on qd.user_id = us.user_id

order by qd.query_timestamp desc
