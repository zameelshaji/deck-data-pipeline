{{ config(materialized='table') }}

-- Social/Sharing Analytics — Viral Loop
-- Grain: one row per share event (share_link)
-- Answers: "What % of shares get opened?", "Do share recipients convert to signups?",
--          "K-factor?", "Share rate by user type?"

with shares as (
    select
        sl.id as share_link_id,
        sl.sharer_user_id,
        sl.share_type,
        sl.share_channel,
        sl.created_at as shared_at,
        date(sl.created_at) as share_date,
        sl.board_id,
        sl.card_id,
        sl.session_id
    from {{ ref('src_share_links') }} sl
    inner join {{ ref('stg_users') }} u on sl.sharer_user_id = u.user_id
    where u.is_test_user = 0
),

-- Viewer interactions per share link
share_viewers as (
    select
        si.share_link_id,
        count(distinct coalesce(si.viewer_user_id::text, si.viewer_anon_id)) as unique_viewers,
        count(distinct si.viewer_user_id) filter (
            where si.viewer_user_id is not null
        ) as unique_authenticated_viewers,
        count(distinct si.viewer_anon_id) filter (
            where si.viewer_user_id is null and si.viewer_anon_id is not null
        ) as unique_anonymous_viewers,
        count(*) as total_interactions,
        min(si.interaction_timestamp) as first_view_at
    from {{ ref('stg_share_interactions_clean') }} si
    group by si.share_link_id
),

-- Track signups that came from share viewers
-- A viewer who signed up after viewing the share
viewer_signups as (
    select
        si.share_link_id,
        count(distinct u.user_id) as viewers_who_signed_up,
        min(u.created_at) as first_signup_at
    from {{ ref('stg_share_interactions_clean') }} si
    inner join {{ ref('stg_users') }} u
        on si.viewer_user_id = u.user_id
        and u.created_at > si.interaction_timestamp - interval '1 hour'
    group by si.share_link_id
),

-- Track viewers who saved after viewing
viewer_saves as (
    select
        si.share_link_id,
        count(distinct si.viewer_user_id) filter (
            where si.interaction_type = 'save' or si.interaction_type = 'saved'
        ) as viewers_who_saved
    from {{ ref('stg_share_interactions_clean') }} si
    where si.viewer_user_id is not null
    group by si.share_link_id
),

-- Sharer archetype from fct_user_segments
sharer_info as (
    select
        user_id,
        user_archetype as sharer_archetype
    from {{ ref('fct_user_segments') }}
)

select
    s.share_link_id,
    s.sharer_user_id,
    si.sharer_archetype,
    s.share_type,
    s.share_channel,
    s.shared_at,
    s.share_date,
    s.board_id,
    s.card_id,
    s.session_id,

    -- Reach
    coalesce(sv.total_interactions, 0) as total_views,
    coalesce(sv.unique_viewers, 0) as unique_viewers,
    coalesce(sv.unique_authenticated_viewers, 0) as unique_authenticated_viewers,
    coalesce(sv.unique_anonymous_viewers, 0) as unique_anonymous_viewers,
    coalesce(sv.total_interactions, 0) as total_interactions,

    -- Conversions
    coalesce(vs.viewers_who_signed_up, 0) as viewers_who_signed_up,
    coalesce(vsv.viewers_who_saved, 0) as viewers_who_saved,
    case
        when coalesce(sv.unique_viewers, 0) > 0
        then round(coalesce(vs.viewers_who_signed_up, 0)::numeric / sv.unique_viewers, 4)
        else null
    end as signup_conversion_rate,

    -- K-factor (signups per share)
    coalesce(vs.viewers_who_signed_up, 0)::numeric as effective_k_factor,

    -- Timing
    case
        when sv.first_view_at is not null
        then round(extract(epoch from (sv.first_view_at - s.shared_at)) / 60.0, 1)
        else null
    end as time_to_first_view_minutes,
    case
        when vs.first_signup_at is not null
        then round(extract(epoch from (vs.first_signup_at - s.shared_at)) / 60.0, 1)
        else null
    end as time_to_first_signup_minutes

from shares s
left join share_viewers sv on s.share_link_id = sv.share_link_id
left join viewer_signups vs on s.share_link_id = vs.share_link_id
left join viewer_saves vsv on s.share_link_id = vsv.share_link_id
left join sharer_info si on s.sharer_user_id = si.user_id
