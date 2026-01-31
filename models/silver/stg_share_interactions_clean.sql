select
    si.id,
    si.share_link_id,
    si.viewer_user_id,
    si.viewer_anon_id,
    si.interaction_type,
    si.interaction_timestamp,
    si.is_sharer,

    -- Share link context
    sl.sharer_user_id,
    sl.session_id,
    sl.share_type,
    sl.share_channel,
    sl.board_id,
    sl.card_id,
    sl.created_at as share_created_at,

    -- Time since share
    extract(epoch from (si.interaction_timestamp - sl.created_at)) / 60.0 as time_since_share_minutes

from {{ ref('src_share_interactions') }} si
inner join {{ ref('src_share_links') }} sl
    on si.share_link_id = sl.id
where coalesce(si.is_sharer, false) = false
  and si.interaction_timestamp is not null
