{{ config(materialized='table') }}

with card_share_events as (
    select
        user_id,
        event_timestamp as shared_at,
        'card_share' as share_source,
        'medium' as attribution_confidence
    from {{ ref('int_inferred_session_events') }}
    where event_type = 'card_share'
),

-- Share links attributed by user + time window
share_link_events as (
    select
        sl.sharer_user_id as user_id,
        sl.created_at as shared_at,
        'share_link' as share_source,
        'low' as attribution_confidence
    from {{ ref('src_share_links') }} sl
    where sl.sharer_user_id is not null
      and sl.created_at is not null
      and sl.session_id is null  -- only unattributed share links (no native session)
),

all_shares as (
    select * from card_share_events
    union all
    select * from share_link_events
),

shares_with_sessions as (
    select
        s.session_id,
        a.user_id,
        a.shared_at,
        a.share_source,
        a.attribution_confidence
    from all_shares a
    inner join {{ ref('int_inferred_sessions') }} s
        on a.user_id = s.user_id
        and a.shared_at between s.started_at and s.ended_at
)

select
    session_id,
    user_id,
    count(*) as share_count,
    min(shared_at) as first_share_at,
    max(shared_at) as last_share_at,
    -- Use the best confidence level for the session
    case
        when bool_or(attribution_confidence = 'medium') then 'medium'
        else 'low'
    end as share_attribution_confidence
from shares_with_sessions
group by session_id, user_id
