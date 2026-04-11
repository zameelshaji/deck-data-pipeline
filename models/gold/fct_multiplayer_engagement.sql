{{ config(materialized='table') }}

-- Multiplayer Engagement — Per-multiplayer_id adoption + funnel
--
-- Grain: one row per multiplayer_id (the UUID assigned at creation).
-- Answers: "How often do multiplayer sessions get joined / voted in?"
--          "What's the attach rate to boards (source_board_id)?"
--          "Which creators drive the most multiplayer engagement?"
--
-- Feeds a future Multiplayer dashboard page; also a data source for
-- fct_session_outcomes attribution of multiplayer → PSR.

with mp_events as (
    -- All multiplayer events. multiplayer_id lives as a top-level column
    -- on app_events (not in properties) — passed through from bronze via
    -- stg_app_events_enriched.
    select
        effective_session_id as session_id,
        user_id,
        event_name,
        event_timestamp,
        multiplayer_id,
        source_board_id,
        places_count
    from {{ ref('stg_app_events_enriched') }}
    inner join {{ ref('stg_users') }} using (user_id)
    where event_name in ('multiplayer_created', 'multiplayer_joined', 'multiplayer_voted')
      and is_test_user = 0
      and multiplayer_id is not null
),

-- The creator is the user whose multiplayer_created event matches the id
creators as (
    select
        multiplayer_id,
        user_id as creator_user_id,
        session_id as creator_session_id,
        event_timestamp as created_at,
        source_board_id,
        places_count
    from mp_events
    where event_name = 'multiplayer_created'
),

-- Joiners — count distinct users who joined the multiplayer
joiners as (
    select
        multiplayer_id,
        count(distinct user_id) as distinct_joiners,
        min(event_timestamp) as first_join_at
    from mp_events
    where event_name = 'multiplayer_joined'
    group by multiplayer_id
),

-- Voters — count distinct users and total vote events
voters as (
    select
        multiplayer_id,
        count(*) as total_votes,
        count(distinct user_id) as distinct_voters,
        min(event_timestamp) as first_vote_at,
        max(event_timestamp) as last_vote_at
    from mp_events
    where event_name = 'multiplayer_voted'
    group by multiplayer_id
),

-- Share link (if any) for this multiplayer session
mp_shares as (
    select
        multiplayer_id,
        count(*) as shares_count,
        min(created_at) as first_shared_at
    from {{ ref('src_share_links') }}
    where share_type = 'multiplayer'
      and multiplayer_id is not null
    group by multiplayer_id
),

-- Post-share interactions (opens, joins via link, etc.)
mp_share_interactions as (
    select
        sl.multiplayer_id,
        count(*) as share_interaction_count,
        count(distinct coalesce(si.viewer_user_id::text, si.viewer_anon_id::text)) as share_unique_viewers
    from {{ ref('src_share_links') }} sl
    inner join {{ ref('stg_share_interactions_clean') }} si
        on sl.id = si.share_link_id
    where sl.share_type = 'multiplayer'
      and sl.multiplayer_id is not null
    group by sl.multiplayer_id
)

select
    c.multiplayer_id,
    c.creator_user_id,
    c.creator_session_id,
    c.created_at,
    date(c.created_at) as created_date,
    c.source_board_id,
    c.places_count as initial_places_count,

    -- Adoption funnel
    coalesce(j.distinct_joiners, 0) as distinct_joiners,
    coalesce(v.distinct_voters, 0) as distinct_voters,
    coalesce(v.total_votes, 0) as total_votes,

    -- Funnel flags
    coalesce(j.distinct_joiners, 0) > 0 as was_joined,
    coalesce(v.total_votes, 0) > 0 as was_voted_on,

    -- Share fanout
    coalesce(s.shares_count, 0) as share_count,
    coalesce(si.share_interaction_count, 0) as share_interaction_count,
    coalesce(si.share_unique_viewers, 0) as share_unique_viewers,

    -- Timing
    j.first_join_at,
    v.first_vote_at,
    v.last_vote_at,
    case
        when j.first_join_at is not null
        then extract(epoch from (j.first_join_at - c.created_at)) / 60.0
    end as minutes_to_first_join,
    case
        when v.first_vote_at is not null and v.last_vote_at is not null
        then extract(epoch from (v.last_vote_at - v.first_vote_at)) / 60.0
    end as voting_duration_minutes

from creators c
left join joiners j on c.multiplayer_id = j.multiplayer_id
left join voters v on c.multiplayer_id = v.multiplayer_id
left join mp_shares s on c.multiplayer_id = s.multiplayer_id
left join mp_share_interactions si on c.multiplayer_id = si.multiplayer_id
