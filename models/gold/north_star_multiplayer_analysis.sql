-- Multiplayer engagement analysis
-- Treated as its own product feature, NOT a share proxy

with multiplayer_actions_weekly as (
    -- Action-level metrics from our unified model
    select
        action_week as metric_week,
        action_type,
        count(*) as action_count,
        count(distinct user_id) as unique_users,
        count(distinct multiplayer_id) as unique_sessions,
        count(case when is_ai_generated then 1 end) as ai_initiated_count
    from {{ ref('int_user_multiplayer_actions') }}
    group by action_week, action_type
),

session_quality_weekly as (
    -- Session-level metrics from stg_multiplayer (requires 2+ participants)
    select
        date_trunc('week', created_at)::date as metric_week,
        count(*) as true_multiplayer_sessions,
        avg(total_participants) as avg_participants,
        percentile_cont(0.5) within group (order by total_participants) as median_participants,
        sum(case when total_votes > 0 then 1 else 0 end) as sessions_with_voting,
        avg(case when total_votes > 0 then highest_consensus_percent end) as avg_consensus_pct,
        sum(case when collaboration_effectiveness = 'Clear Consensus' then 1 else 0 end) as sessions_with_clear_consensus,
        sum(case when collaboration_effectiveness in ('Clear Consensus', 'Multiple Strong Options') then 1 else 0 end) as sessions_with_strong_consensus,
        avg(total_votes) as avg_votes_per_session,
        avg(case when total_votes > 0 then overall_like_rate end) as avg_like_rate
    from {{ ref('stg_multiplayer') }}
    group by date_trunc('week', created_at)::date
),

pivoted_actions as (
    -- Pivot action types into columns
    select
        metric_week,
        sum(case when action_type = 'multiplayer_create' then action_count else 0 end) as sessions_created,
        sum(case when action_type = 'multiplayer_create' then unique_users else 0 end) as creators,
        sum(case when action_type = 'multiplayer_join' then action_count else 0 end) as joins,
        sum(case when action_type = 'multiplayer_join' then unique_users else 0 end) as joiners,
        sum(case when action_type = 'multiplayer_create' then ai_initiated_count else 0 end) as ai_initiated_sessions
    from multiplayer_actions_weekly
    group by metric_week
)

select
    coalesce(pa.metric_week, sq.metric_week) as metric_week,

    -- Creation metrics
    coalesce(pa.sessions_created, 0) as sessions_created,
    coalesce(pa.creators, 0) as unique_creators,

    -- Join metrics
    coalesce(pa.joins, 0) as total_joins,
    coalesce(pa.joiners, 0) as unique_joiners,

    -- True multiplayer sessions (2+ participants)
    coalesce(sq.true_multiplayer_sessions, 0) as true_multiplayer_sessions,
    round(100.0 * coalesce(sq.true_multiplayer_sessions, 0) / nullif(pa.sessions_created, 0), 2) as collaboration_rate,

    -- Participant metrics
    round(sq.avg_participants, 2) as avg_participants,
    sq.median_participants,

    -- Voting engagement
    coalesce(sq.sessions_with_voting, 0) as sessions_with_voting,
    round(100.0 * coalesce(sq.sessions_with_voting, 0) / nullif(sq.true_multiplayer_sessions, 0), 2) as voting_rate,
    round(sq.avg_votes_per_session, 1) as avg_votes_per_session,
    round(sq.avg_like_rate, 2) as avg_like_rate,

    -- Consensus metrics
    coalesce(sq.sessions_with_clear_consensus, 0) as sessions_with_clear_consensus,
    round(100.0 * coalesce(sq.sessions_with_clear_consensus, 0) / nullif(sq.true_multiplayer_sessions, 0), 2) as clear_consensus_rate,
    coalesce(sq.sessions_with_strong_consensus, 0) as sessions_with_strong_consensus,
    round(sq.avg_consensus_pct, 2) as avg_consensus_pct,

    -- AI-initiated sessions
    coalesce(pa.ai_initiated_sessions, 0) as ai_initiated_sessions,
    round(100.0 * coalesce(pa.ai_initiated_sessions, 0) / nullif(pa.sessions_created, 0), 2) as ai_source_rate,

    -- Virality: joins per session
    round(coalesce(pa.joins, 0)::numeric / nullif(pa.sessions_created, 0), 2) as joins_per_session,

    -- Engagement ratio: joiners / creators
    round(coalesce(pa.joiners, 0)::numeric / nullif(pa.creators, 0), 2) as joiner_to_creator_ratio

from pivoted_actions pa
full outer join session_quality_weekly sq on pa.metric_week = sq.metric_week
order by metric_week desc
