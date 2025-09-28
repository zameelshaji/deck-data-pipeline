-- Multiplayer collaboration analytics - TRUE multiplayer sessions only
with
    session_details as (
        select
            ms.multiplayer_id,
            ms.creator_id,
            ms.title,
            ms.status,
            ms.max_participants,
            ms.created_at,
            ms.updated_at,
            ms.expires_at,
            ms.source_type,
            ms.ai_prompt,

            -- Participant metrics
            count(distinct sp.user_id) as total_participants,

            -- Activity timing
            min(sp.joined_at) as first_join,
            max(sp.last_active) as last_activity,

            -- Session duration
            extract(
                epoch from (max(sp.last_active) - min(sp.joined_at))
            ) as session_duration_hours

        from {{ ref("src_multiplayer_sessions") }} ms
        left join
            {{ ref("src_session_participants") }} sp
            on ms.multiplayer_id = sp.multiplayer_id
        group by
            ms.multiplayer_id,
            ms.creator_id,
            ms.title,
            ms.status,
            ms.max_participants,
            ms.created_at,
            ms.updated_at,
            ms.expires_at,
            ms.source_type,
            ms.ai_prompt

        -- FILTER: Only include sessions with 2+ participants (true collaboration)
        having count(distinct sp.user_id) >= 2
    ),

    place_voting as (
        select
            sv.multiplayer_id,
            count(distinct sp.card_id) as total_places,
            count(*) as total_votes,
            count(case when sv.vote_type = 'like' then 1 end) as like_votes,
            count(case when sv.vote_type = 'pass' then 1 end) as pass_votes,
            count(distinct sv.user_id) as voting_participants
        from {{ ref("src_session_votes") }} sv
        -- Only include votes from sessions that passed our participant filter
        inner join
            (select distinct multiplayer_id from session_details) filtered_sessions
            on sv.multiplayer_id = filtered_sessions.multiplayer_id
        left join {{ ref("src_session_places") }} sp on sv.place_id = sp.id
        group by sv.multiplayer_id
    ),

    consensus_analysis as (
        select
            sv.multiplayer_id,
            sp.card_id,
            count(*) as votes_for_place,
            count(case when sv.vote_type = 'like' then 1 end) as likes,
            count(case when sv.vote_type = 'pass' then 1 end) as passes,
            round(
                100.0 * count(case when sv.vote_type = 'like' then 1 end) / count(*), 2
            ) as like_percentage
        from {{ ref("src_session_votes") }} sv
        -- Only include votes from sessions that passed our participant filter
        inner join
            (select distinct multiplayer_id from session_details) filtered_sessions
            on sv.multiplayer_id = filtered_sessions.multiplayer_id
        left join {{ ref("src_session_places") }} sp on sv.place_id = sp.id
        group by sv.multiplayer_id, sp.card_id
    ),

    top_consensus as (
        select
            multiplayer_id,
            max(like_percentage) as highest_consensus_percent,
            count(case when like_percentage >= 75 then 1 end) as high_consensus_places,
            -- Average consensus across all places in session
            round(avg(like_percentage), 2) as average_consensus_percent
        from consensus_analysis
        group by multiplayer_id
    ),

    places_with_max_consensus as (
        select ca.multiplayer_id, count(*) as places_with_top_consensus
        from consensus_analysis ca
        inner join top_consensus tc on ca.multiplayer_id = tc.multiplayer_id
        where ca.like_percentage = tc.highest_consensus_percent
        group by ca.multiplayer_id
    )

select
    sd.*,

    -- Place and voting metrics
    coalesce(pv.total_places, 0) as total_places,
    coalesce(pv.total_votes, 0) as total_votes,
    coalesce(pv.like_votes, 0) as like_votes,
    coalesce(pv.pass_votes, 0) as pass_votes,
    coalesce(pv.voting_participants, 0) as voting_participants,

    -- Consensus metrics (enhanced for multiple top consensus)
    coalesce(tc.highest_consensus_percent, 0) as highest_consensus_percent,
    coalesce(tc.high_consensus_places, 0) as high_consensus_places,
    coalesce(pmc.places_with_top_consensus, 0) as places_with_top_consensus,
    coalesce(tc.average_consensus_percent, 0) as average_consensus_percent,

    -- Engagement calculations
    case
        when sd.total_participants > 0
        then
            round(
                100.0 * coalesce(pv.voting_participants, 0) / sd.total_participants, 2
            )
        else 0
    end as participant_engagement_rate,

    case
        when coalesce(pv.total_votes, 0) > 0
        then round(100.0 * coalesce(pv.like_votes, 0) / pv.total_votes, 2)
        else 0
    end as overall_like_rate,

    -- Session success indicators (updated for true collaboration)
    case
        when sd.status = 'completed' and tc.high_consensus_places > 0
        then 'Successful - Consensus Reached'
        when sd.status = 'completed' and sd.total_participants >= 3
        then 'Completed - Group Decision'
        when sd.status = 'completed'
        then 'Completed - Pair Decision'
        when sd.status = 'active' and sd.total_participants >= 3
        then 'Active - Group Planning'
        when sd.status = 'active'
        then 'Active - Pair Planning'
        else 'Inactive'
    end as session_outcome,

    -- Collaboration quality (enhanced for multi-participant focus)
    case
        when sd.total_participants >= 4 and pv.voting_participants >= 3
        then 'Large Group Collaboration'
        when sd.total_participants >= 3 and pv.voting_participants >= 2
        then 'Group Collaboration'
        when sd.total_participants = 2 and pv.voting_participants >= 1
        then 'Pair Collaboration'
        else 'Limited Collaboration'
    end as collaboration_level,

    -- Additional insight: Collaboration effectiveness (enhanced for multiple consensus)
    case
        when tc.highest_consensus_percent >= 75 and pmc.places_with_top_consensus = 1
        then 'Clear Consensus'
        when tc.highest_consensus_percent >= 75 and pmc.places_with_top_consensus > 1
        then 'Multiple Strong Options'
        when tc.highest_consensus_percent >= 50 and pmc.places_with_top_consensus <= 2
        then 'Moderate Agreement'
        when tc.average_consensus_percent >= 50
        then 'Generally Positive'
        when pv.total_votes > 0
        then 'Mixed Opinions'
        else 'No Clear Direction'
    end as collaboration_effectiveness

from session_details sd
left join place_voting pv on sd.multiplayer_id = pv.multiplayer_id
left join top_consensus tc on sd.multiplayer_id = tc.multiplayer_id
left join places_with_max_consensus pmc on sd.multiplayer_id = pmc.multiplayer_id
