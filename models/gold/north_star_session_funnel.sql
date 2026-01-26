-- Session funnel analysis showing drop-off at each stage
-- Tracks progression: Initiated → Browsed → Engaged → Saved → Shortlisted → Social

with session_funnel as (
    select
        session_week,

        -- Stage 1: Initiated - All planning sessions
        count(distinct case when is_planning_session then derived_session_id end) as initiated,

        -- Stage 2: Browsed - Had engagement beyond just starting
        count(distinct case
            when is_planning_session and total_events > 1
            then derived_session_id
        end) as browsed,

        -- Stage 3: Engaged - Positive action (swipe right or interaction)
        count(distinct case
            when is_planning_session and (has_swipe or unique_cards_viewed > 0)
            then derived_session_id
        end) as engaged,

        -- Stage 4: Saved - At least one save
        count(distinct case
            when is_planning_session and has_save
            then derived_session_id
        end) as saved,

        -- Stage 5: Shortlisted - 3+ saves (power user session)
        count(distinct case
            when is_planning_session and is_scr3
            then derived_session_id
        end) as shortlisted,

        -- Stage 6: Social - Share or multiplayer activity
        count(distinct case
            when is_planning_session and (has_card_share or has_multiplayer_activity)
            then derived_session_id
        end) as social,

        -- Stage 7: Converted - Booking/directions/website action
        count(distinct case
            when is_planning_session and has_conversion
            then derived_session_id
        end) as converted

    from {{ ref('north_star_session_metrics') }}
    group by session_week
)

select
    session_week,

    -- Absolute counts at each stage
    initiated,
    browsed,
    engaged,
    saved,
    shortlisted,
    social,
    converted,

    -- Conversion rates from start (percentage of initiated that reach each stage)
    100.0 as pct_initiated,
    round(100.0 * browsed / nullif(initiated, 0), 1) as pct_browsed,
    round(100.0 * engaged / nullif(initiated, 0), 1) as pct_engaged,
    round(100.0 * saved / nullif(initiated, 0), 1) as pct_saved,
    round(100.0 * shortlisted / nullif(initiated, 0), 1) as pct_shortlisted,
    round(100.0 * social / nullif(initiated, 0), 1) as pct_social,
    round(100.0 * converted / nullif(initiated, 0), 1) as pct_converted,

    -- Stage-to-stage conversion rates
    round(100.0 * browsed / nullif(initiated, 0), 1) as cvr_initiated_to_browsed,
    round(100.0 * engaged / nullif(browsed, 0), 1) as cvr_browsed_to_engaged,
    round(100.0 * saved / nullif(engaged, 0), 1) as cvr_engaged_to_saved,
    round(100.0 * shortlisted / nullif(saved, 0), 1) as cvr_saved_to_shortlisted,
    round(100.0 * social / nullif(shortlisted, 0), 1) as cvr_shortlisted_to_social,
    round(100.0 * converted / nullif(social, 0), 1) as cvr_social_to_converted,

    -- Drop-off rates at each stage (% that didn't progress)
    round(100.0 * (initiated - browsed) / nullif(initiated, 0), 1) as drop_initiated,
    round(100.0 * (browsed - engaged) / nullif(browsed, 0), 1) as drop_browsed,
    round(100.0 * (engaged - saved) / nullif(engaged, 0), 1) as drop_engaged,
    round(100.0 * (saved - shortlisted) / nullif(saved, 0), 1) as drop_saved,
    round(100.0 * (shortlisted - social) / nullif(shortlisted, 0), 1) as drop_shortlisted,

    -- WoW changes for key stages
    lag(initiated) over (order by session_week) as prev_week_initiated,
    lag(saved) over (order by session_week) as prev_week_saved,
    lag(shortlisted) over (order by session_week) as prev_week_shortlisted

from session_funnel
order by session_week desc
