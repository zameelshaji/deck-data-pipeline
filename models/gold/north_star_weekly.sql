-- Weekly aggregated North Star metrics
-- Primary reporting model for SSR, SCR3, WAP, TTFS

with weekly_sessions as (
    select
        session_week,

        -- Volume metrics
        count(distinct derived_session_id) as total_sessions,
        count(distinct case when is_planning_session then derived_session_id end) as planning_sessions,
        count(distinct user_id) as total_users,
        count(distinct case when is_planning_session then user_id end) as planning_users,

        -- Save metrics (SSR ladder)
        count(distinct case when has_save then derived_session_id end) as sessions_with_save,
        count(distinct case when is_scr3 then derived_session_id end) as sessions_with_scr3,
        sum(unique_saves) as total_unique_saves,
        avg(case when has_save then unique_saves end) as avg_save_density,

        -- TTFS (Time to First Save)
        avg(case when has_save then ttfs_seconds end) as avg_ttfs_seconds,
        percentile_cont(0.5) within group (order by ttfs_seconds)
            filter (where has_save) as median_ttfs_seconds,

        -- Share metrics (limited data)
        count(distinct case when has_card_share then derived_session_id end) as sessions_with_card_share,
        sum(card_shares_count) as total_card_shares,

        -- Multiplayer metrics (separate from shares)
        count(distinct case when has_multiplayer_create then derived_session_id end) as sessions_with_multiplayer_create,
        count(distinct case when has_multiplayer_activity then derived_session_id end) as sessions_with_multiplayer_activity,

        -- Combined outcomes
        count(distinct case when is_psr_save_and_multiplayer then derived_session_id end) as sessions_save_and_multiplayer,
        count(distinct case when is_no_value and is_planning_session then derived_session_id end) as sessions_no_value,

        -- Engagement metrics
        count(distinct case when has_swipe then derived_session_id end) as sessions_with_swipe,
        count(distinct case when has_conversion then derived_session_id end) as sessions_with_conversion,

        -- Session quality
        avg(session_duration_minutes) as avg_session_duration_min,
        percentile_cont(0.5) within group (order by session_duration_minutes) as median_session_duration_min,
        avg(total_events) as avg_events_per_session,

        -- WAP (Weekly Active Planners) - users with meaningful activity
        count(distinct case when has_save or has_multiplayer_activity then user_id end) as wap,
        count(distinct case when has_save then user_id end) as wap_savers,
        count(distinct case when has_multiplayer_activity then user_id end) as wap_multiplayer

    from {{ ref('north_star_session_metrics') }}
    group by session_week
)

select
    session_week,

    -- Volume
    total_sessions,
    planning_sessions,
    total_users,
    planning_users,

    -- SSR: Session Save Rate (among planning sessions)
    sessions_with_save,
    round(100.0 * sessions_with_save / nullif(planning_sessions, 0), 2) as ssr,

    -- SCR3: Session Conversion Rate 3+ saves
    sessions_with_scr3,
    round(100.0 * sessions_with_scr3 / nullif(planning_sessions, 0), 2) as scr3,

    -- Save density
    total_unique_saves,
    round(avg_save_density, 2) as avg_save_density,

    -- TTFS: Time to First Save
    round(avg_ttfs_seconds, 1) as avg_ttfs_seconds,
    round(median_ttfs_seconds, 1) as median_ttfs_seconds,

    -- Share metrics (note: significantly undercounted)
    sessions_with_card_share,
    round(100.0 * sessions_with_card_share / nullif(planning_sessions, 0), 2) as card_share_rate,
    total_card_shares,

    -- Multiplayer metrics (separate from shares)
    sessions_with_multiplayer_create,
    round(100.0 * sessions_with_multiplayer_create / nullif(planning_sessions, 0), 2) as multiplayer_create_rate,
    sessions_with_multiplayer_activity,
    round(100.0 * sessions_with_multiplayer_activity / nullif(planning_sessions, 0), 2) as multiplayer_activity_rate,

    -- Combined outcomes
    sessions_save_and_multiplayer,
    round(100.0 * sessions_save_and_multiplayer / nullif(planning_sessions, 0), 2) as psr_save_and_multiplayer,

    -- No value rate
    sessions_no_value,
    round(100.0 * sessions_no_value / nullif(planning_sessions, 0), 2) as nvr,

    -- Engagement
    sessions_with_swipe,
    sessions_with_conversion,
    round(100.0 * sessions_with_conversion / nullif(planning_sessions, 0), 2) as conversion_rate,

    -- Session quality
    round(avg_session_duration_min, 2) as avg_session_duration_min,
    round(median_session_duration_min, 2) as median_session_duration_min,
    round(avg_events_per_session, 1) as avg_events_per_session,

    -- WAP: Weekly Active Planners
    wap,
    wap_savers,
    wap_multiplayer,

    -- WoW growth calculations
    lag(total_sessions) over (order by session_week) as prev_week_sessions,
    round(100.0 * (total_sessions - lag(total_sessions) over (order by session_week))
        / nullif(lag(total_sessions) over (order by session_week), 0), 2) as sessions_wow_growth,

    lag(sessions_with_save) over (order by session_week) as prev_week_saves,
    lag(wap) over (order by session_week) as prev_week_wap,
    round(100.0 * (wap - lag(wap) over (order by session_week))
        / nullif(lag(wap) over (order by session_week), 0), 2) as wap_wow_growth

from weekly_sessions
order by session_week desc
