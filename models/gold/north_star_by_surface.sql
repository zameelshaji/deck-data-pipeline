-- North Star metrics split by primary surface
-- Allows comparison across prompt/featured/multiplayer/board entry points

with surface_metrics as (
    select
        session_week,
        primary_surface,

        -- Volume
        count(distinct derived_session_id) as total_sessions,
        count(distinct user_id) as unique_users,

        -- Save metrics
        count(distinct case when has_save then derived_session_id end) as sessions_with_save,
        count(distinct case when is_scr3 then derived_session_id end) as sessions_with_scr3,
        sum(unique_saves) as total_saves,
        avg(case when has_save then unique_saves end) as avg_saves_per_saver,

        -- TTFS
        avg(case when has_save then ttfs_seconds end) as avg_ttfs_seconds,
        percentile_cont(0.5) within group (order by ttfs_seconds)
            filter (where has_save) as median_ttfs_seconds,

        -- Share metrics
        count(distinct case when has_card_share then derived_session_id end) as sessions_with_share,

        -- Multiplayer metrics
        count(distinct case when has_multiplayer_activity then derived_session_id end) as sessions_with_multiplayer,

        -- Conversion
        count(distinct case when has_conversion then derived_session_id end) as sessions_with_conversion,

        -- Session quality
        avg(session_duration_minutes) as avg_session_duration_min,
        avg(total_events) as avg_events_per_session

    from {{ ref('north_star_session_metrics') }}
    where is_planning_session = true
    group by session_week, primary_surface
)

select
    session_week,
    primary_surface,

    -- Volume
    total_sessions,
    unique_users,

    -- SSR by surface
    sessions_with_save,
    round(100.0 * sessions_with_save / nullif(total_sessions, 0), 2) as ssr_percent,

    -- SCR3 by surface
    sessions_with_scr3,
    round(100.0 * sessions_with_scr3 / nullif(total_sessions, 0), 2) as scr3_percent,

    -- Save depth
    total_saves,
    round(avg_saves_per_saver, 2) as avg_saves_per_saver,

    -- TTFS by surface
    round(avg_ttfs_seconds, 1) as avg_ttfs_seconds,
    round(median_ttfs_seconds, 1) as median_ttfs_seconds,

    -- Share rate by surface
    sessions_with_share,
    round(100.0 * sessions_with_share / nullif(total_sessions, 0), 2) as share_rate,

    -- Multiplayer rate by surface
    sessions_with_multiplayer,
    round(100.0 * sessions_with_multiplayer / nullif(total_sessions, 0), 2) as multiplayer_rate,

    -- Conversion rate by surface
    sessions_with_conversion,
    round(100.0 * sessions_with_conversion / nullif(total_sessions, 0), 2) as conversion_rate,

    -- Session quality by surface
    round(avg_session_duration_min, 2) as avg_session_duration_min,
    round(avg_events_per_session, 1) as avg_events_per_session,

    -- Surface share of total sessions (calculate % of week's sessions)
    round(100.0 * total_sessions / nullif(
        sum(total_sessions) over (partition by session_week), 0
    ), 2) as pct_of_week_sessions

from surface_metrics
order by session_week desc, total_sessions desc
