-- Per-session North Star metrics and flags
-- Foundation for weekly aggregations and funnel analysis

with session_events_agg as (
    select
        derived_session_id,
        user_id,

        -- Save metrics
        max(case when event_name in ('saved', 'board_save') then 1 else 0 end) as has_save,
        max(case when event_name = 'saved' then 1 else 0 end) as has_direct_save,
        max(case when event_name = 'board_save' then 1 else 0 end) as has_board_save,
        count(case when event_name in ('saved', 'board_save') then 1 end) as total_saves,
        count(distinct case when event_name in ('saved', 'board_save') then card_id end) as unique_saves,

        -- Time to first save (TTFS) in seconds
        min(case when event_name in ('saved', 'board_save') then seconds_since_session_start end) as ttfs_seconds,

        -- Share metrics (card shares only, NOT multiplayer)
        max(case when event_name = 'card_share' then 1 else 0 end) as has_card_share,
        count(case when event_name = 'card_share' then 1 end) as card_shares_count,

        -- Multiplayer metrics (separate from shares)
        max(case when event_name = 'multiplayer_create' then 1 else 0 end) as has_multiplayer_create,
        max(case when event_name = 'multiplayer_join' then 1 else 0 end) as has_multiplayer_join,
        max(case when event_source = 'multiplayer' then 1 else 0 end) as has_multiplayer_activity,

        -- Swipe engagement
        max(case when event_name = 'swipe_right' then 1 else 0 end) as has_swipe,
        count(case when event_name = 'swipe_right' then 1 end) as swipes_right,
        count(case when event_name = 'swipe_left' then 1 end) as swipes_left,
        count(case when event_name in ('swipe_right', 'swipe_left') then 1 end) as total_swipes,

        -- Conversion metrics
        max(case when event_category = 'Conversion Action' then 1 else 0 end) as has_conversion,
        count(case when event_category = 'Conversion Action' then 1 end) as conversion_actions,

        -- Content discovery
        count(distinct card_id) filter (where card_id is not null) as unique_cards_viewed,

        -- Event totals
        count(*) as total_events,
        count(distinct event_source) as unique_surfaces

    from {{ ref('int_session_events') }}
    group by derived_session_id, user_id
),

sessions as (
    select
        derived_session_id,
        user_id,
        session_number,
        session_start,
        session_end,
        session_duration_seconds,
        session_duration_minutes,
        event_count,
        unique_cards,
        primary_surface,
        has_prompt,
        has_featured,
        has_multiplayer,
        has_board,
        is_planning_session,
        session_date,
        session_week,
        session_hour,
        session_day_of_week
    from {{ ref('int_derived_sessions') }}
)

select
    s.derived_session_id,
    s.user_id,
    s.session_number,
    s.session_start,
    s.session_end,
    s.session_date,
    s.session_week,
    s.session_hour,
    s.session_day_of_week,
    s.session_duration_seconds,
    s.session_duration_minutes,
    s.primary_surface,
    s.is_planning_session,

    -- Surface flags
    s.has_prompt,
    s.has_featured,
    s.has_multiplayer as used_multiplayer,
    s.has_board as used_board,

    -- Save Metrics
    coalesce(e.has_save, 0)::boolean as has_save,
    coalesce(e.has_direct_save, 0)::boolean as has_direct_save,
    coalesce(e.has_board_save, 0)::boolean as has_board_save,
    coalesce(e.unique_saves, 0) as unique_saves,
    coalesce(e.total_saves, 0) as total_saves,

    -- SSR and SCR3 flags
    coalesce(e.has_save, 0)::boolean as is_ssr,
    case when coalesce(e.unique_saves, 0) >= 3 then true else false end as is_scr3,

    -- Time to First Save
    e.ttfs_seconds,
    round(coalesce(e.ttfs_seconds, 0) / 60.0, 2) as ttfs_minutes,

    -- Share Metrics (limited data)
    coalesce(e.has_card_share, 0)::boolean as has_card_share,
    coalesce(e.card_shares_count, 0) as card_shares_count,

    -- Multiplayer Metrics (separate from shares)
    coalesce(e.has_multiplayer_create, 0)::boolean as has_multiplayer_create,
    coalesce(e.has_multiplayer_join, 0)::boolean as has_multiplayer_join,
    coalesce(e.has_multiplayer_activity, 0)::boolean as has_multiplayer_activity,

    -- Engagement Metrics
    coalesce(e.has_swipe, 0)::boolean as has_swipe,
    coalesce(e.swipes_right, 0) as swipes_right,
    coalesce(e.swipes_left, 0) as swipes_left,
    coalesce(e.total_swipes, 0) as total_swipes,
    case
        when coalesce(e.total_swipes, 0) > 0
        then round(100.0 * e.swipes_right / e.total_swipes, 2)
        else null
    end as swipe_right_rate,
    coalesce(e.unique_cards_viewed, 0) as unique_cards_viewed,

    -- Conversion
    coalesce(e.has_conversion, 0)::boolean as has_conversion,
    coalesce(e.conversion_actions, 0) as conversion_actions,

    -- Combined Outcomes
    case
        when coalesce(e.has_save, 0) = 1 and coalesce(e.has_multiplayer_activity, 0) = 1 then true
        else false
    end as is_psr_save_and_multiplayer,

    case
        when coalesce(e.has_save, 0) = 1 and coalesce(e.has_card_share, 0) = 1 then true
        else false
    end as is_psr_save_and_share,

    -- No value session (planning session with no save, share, or multiplayer)
    case
        when s.is_planning_session
            and coalesce(e.has_save, 0) = 0
            and coalesce(e.has_card_share, 0) = 0
            and coalesce(e.has_multiplayer_activity, 0) = 0
        then true
        else false
    end as is_no_value,

    -- Session outcome categorization
    case
        when coalesce(e.has_save, 0) = 1 and coalesce(e.has_card_share, 0) = 1 and coalesce(e.has_multiplayer_activity, 0) = 1
            then 'full_loop'
        when coalesce(e.has_save, 0) = 1 and coalesce(e.has_multiplayer_activity, 0) = 1
            then 'save_and_multiplayer'
        when coalesce(e.has_save, 0) = 1 and coalesce(e.has_card_share, 0) = 1
            then 'save_and_share'
        when coalesce(e.has_save, 0) = 1
            then 'save_only'
        when coalesce(e.has_card_share, 0) = 1
            then 'share_only'
        when coalesce(e.has_multiplayer_activity, 0) = 1
            then 'multiplayer_only'
        else 'no_value'
    end as session_outcome,

    -- Event totals
    coalesce(e.total_events, s.event_count) as total_events,
    coalesce(e.unique_surfaces, 1) as unique_surfaces

from sessions s
left join session_events_agg e
    on s.derived_session_id = e.derived_session_id
