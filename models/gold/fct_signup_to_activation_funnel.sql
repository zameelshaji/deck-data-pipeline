-- Signup to Activation Funnel
-- Tracks conversion from signup through activation with weekly cohorts
-- Funnel steps: Signup → App Open → Planning Initiated → Content Engagement → Activated

with users as (
    select
        user_id,
        date(created_at) as signup_date,
        date_trunc('week', created_at)::date as signup_week,
        created_at as signup_timestamp
    from {{ ref('stg_users') }}
    where is_test_user = 0
      and created_at is not null
),

-- Get all events per user for funnel analysis
user_events as (
    select
        e.user_id,
        e.event_timestamp,
        date(e.event_timestamp) as event_date,
        e.event_name,
        e.event_source,
        e.event_category
    from {{ ref('stg_events') }} e
),

-- Get session-level data for activation tracking
user_sessions as (
    select
        user_id,
        session_date,
        has_save,
        has_share,
        is_prompt_session,
        initiation_surface
    from {{ ref('fct_session_outcomes') }}
),

-- Calculate funnel stages for each user within 7-day window
user_funnel as (
    select
        u.user_id,
        u.signup_date,
        u.signup_week,
        u.signup_timestamp,

        -- F1: App Open (any event within 7 days of signup)
        bool_or(
            e.event_date between u.signup_date and u.signup_date + 7
        ) as had_app_open_7d,

        -- F2: Planning Initiated (prompt, search, or browse featured within 7 days)
        bool_or(
            e.event_date between u.signup_date and u.signup_date + 7
            and (
                e.event_name = 'dextr_query'  -- AI prompt
                or e.event_source in ('featured_section', 'featured_carousel')  -- Browse featured
                or e.event_name in ('category_clicked', 'spotlight_click')  -- Browse/search
            )
        ) as had_planning_initiated_7d,

        -- F3: Content Engagement (swipe, view detail within 7 days)
        bool_or(
            e.event_date between u.signup_date and u.signup_date + 7
            and e.event_name in (
                'swipe_right', 'swipe_left',  -- Card swiping
                'detail_view_open', 'detail_open',  -- Viewing card details
                'click'  -- Click on content
            )
        ) as had_content_engagement_7d,

        -- First event timestamp for time-to-activation calculation
        min(e.event_timestamp) filter (
            where e.event_date between u.signup_date and u.signup_date + 7
        ) as first_event_timestamp

    from users u
    left join user_events e on u.user_id = e.user_id
    group by u.user_id, u.signup_date, u.signup_week, u.signup_timestamp
),

-- Get activation data from sessions
user_activation as (
    select
        u.user_id,
        u.signup_date,
        u.signup_week,

        -- F4: Activation (save, share, or prompted within 7 days)
        bool_or(
            s.session_date between u.signup_date and u.signup_date + 7
            and (s.has_save or s.has_share)
        ) as had_activation_7d,

        -- Breakdown by activation type
        bool_or(
            s.session_date between u.signup_date and u.signup_date + 7
            and s.has_save
        ) as had_save_7d,
        bool_or(
            s.session_date between u.signup_date and u.signup_date + 7
            and s.has_share
        ) as had_share_7d,
        bool_or(
            s.session_date between u.signup_date and u.signup_date + 7
            and s.is_prompt_session and s.has_save
        ) as had_save_prompted_7d,

        -- Initiation surface breakdown
        bool_or(
            s.session_date between u.signup_date and u.signup_date + 7
            and s.initiation_surface = 'dextr'
            and (s.has_save or s.has_share)
        ) as activated_via_prompt,
        bool_or(
            s.session_date between u.signup_date and u.signup_date + 7
            and s.initiation_surface in ('search')
            and (s.has_save or s.has_share)
        ) as activated_via_search,
        bool_or(
            s.session_date between u.signup_date and u.signup_date + 7
            and s.initiation_surface in ('featured', 'home')
            and (s.has_save or s.has_share)
        ) as activated_via_featured,

        -- First activation date and timestamp
        min(s.session_date) filter (
            where s.has_save or s.has_share
        ) as first_activation_date

    from users u
    left join user_sessions s on u.user_id = s.user_id
    group by u.user_id, u.signup_date, u.signup_week
),

-- Combine funnel data
user_complete_funnel as (
    select
        uf.user_id,
        uf.signup_date,
        uf.signup_week,
        uf.signup_timestamp,
        uf.first_event_timestamp,

        coalesce(uf.had_app_open_7d, false) as had_app_open_7d,
        coalesce(uf.had_planning_initiated_7d, false) as had_planning_initiated_7d,
        coalesce(uf.had_content_engagement_7d, false) as had_content_engagement_7d,

        coalesce(ua.had_activation_7d, false) as had_activation_7d,
        coalesce(ua.had_save_7d, false) as had_save_7d,
        coalesce(ua.had_share_7d, false) as had_share_7d,
        coalesce(ua.had_save_prompted_7d, false) as had_save_prompted_7d,

        coalesce(ua.activated_via_prompt, false) as activated_via_prompt,
        coalesce(ua.activated_via_search, false) as activated_via_search,
        coalesce(ua.activated_via_featured, false) as activated_via_featured,

        ua.first_activation_date,

        -- Calculate time to activation in hours
        case
            when ua.first_activation_date is not null and uf.signup_timestamp is not null
                then extract(epoch from (ua.first_activation_date - date(uf.signup_timestamp))) / 3600.0
            else null
        end as hours_to_activation

    from user_funnel uf
    left join user_activation ua on uf.user_id = ua.user_id
),

-- Aggregate by signup week
weekly_funnel as (
    select
        signup_week,

        -- Funnel counts
        count(*) as total_signups,
        count(*) filter (where had_app_open_7d) as had_app_open_7d,
        count(*) filter (where had_planning_initiated_7d) as had_planning_initiated_7d,
        count(*) filter (where had_content_engagement_7d) as had_content_engagement_7d,
        count(*) filter (where had_activation_7d) as had_activation_7d,

        -- Activation breakdown
        count(*) filter (where had_save_7d) as activated_with_save,
        count(*) filter (where had_share_7d) as activated_with_share,
        count(*) filter (where had_save_prompted_7d) as activated_with_save_prompted,

        -- Surface breakdown for activated users
        count(*) filter (where activated_via_prompt) as activated_via_prompt,
        count(*) filter (where activated_via_search) as activated_via_search,
        count(*) filter (where activated_via_featured) as activated_via_featured,

        -- Timing metrics
        percentile_cont(0.5) within group (order by hours_to_activation)
            filter (where hours_to_activation is not null) as avg_time_to_activation_hours

    from user_complete_funnel
    group by signup_week
)

select
    signup_week,
    total_signups,

    -- Funnel counts
    had_app_open_7d,
    had_planning_initiated_7d,
    had_content_engagement_7d,
    had_activation_7d,

    -- Step conversion rates (as decimal 0-1)
    case when total_signups > 0
        then had_app_open_7d::numeric / total_signups
        else null end as conversion_f0_to_f1,
    case when had_app_open_7d > 0
        then had_planning_initiated_7d::numeric / had_app_open_7d
        else null end as conversion_f1_to_f2,
    case when had_planning_initiated_7d > 0
        then had_content_engagement_7d::numeric / had_planning_initiated_7d
        else null end as conversion_f2_to_f3,
    case when had_content_engagement_7d > 0
        then had_activation_7d::numeric / had_content_engagement_7d
        else null end as conversion_f3_to_f4,

    -- Overall activation rate
    case when total_signups > 0
        then had_activation_7d::numeric / total_signups
        else null end as overall_activation_rate_7d,

    -- Activation breakdown
    activated_with_save,
    activated_with_share,
    activated_with_save_prompted,

    -- Surface breakdown
    activated_via_prompt,
    activated_via_search,
    activated_via_featured,

    -- Timing
    avg_time_to_activation_hours

from weekly_funnel
order by signup_week desc
