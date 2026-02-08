-- User Segmentation: Planners vs Passengers
-- Identifies users who actively initiate planning (Planners) vs users who engage passively (Passengers)
-- Critical for retention strategies: If planners don't come back, nothing else matters

with
    -- Step 1: Aggregate user activity from events
    user_activity as (
        select
            e.user_id,

            -- AI Interaction metrics
            count(
                case when e.event_type = 'dextr_query' then 1 end
            ) as total_prompts,

            -- Swipe metrics (left + right from all sources)
            count(
                case
                    when e.event_type in ('swipe_left', 'swipe_right') then 1
                end
            ) as total_swipes,

            -- Save metrics
            count(case when e.event_type = 'saved' then 1 end) as total_saves,

            -- Share metrics from events
            count(case when e.event_type = 'share' then 1 end) as total_shares_events,

            -- Conversion metrics
            count(
                case
                    when
                        e.event_type in (
                            'opened_website',
                            'book_with_deck',
                            'click_directions',
                            'click_phone'
                        )
                    then 1
                end
            ) as total_conversions,

            -- Activity frequency metrics
            count(distinct date(e.event_timestamp)) as days_active,
            max(date(e.event_timestamp)) as last_activity_date

        from {{ ref("stg_unified_events") }} e
        group by e.user_id
    ),

    -- Step 2: Count shares from core_card_actions (in case they're tracked there too)
    user_shares as (
        select
            user_id,
            count(case when action_type = 'share' then 1 end) as total_shares_actions
        from {{ ref("src_core_card_actions") }}
        group by user_id
    ),

    -- Step 3: Count decks created (non-default boards)
    user_decks as (
        select
            user_id,
            count(
                case when is_default = false then 1 end
            ) as total_decks_created
        from {{ ref("src_boards") }}
        group by user_id
    ),

    -- Step 4: Count multiplayer participation
    user_multiplayer as (
        select
            creator_id as user_id,
            count(*) as total_multiplayer_sessions_created
        from {{ ref("stg_multiplayer") }}
        group by creator_id
    ),

    user_multiplayer_participation as (
        select
            sp.user_id,
            count(distinct sp.multiplayer_id) as total_multiplayer_sessions_participated
        from {{ ref("src_session_participants") }} sp
        group by sp.user_id
    ),

    -- Step 4b: Count referrals given
    user_referrals as (
        select
            referrer_user_id as user_id,
            count(distinct referred_user_id) as total_referrals_given
        from {{ ref("stg_referral_relationships") }}
        group by referrer_user_id
    ),

    -- Step 5: Combine all metrics
    user_metrics as (
        select
            -- Core identity from stg_users
            u.user_id,
            u.email,
            u.username,
            u.full_name,
            u.created_at,
            u.is_test_user,

            -- Engagement metrics
            coalesce(ua.total_prompts, 0) as total_prompts,
            coalesce(ua.total_swipes, 0) as total_swipes,
            coalesce(ua.total_saves, 0) as total_saves,

            -- Combine shares from both sources
            coalesce(ua.total_shares_events, 0)
                + coalesce(us.total_shares_actions, 0) as total_shares,

            coalesce(ud.total_decks_created, 0) as total_decks_created,
            coalesce(um.total_multiplayer_sessions_created, 0)
                as total_multiplayer_sessions_created,
            coalesce(ump.total_multiplayer_sessions_participated, 0)
                as total_multiplayer_sessions_participated,
            coalesce(ur.total_referrals_given, 0)
                as total_referrals_given,

            -- Additional behavioral signals
            coalesce(ua.total_conversions, 0) as total_conversions,
            coalesce(ua.days_active, 0) as days_active,
            ua.last_activity_date,

            -- Time-based metrics
            (current_date - u.created_at::date)::integer
                as days_since_signup,
            case
                when ua.last_activity_date is not null
                then (current_date - ua.last_activity_date)::integer
                else null
            end as days_since_last_activity

        from {{ ref("stg_users") }} u
        left join user_activity ua on u.user_id = ua.user_id
        left join user_shares us on u.user_id = us.user_id
        left join user_decks ud on u.user_id = ud.user_id
        left join user_multiplayer um on u.user_id = um.user_id
        left join
            user_multiplayer_participation ump on u.user_id = ump.user_id
        left join user_referrals ur on u.user_id = ur.user_id

        -- Exclude test users
        where u.is_test_user = 0
    ),

    -- Step 6: Apply segmentation logic
    user_segmentation as (
        select
            *,

            -- Engagement quality metrics
            case
                when days_active > 0
                then round(total_prompts::numeric / days_active, 2)
                else 0
            end as prompts_per_active_day,

            case
                when total_prompts > 0
                then round(total_swipes::numeric / total_prompts, 2)
                else 0
            end as swipes_per_prompt,

            case
                when total_swipes > 0
                then round((total_conversions::numeric / total_swipes) * 100, 2)
                else 0
            end as conversion_rate,

            -- Planner criteria logic
            case
                when
                    (total_prompts >= 2 and total_swipes >= 3)
                    or total_decks_created >= 1
                    or total_shares >= 1
                    or total_multiplayer_sessions_created >= 1
                then 'Planner'
                else 'Passenger'
            end as user_type,

            -- Track which criteria qualified them as a Planner
            array_remove(
                array [
                    case
                        when total_prompts >= 2 and total_swipes >= 3
                        then 'prompt_and_swipe'
                        else null
                    end,
                    case
                        when total_decks_created >= 1 then 'created_deck' else null
                    end,
                    case when total_shares >= 1 then 'shared_content' else null end,
                    case
                        when total_multiplayer_sessions_created >= 1
                        then 'created_multiplayer'
                        else null
                    end
                ],
                null
            ) as planner_criteria_met

        from user_metrics
    )

select
    -- Core Identity
    user_id,
    email,
    username,
    full_name,
    created_at,

    -- Segmentation
    user_type,
    planner_criteria_met,

    -- Engagement Metrics
    total_prompts,
    total_swipes,
    total_saves,
    total_shares,
    total_decks_created,
    total_multiplayer_sessions_created,
    total_multiplayer_sessions_participated,
    total_referrals_given,

    -- Additional Behavioral Signals
    total_conversions,
    days_active,
    last_activity_date,
    days_since_signup,
    days_since_last_activity,
    is_test_user,

    -- Engagement Quality
    prompts_per_active_day,
    swipes_per_prompt,
    conversion_rate

from user_segmentation
order by created_at desc
