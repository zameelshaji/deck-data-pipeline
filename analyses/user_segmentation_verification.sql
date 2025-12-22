-- User Segmentation Verification Queries
-- Run these queries to validate the user_segmentation model

-- =========================================
-- 1. SANITY CHECK: Planner vs Passenger Totals
-- =========================================
-- Question: How many Planners vs Passengers? What % are Planners?
SELECT
    user_type,
    COUNT(*) as user_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM {{ ref('user_segmentation') }}
GROUP BY user_type
ORDER BY user_count DESC;

-- =========================================
-- 2. CRITERIA DISTRIBUTION
-- =========================================
-- Question: How many users qualified via each criterion?
WITH criteria_breakdown AS (
    SELECT
        user_id,
        user_type,
        UNNEST(planner_criteria_met) as criterion
    FROM {{ ref('user_segmentation') }}
    WHERE user_type = 'Planner'
)
SELECT
    criterion,
    COUNT(DISTINCT user_id) as users_meeting_criterion,
    ROUND(100.0 * COUNT(DISTINCT user_id) / (SELECT COUNT(*) FROM {{ ref('user_segmentation') }} WHERE user_type = 'Planner'), 2) as pct_of_planners
FROM criteria_breakdown
GROUP BY criterion
ORDER BY users_meeting_criterion DESC;

-- =========================================
-- 3. MULTIPLE CRITERIA ANALYSIS
-- =========================================
-- Question: How many Planners met multiple criteria?
SELECT
    ARRAY_LENGTH(planner_criteria_met, 1) as num_criteria_met,
    COUNT(*) as planner_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM {{ ref('user_segmentation') }}
WHERE user_type = 'Planner'
GROUP BY ARRAY_LENGTH(planner_criteria_met, 1)
ORDER BY num_criteria_met;

-- =========================================
-- 4. SAMPLE PLANNERS
-- =========================================
-- Question: Show 5 example Planners with their qualifying criteria
SELECT
    username,
    email,
    user_type,
    planner_criteria_met,
    total_prompts,
    total_swipes,
    total_decks_created,
    total_shares,
    total_multiplayer_sessions_created,
    days_active,
    days_since_signup
FROM {{ ref('user_segmentation') }}
WHERE user_type = 'Planner'
ORDER BY days_active DESC
LIMIT 5;

-- =========================================
-- 5. SAMPLE PASSENGERS
-- =========================================
-- Question: Show 5 example Passengers with their engagement levels
SELECT
    username,
    email,
    user_type,
    total_prompts,
    total_swipes,
    total_saves,
    days_active,
    days_since_signup,
    last_activity_date
FROM {{ ref('user_segmentation') }}
WHERE user_type = 'Passenger'
ORDER BY days_active DESC
LIMIT 5;

-- =========================================
-- 6. EDGE CASE: Users with 0 Activity
-- =========================================
-- Question: How many users have 0 activity?
SELECT
    user_type,
    COUNT(*) as zero_activity_users
FROM {{ ref('user_segmentation') }}
WHERE days_active = 0
GROUP BY user_type;

-- =========================================
-- 7. EDGE CASE: Recent Signups (< 7 days)
-- =========================================
-- Question: How do recent signups segment?
SELECT
    user_type,
    COUNT(*) as user_count,
    AVG(days_active) as avg_days_active,
    AVG(total_prompts) as avg_prompts,
    AVG(total_swipes) as avg_swipes
FROM {{ ref('user_segmentation') }}
WHERE days_since_signup < 7
GROUP BY user_type;

-- =========================================
-- 8. EDGE CASE: Test Users (Should be 0)
-- =========================================
-- Question: Are any test users included? (Should be empty)
SELECT
    COUNT(*) as test_users_count
FROM {{ ref('user_segmentation') }}
WHERE is_test_user = 1;

-- =========================================
-- 9. ENGAGEMENT QUALITY BY SEGMENT
-- =========================================
-- Question: How do Planners vs Passengers compare on quality metrics?
SELECT
    user_type,
    COUNT(*) as users,
    ROUND(AVG(prompts_per_active_day), 2) as avg_prompts_per_active_day,
    ROUND(AVG(swipes_per_prompt), 2) as avg_swipes_per_prompt,
    ROUND(AVG(conversion_rate), 2) as avg_conversion_rate,
    ROUND(AVG(days_active), 1) as avg_days_active
FROM {{ ref('user_segmentation') }}
WHERE days_active > 0  -- Only users with some activity
GROUP BY user_type;

-- =========================================
-- 10. PLANNER COHORT ANALYSIS
-- =========================================
-- Question: What % of users become Planners by signup cohort (weekly)?
SELECT
    DATE_TRUNC('week', created_at) as signup_week,
    COUNT(*) as total_users,
    COUNT(CASE WHEN user_type = 'Planner' THEN 1 END) as planners,
    ROUND(100.0 * COUNT(CASE WHEN user_type = 'Planner' THEN 1 END) / COUNT(*), 2) as planner_percentage
FROM {{ ref('user_segmentation') }}
GROUP BY DATE_TRUNC('week', created_at)
ORDER BY signup_week DESC
LIMIT 12;  -- Last 12 weeks

-- =========================================
-- 11. DETAILED PLANNER CRITERIA BREAKDOWN
-- =========================================
-- Question: For Planners, what's the distribution of specific criteria combinations?
SELECT
    planner_criteria_met,
    COUNT(*) as planner_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(days_active), 1) as avg_days_active,
    ROUND(AVG(total_conversions), 1) as avg_conversions
FROM {{ ref('user_segmentation') }}
WHERE user_type = 'Planner'
GROUP BY planner_criteria_met
ORDER BY planner_count DESC
LIMIT 10;

-- =========================================
-- 12. PASSENGER ENGAGEMENT DEPTH
-- =========================================
-- Question: Among Passengers, how many are "close" to being Planners?
SELECT
    CASE
        WHEN total_prompts >= 1 AND total_swipes >= 1 THEN 'Active but not enough engagement'
        WHEN total_prompts >= 1 OR total_swipes >= 1 THEN 'Some engagement'
        ELSE 'No AI/swipe activity'
    END as passenger_category,
    COUNT(*) as passenger_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(days_active), 1) as avg_days_active
FROM {{ ref('user_segmentation') }}
WHERE user_type = 'Passenger'
GROUP BY
    CASE
        WHEN total_prompts >= 1 AND total_swipes >= 1 THEN 'Active but not enough engagement'
        WHEN total_prompts >= 1 OR total_swipes >= 1 THEN 'Some engagement'
        ELSE 'No AI/swipe activity'
    END
ORDER BY passenger_count DESC;
