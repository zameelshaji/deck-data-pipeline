-- ============================================================================
-- DECK Gold Layer: Example Queries for Conversational Analytics Agent
-- ============================================================================
-- These queries are designed for Nameel's Claude + Supabase MCP agent.
-- All gold tables live in schema: analytics_prod_gold
-- All tables exclude test users already — no additional filtering needed.
-- ============================================================================


-- ============================================================================
-- 1. USER ARCHETYPES & TOP USERS
-- ============================================================================

-- Q: "Who are our best users?"
-- Top 10 users by total saves (planners who actually use the product)
select user_id, username, email, user_archetype, total_saves, total_shares,
       total_prompts, total_sessions, activation_date
from analytics_prod_gold.fct_user_segments
where is_activated = true
order by total_saves desc
limit 10;

-- Q: "What percentage of users are one-and-done?"
select
    count(*) as total_activated,
    count(*) filter (where user_archetype = 'one_and_done') as one_and_done,
    round(100.0 * count(*) filter (where user_archetype = 'one_and_done') / count(*), 1) as one_and_done_pct,
    count(*) filter (where user_archetype = 'planner') as planners,
    count(*) filter (where user_archetype = 'power_planner') as power_planners,
    round(100.0 * count(*) filter (where is_planner) / count(*), 1) as planner_pct
from analytics_prod_gold.fct_user_segments
where is_activated = true;

-- Q: "Planner vs passenger breakdown"
select
    case when is_planner then 'Planner' else 'Passenger' end as segment,
    count(*) as users,
    round(avg(total_saves), 1) as avg_saves,
    round(avg(total_shares), 1) as avg_shares,
    round(avg(total_sessions), 1) as avg_sessions,
    round(100.0 * count(*) filter (where retained_d30) / nullif(count(*), 0), 1) as d30_retention_pct
from analytics_prod_gold.fct_user_segments
where is_activated = true
group by 1;


-- ============================================================================
-- 2. COHORT QUALITY & RETENTION
-- ============================================================================

-- Q: "Which cohort had the best D30 retention?"
select activation_week, cohort_size, retention_rate_d7, retention_rate_d30,
       retention_rate_d60, planner_pct, one_and_done_pct
from analytics_prod_gold.fct_cohort_quality
where cohort_size >= 5
order by retention_rate_d30 desc nulls last
limit 10;

-- Q: "Show me D7/D30/D60 retention trends over time"
select activation_week, cohort_size,
       round(retention_rate_d7 * 100, 1) as d7_pct,
       round(retention_rate_d30 * 100, 1) as d30_pct,
       round(retention_rate_d60 * 100, 1) as d60_pct,
       round(planner_pct * 100, 1) as planner_pct
from analytics_prod_gold.fct_cohort_quality
order by activation_week desc;

-- Q: "Organic vs referral cohort quality"
select activation_week,
       organic_count, referral_count,
       round(organic_retention_d30 * 100, 1) as organic_d30_pct,
       round(referral_retention_d30 * 100, 1) as referral_d30_pct
from analytics_prod_gold.fct_cohort_quality
where organic_count > 0 or referral_count > 0
order by activation_week desc;


-- ============================================================================
-- 3. ENGAGEMENT TRENDS
-- ============================================================================

-- Q: "How does engagement change week-over-week for a cohort?"
select activation_week, weeks_since_activation,
       count(distinct user_id) as active_users,
       round(avg(saves_count), 2) as avg_saves,
       round(avg(prompts_count), 2) as avg_prompts,
       round(avg(pct_sessions_with_save) * 100, 1) as avg_save_session_pct
from analytics_prod_gold.fct_user_engagement_trajectory
where activation_week >= '2025-09-01'
group by 1, 2
order by 1, 2;

-- Q: "Average cards viewed per session over time"
select activity_week,
       count(distinct user_id) as active_users,
       round(avg(avg_cards_per_session), 1) as avg_cards_per_session,
       round(avg(swipe_to_save_rate), 3) as avg_swipe_to_save_rate
from analytics_prod_gold.fct_user_engagement_trajectory
group by 1
order by 1 desc;


-- ============================================================================
-- 4. AI PROMPT ANALYSIS
-- ============================================================================

-- Q: "Which prompt types lead to the most saves?"
select prompt_intent,
       count(*) as total_prompts,
       round(avg(save_rate), 3) as avg_save_rate,
       round(avg(like_rate), 3) as avg_like_rate,
       round(100.0 * count(*) filter (where zero_save_prompt) / count(*), 1) as zero_save_pct,
       round(100.0 * count(*) filter (where led_to_share) / count(*), 1) as led_to_share_pct
from analytics_prod_gold.fct_prompt_analysis
group by 1
order by avg_save_rate desc;

-- Q: "Do re-prompts (refinements) perform better?"
select
    case when is_refinement then 'Refinement (2nd+ prompt)' else 'First prompt' end as prompt_type,
    count(*) as total,
    round(avg(save_rate), 3) as avg_save_rate,
    round(avg(like_rate), 3) as avg_like_rate,
    round(100.0 * count(*) filter (where zero_save_prompt) / count(*), 1) as zero_save_pct
from analytics_prod_gold.fct_prompt_analysis
group by 1;

-- Q: "Prompt specificity vs outcomes"
select prompt_specificity,
       count(*) as total_prompts,
       round(avg(save_rate), 3) as avg_save_rate,
       round(100.0 * count(*) filter (where led_to_save) / count(*), 1) as led_to_save_pct
from analytics_prod_gold.fct_prompt_analysis
group by 1
order by avg_save_rate desc;


-- ============================================================================
-- 5. PACK & CARD PERFORMANCE
-- ============================================================================

-- Q: "Best and worst performing packs by save rate"
-- Top 10 packs
select pack_id, query_text, total_cards_generated, cards_saved,
       round(save_rate, 3) as save_rate, round(like_rate, 3) as like_rate,
       prompt_intent_category
from analytics_prod_gold.fct_pack_performance
where total_cards_generated >= 3
order by save_rate desc
limit 10;

-- Q: "Best performing places/cards"
select card_id, place_name, category, neighborhood,
       total_impressions, total_saves, total_shares,
       round(save_rate, 3) as save_rate,
       round(right_swipe_rate, 3) as right_swipe_rate,
       round(viral_score, 2) as viral_score
from analytics_prod_gold.fct_place_performance
where total_impressions >= 10
order by save_rate desc
limit 20;

-- Q: "Category comparison — which categories perform best?"
select category,
       count(*) as places,
       sum(total_impressions) as total_impressions,
       round(avg(save_rate), 3) as avg_save_rate,
       round(avg(right_swipe_rate), 3) as avg_swipe_rate,
       round(avg(conversion_rate), 4) as avg_conversion_rate
from analytics_prod_gold.fct_place_performance
where category is not null
group by 1
order by avg_save_rate desc;


-- ============================================================================
-- 6. BOOKING INTENT & CONVERSION
-- ============================================================================

-- Q: "How many saves lead to booking clicks?"
select
    count(*) as total_conversions,
    count(*) filter (where was_saved_first) as saved_first,
    round(100.0 * count(*) filter (where was_saved_first) / count(*), 1) as pct_saved_first,
    round(avg(time_from_save_to_conversion_minutes) filter (where was_saved_first), 0) as avg_minutes_save_to_convert,
    count(*) filter (where was_prompt_initiated) as prompt_initiated,
    round(100.0 * count(*) filter (where was_prompt_initiated) / count(*), 1) as pct_prompt_initiated
from analytics_prod_gold.fct_conversion_signals;

-- Q: "Conversion breakdown by action type"
select action_type,
       count(*) as total,
       count(*) filter (where was_saved_first) as saved_first,
       count(*) filter (where was_prompt_initiated) as prompt_initiated,
       count(distinct user_id) as unique_users
from analytics_prod_gold.fct_conversion_signals
group by 1
order by total desc;

-- Q: "Which categories drive the most conversions?"
select place_category,
       count(*) as conversions,
       count(distinct user_id) as unique_users,
       round(100.0 * count(*) filter (where was_saved_first) / count(*), 1) as pct_saved_first
from analytics_prod_gold.fct_conversion_signals
where place_category is not null
group by 1
order by conversions desc;


-- ============================================================================
-- 7. VIRAL LOOP & SHARING
-- ============================================================================

-- Q: "What percentage of shares actually get viewed?"
select
    count(*) as total_shares,
    count(*) filter (where unique_viewers > 0) as shares_with_views,
    round(100.0 * count(*) filter (where unique_viewers > 0) / nullif(count(*), 0), 1) as pct_viewed,
    round(avg(unique_viewers) filter (where unique_viewers > 0), 1) as avg_viewers_when_viewed,
    sum(viewers_who_signed_up) as total_signups_from_shares,
    round(avg(effective_k_factor), 3) as avg_k_factor,
    round(avg(time_to_first_view_minutes) filter (where time_to_first_view_minutes is not null), 0) as avg_minutes_to_first_view
from analytics_prod_gold.fct_viral_loop;

-- Q: "Share effectiveness by sharer archetype"
select sharer_archetype,
       count(*) as shares,
       round(avg(unique_viewers), 1) as avg_viewers,
       round(avg(signup_conversion_rate), 3) as avg_signup_rate,
       round(avg(effective_k_factor), 3) as avg_k_factor,
       sum(viewers_who_saved) as total_viewer_saves
from analytics_prod_gold.fct_viral_loop
where sharer_archetype is not null
group by 1
order by avg_k_factor desc;

-- Q: "Share type comparison"
select share_type,
       count(*) as shares,
       round(avg(unique_viewers), 1) as avg_viewers,
       sum(viewers_who_signed_up) as signups,
       round(avg(effective_k_factor), 3) as avg_k_factor
from analytics_prod_gold.fct_viral_loop
group by 1
order by shares desc;
