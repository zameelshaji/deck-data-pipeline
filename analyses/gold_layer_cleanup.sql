-- =============================================================================
-- Gold Layer Cleanup Script
-- Run this AFTER dbt build to drop old database tables
-- =============================================================================
-- This script drops:
-- 1. Unprefixed duplicate tables (replaced by vis_ prefixed models)
-- 2. Deleted-page vis_ tables (User Analytics, Supplier Portal)
-- 3. Legacy tables replaced by fct_ models
-- =============================================================================

-- Step 1: Drop unprefixed duplicates (now rebuilt as vis_ prefixed tables)
DROP TABLE IF EXISTS analytics_prod_gold.executive_summary;
DROP TABLE IF EXISTS analytics_prod_gold.headline_metrics;
DROP TABLE IF EXISTS analytics_prod_gold.daily_active_users;
DROP TABLE IF EXISTS analytics_prod_gold.weekly_active_users;
DROP TABLE IF EXISTS analytics_prod_gold.monthly_active_users;
DROP TABLE IF EXISTS analytics_prod_gold.user_acquisition_funnel;
DROP TABLE IF EXISTS analytics_prod_gold.content_performance;
DROP TABLE IF EXISTS analytics_prod_gold.dextr_performance;
DROP TABLE IF EXISTS analytics_prod_gold.gold_homepage_totals;
DROP TABLE IF EXISTS analytics_prod_gold.onboarding_daily_summary;
DROP TABLE IF EXISTS analytics_prod_gold.referral_analytics;
DROP TABLE IF EXISTS analytics_prod_gold.dextr_query_user_performance;

-- Step 2: Drop disabled/obsolete tables
DROP TABLE IF EXISTS analytics_prod_gold.user_session_derived;
DROP TABLE IF EXISTS analytics_prod_gold.user_segmentation;
DROP TABLE IF EXISTS analytics_prod_gold.user_segmentation_summary;
DROP TABLE IF EXISTS analytics_prod_gold.user_cohort_retention_monthly;

-- Step 3: Drop deleted-page vis_ tables
DROP TABLE IF EXISTS analytics_prod_gold.vis_user_segmentation;
DROP TABLE IF EXISTS analytics_prod_gold.vis_user_segmentation_summary;
DROP TABLE IF EXISTS analytics_prod_gold.vis_user_cohort_retention_monthly;
DROP TABLE IF EXISTS analytics_prod_gold.vis_supplier_performance;

-- Step 4: Drop disabled gold tables (broken dependencies)
DROP TABLE IF EXISTS analytics_prod_gold.supplier_performance;
DROP TABLE IF EXISTS analytics_prod_gold.category_combination_intent;
DROP TABLE IF EXISTS analytics_prod_gold.category_combination_metrics;
DROP TABLE IF EXISTS analytics_prod_gold.category_price_combinations;
DROP TABLE IF EXISTS analytics_prod_gold.category_combination_ratings;
DROP TABLE IF EXISTS analytics_prod_gold.price_band_performance;
DROP TABLE IF EXISTS analytics_prod_gold.category_intent_count;
DROP TABLE IF EXISTS analytics_prod_gold.interest_to_conversion;
