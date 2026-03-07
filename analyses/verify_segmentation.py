#!/usr/bin/env python3
"""
Verification script for user_segmentation model
Runs key validation queries and displays results
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import yaml

# Load dbt profile
with open('/Users/zameelshaji/.dbt/profiles.yml', 'r') as f:
    profiles = yaml.safe_load(f)

db_config = profiles['default']['outputs']['dev']

# Connect to database
conn = psycopg2.connect(
    host=db_config['host'],
    port=db_config['port'],
    user=db_config['user'],
    password=db_config['password'],
    database=db_config['database']
)

def run_query(query, description):
    """Run a query and print formatted results"""
    print(f"\n{'='*80}")
    print(f"{description}")
    print(f"{'='*80}")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        results = cur.fetchall()

        if not results:
            print("No results found")
            return

        # Print header
        headers = list(results[0].keys())
        header_line = " | ".join(f"{h:20}" for h in headers)
        print(header_line)
        print("-" * len(header_line))

        # Print rows
        for row in results:
            row_line = " | ".join(f"{str(row[h])[:20]:20}" for h in headers)
            print(row_line)

    print()

# 1. Planner vs Passenger totals
run_query("""
    SELECT
        user_type,
        COUNT(*) as user_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM analytics_prod_gold.user_segmentation
    GROUP BY user_type
    ORDER BY user_count DESC
""", "1. PLANNER VS PASSENGER BREAKDOWN")

# 2. Criteria distribution
run_query("""
    WITH criteria_breakdown AS (
        SELECT
            user_id,
            user_type,
            UNNEST(planner_criteria_met) as criterion
        FROM analytics_prod_gold.user_segmentation
        WHERE user_type = 'Planner'
    )
    SELECT
        criterion,
        COUNT(DISTINCT user_id) as users_meeting_criterion,
        ROUND(100.0 * COUNT(DISTINCT user_id) /
            (SELECT COUNT(*) FROM analytics_prod_gold.user_segmentation WHERE user_type = 'Planner'), 2)
            as pct_of_planners
    FROM criteria_breakdown
    GROUP BY criterion
    ORDER BY users_meeting_criterion DESC
""", "2. PLANNER CRITERIA DISTRIBUTION")

# 3. Multiple criteria
run_query("""
    SELECT
        ARRAY_LENGTH(planner_criteria_met, 1) as num_criteria_met,
        COUNT(*) as planner_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM analytics_prod_gold.user_segmentation
    WHERE user_type = 'Planner'
    GROUP BY ARRAY_LENGTH(planner_criteria_met, 1)
    ORDER BY num_criteria_met
""", "3. PLANNERS BY NUMBER OF CRITERIA MET")

# 4. Sample Planners
run_query("""
    SELECT
        username,
        email,
        user_type,
        planner_criteria_met,
        total_prompts,
        total_swipes,
        days_active
    FROM analytics_prod_gold.user_segmentation
    WHERE user_type = 'Planner'
    ORDER BY days_active DESC
    LIMIT 5
""", "4. TOP 5 MOST ACTIVE PLANNERS")

# 5. Sample Passengers
run_query("""
    SELECT
        username,
        email,
        user_type,
        total_prompts,
        total_swipes,
        days_active
    FROM analytics_prod_gold.user_segmentation
    WHERE user_type = 'Passenger'
    ORDER BY days_active DESC
    LIMIT 5
""", "5. TOP 5 MOST ACTIVE PASSENGERS")

# 6. Edge case: Zero activity
run_query("""
    SELECT
        user_type,
        COUNT(*) as zero_activity_users
    FROM analytics_prod_gold.user_segmentation
    WHERE days_active = 0
    GROUP BY user_type
""", "6. USERS WITH ZERO ACTIVITY")

# 7. Edge case: Recent signups
run_query("""
    SELECT
        user_type,
        COUNT(*) as user_count,
        ROUND(AVG(days_active), 1) as avg_days_active,
        ROUND(AVG(total_prompts), 1) as avg_prompts
    FROM analytics_prod_gold.user_segmentation
    WHERE days_since_signup < 7
    GROUP BY user_type
""", "7. RECENT SIGNUPS (< 7 DAYS)")

# 8. Edge case: Test users
run_query("""
    SELECT
        COUNT(*) as test_users_count
    FROM analytics_prod_gold.user_segmentation
    WHERE is_test_user = 1
""", "8. TEST USERS (SHOULD BE 0)")

# 9. Engagement quality by segment
run_query("""
    SELECT
        user_type,
        COUNT(*) as users,
        ROUND(AVG(prompts_per_active_day), 2) as avg_prompts_per_active_day,
        ROUND(AVG(swipes_per_prompt), 2) as avg_swipes_per_prompt,
        ROUND(AVG(conversion_rate), 2) as avg_conversion_rate
    FROM analytics_prod_gold.user_segmentation
    WHERE days_active > 0
    GROUP BY user_type
""", "9. ENGAGEMENT QUALITY BY SEGMENT")

# 10. Planner percentage by week
run_query("""
    SELECT
        DATE_TRUNC('week', created_at)::date as signup_week,
        COUNT(*) as total_users,
        COUNT(CASE WHEN user_type = 'Planner' THEN 1 END) as planners,
        ROUND(100.0 * COUNT(CASE WHEN user_type = 'Planner' THEN 1 END) / COUNT(*), 2) as planner_pct
    FROM analytics_prod_gold.user_segmentation
    GROUP BY DATE_TRUNC('week', created_at)::date
    ORDER BY signup_week DESC
    LIMIT 8
""", "10. PLANNER % BY SIGNUP WEEK (LAST 8 WEEKS)")

conn.close()
print("\n" + "="*80)
print("Verification complete!")
print("="*80 + "\n")
