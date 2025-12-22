#!/usr/bin/env python3
"""Verify Dextr Query User Performance Model"""

import psycopg2
from psycopg2.extras import RealDictCursor
import yaml

with open('/Users/zameelshaji/.dbt/profiles.yml', 'r') as f:
    profiles = yaml.safe_load(f)

db_config = profiles['default']['outputs']['dev']

conn = psycopg2.connect(
    host=db_config['host'],
    port=db_config['port'],
    user=db_config['user'],
    password=db_config['password'],
    database=db_config['database']
)

def run_query(query, description):
    """Run a query and print formatted results"""
    print(f"\n{'='*120}")
    print(f"{description}")
    print(f"{'='*120}\n")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        results = cur.fetchall()

        if not results:
            print("No results found")
            return

        # Print header
        headers = list(results[0].keys())
        header_line = " | ".join(f"{h:15}" for h in headers)
        print(header_line)
        print("-" * len(header_line))

        # Print rows
        for row in results:
            row_line = " | ".join(f"{str(row[h])[:15]:15}" for h in headers)
            print(row_line)

    print()

# 1. Overall summary
run_query("""
    SELECT
        COUNT(*) as total_queries,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(CASE WHEN query_engaged = 1 THEN 1 END) as engaged_queries,
        ROUND(100.0 * COUNT(CASE WHEN query_engaged = 1 THEN 1 END) / COUNT(*), 2) as engagement_rate,
        ROUND(AVG(cards_viewed), 1) as avg_cards_viewed,
        ROUND(AVG(like_rate), 2) as avg_like_rate,
        ROUND(AVG(save_rate), 2) as avg_save_rate
    FROM analytics_prod_gold.dextr_query_user_performance
""", "1. OVERALL QUERY PERFORMANCE SUMMARY")

# 2. Performance by user segment
run_query("""
    SELECT
        user_type,
        COUNT(*) as total_queries,
        COUNT(DISTINCT user_id) as unique_users,
        ROUND(AVG(cards_viewed), 1) as avg_cards_viewed,
        ROUND(AVG(cards_liked), 1) as avg_cards_liked,
        ROUND(AVG(cards_saved), 1) as avg_cards_saved,
        ROUND(AVG(like_rate), 2) as avg_like_rate,
        ROUND(AVG(save_rate), 2) as avg_save_rate
    FROM analytics_prod_gold.dextr_query_user_performance
    GROUP BY user_type
    ORDER BY total_queries DESC
""", "2. PERFORMANCE BY USER SEGMENT (PLANNER VS PASSENGER)")

# 3. Top performing queries (highest like rate)
run_query("""
    SELECT
        user_name,
        user_type,
        query_text,
        cards_viewed,
        cards_liked,
        cards_saved,
        like_rate,
        save_rate
    FROM analytics_prod_gold.dextr_query_user_performance
    WHERE cards_viewed >= 5
    ORDER BY like_rate DESC, cards_liked DESC
    LIMIT 10
""", "3. TOP 10 BEST PERFORMING QUERIES (Highest Like Rate, min 5 cards viewed)")

# 4. Recent queries
run_query("""
    SELECT
        date,
        user_name,
        user_type,
        query_text,
        cards_viewed,
        cards_liked,
        cards_disliked,
        cards_saved
    FROM analytics_prod_gold.dextr_query_user_performance
    ORDER BY query_timestamp DESC
    LIMIT 10
""", "4. 10 MOST RECENT QUERIES")

# 5. User query activity
run_query("""
    SELECT
        user_name,
        user_email,
        user_type,
        COUNT(*) as total_queries,
        SUM(cards_viewed) as total_cards_viewed,
        SUM(cards_liked) as total_cards_liked,
        SUM(cards_saved) as total_cards_saved,
        ROUND(AVG(like_rate), 2) as avg_like_rate
    FROM analytics_prod_gold.dextr_query_user_performance
    GROUP BY user_name, user_email, user_type
    HAVING COUNT(*) >= 5
    ORDER BY total_queries DESC
    LIMIT 10
""", "5. TOP 10 POWER USERS (5+ queries)")

# 6. Query engagement breakdown
run_query("""
    SELECT
        CASE
            WHEN query_engaged = 0 THEN 'Not Engaged'
            WHEN query_had_saves = 1 THEN 'Had Saves'
            WHEN query_had_likes = 1 THEN 'Had Likes'
            ELSE 'Viewed Only'
        END as engagement_level,
        COUNT(*) as query_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM analytics_prod_gold.dextr_query_user_performance
    GROUP BY
        CASE
            WHEN query_engaged = 0 THEN 'Not Engaged'
            WHEN query_had_saves = 1 THEN 'Had Saves'
            WHEN query_had_likes = 1 THEN 'Had Likes'
            ELSE 'Viewed Only'
        END
    ORDER BY query_count DESC
""", "6. QUERY ENGAGEMENT BREAKDOWN")

# 7. Daily query trends (last 7 days)
run_query("""
    SELECT
        date,
        COUNT(*) as total_queries,
        COUNT(DISTINCT user_id) as unique_users,
        ROUND(AVG(cards_viewed), 1) as avg_cards_viewed,
        ROUND(AVG(like_rate), 2) as avg_like_rate
    FROM analytics_prod_gold.dextr_query_user_performance
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY date
    ORDER BY date DESC
""", "7. DAILY QUERY TRENDS (LAST 7 DAYS)")

conn.close()
print("\n" + "="*120)
print("Verification complete!")
print("="*120 + "\n")
