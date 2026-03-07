#!/usr/bin/env python3
"""Check referrals column in user_segmentation"""

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

cur = conn.cursor(cursor_factory=RealDictCursor)

# Check referral stats
print("\n" + "="*80)
print("REFERRAL METRICS IN USER SEGMENTATION")
print("="*80 + "\n")

cur.execute("""
    SELECT
        user_type,
        COUNT(*) as total_users,
        COUNT(CASE WHEN total_referrals_given > 0 THEN 1 END) as users_with_referrals,
        ROUND(100.0 * COUNT(CASE WHEN total_referrals_given > 0 THEN 1 END) / COUNT(*), 2) as pct_with_referrals,
        SUM(total_referrals_given) as total_referrals,
        ROUND(AVG(total_referrals_given), 2) as avg_referrals,
        MAX(total_referrals_given) as max_referrals
    FROM analytics_prod_gold.user_segmentation
    GROUP BY user_type
""")

results = cur.fetchall()

for row in results:
    print(f"User Type: {row['user_type']}")
    print(f"  Total Users: {row['total_users']}")
    print(f"  Users with Referrals: {row['users_with_referrals']} ({row['pct_with_referrals']}%)")
    print(f"  Total Referrals Given: {row['total_referrals']}")
    print(f"  Avg Referrals per User: {row['avg_referrals']}")
    print(f"  Max Referrals: {row['max_referrals']}")
    print()

# Top referrers
print("="*80)
print("TOP 10 REFERRERS")
print("="*80 + "\n")

cur.execute("""
    SELECT
        username,
        email,
        user_type,
        total_referrals_given,
        total_prompts,
        total_swipes,
        days_active
    FROM analytics_prod_gold.user_segmentation
    WHERE total_referrals_given > 0
    ORDER BY total_referrals_given DESC
    LIMIT 10
""")

results = cur.fetchall()
for i, row in enumerate(results, 1):
    print(f"{i}. {row['username'] or 'No username'} ({row['email']})")
    print(f"   Type: {row['user_type']} | Referrals: {row['total_referrals_given']} | Prompts: {row['total_prompts']} | Days Active: {row['days_active']}")
    print()

conn.close()
print("="*80 + "\n")
