#!/usr/bin/env python3
"""Show top 25 Planners by engagement"""

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

print("\n" + "="*120)
print("TOP 25 PLANNERS BY ENGAGEMENT (sorted by days active)")
print("="*120 + "\n")

cur.execute("""
    SELECT
        username,
        email,
        planner_criteria_met,
        total_prompts,
        total_swipes,
        total_saves,
        total_shares,
        total_decks_created,
        total_multiplayer_sessions_created,
        total_referrals_given,
        total_conversions,
        days_active,
        days_since_signup,
        days_since_last_activity,
        prompts_per_active_day,
        conversion_rate
    FROM analytics_prod_gold.user_segmentation
    WHERE user_type = 'Planner'
    ORDER BY days_active DESC, total_prompts DESC
    LIMIT 25
""")

results = cur.fetchall()

for i, row in enumerate(results, 1):
    print(f"{i}. {row['username'] or 'No username'}")
    print(f"   Email: {row['email']}")
    print(f"   Qualified via: {', '.join(row['planner_criteria_met'])}")
    print(f"   Activity: {row['days_active']} days active ({row['days_since_signup']} days since signup, last active {row['days_since_last_activity']} days ago)")
    print(f"   Engagement:")
    print(f"     - Prompts: {row['total_prompts']} ({row['prompts_per_active_day']}/day)")
    print(f"     - Swipes: {row['total_swipes']}")
    print(f"     - Saves: {row['total_saves']}")
    print(f"     - Shares: {row['total_shares']}")
    print(f"   Creation:")
    print(f"     - Decks: {row['total_decks_created']}")
    print(f"     - Multiplayer Sessions: {row['total_multiplayer_sessions_created']}")
    print(f"   Growth:")
    print(f"     - Referrals Given: {row['total_referrals_given']}")
    print(f"   Conversion:")
    print(f"     - Total Conversions: {row['total_conversions']} ({row['conversion_rate']}% conversion rate)")
    print()

conn.close()
print("="*120 + "\n")
