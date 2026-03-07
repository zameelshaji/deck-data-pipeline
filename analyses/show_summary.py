#!/usr/bin/env python3
"""Quick summary display script"""

import psycopg2
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

cur = conn.cursor()
cur.execute("SELECT * FROM analytics_prod_gold.user_segmentation_summary")
row = cur.fetchone()
columns = [desc[0] for desc in cur.description]

print("\n" + "="*80)
print("USER SEGMENTATION SUMMARY")
print("="*80 + "\n")

for col, val in zip(columns, row):
    if col != 'report_generated_at':
        print(f"{col:45}: {val}")

print("\n" + "="*80 + "\n")

conn.close()
