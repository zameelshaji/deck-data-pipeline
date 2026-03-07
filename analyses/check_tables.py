"""Quick script to check which tables exist in the database"""
from sqlalchemy import create_engine, text

# Connection details
db_host = "db.lzapzucmzvztogacckee.supabase.co"
db_port = "5432"
db_name = "postgres"
db_user = "postgres"
db_password = "xHwxfaNTkDaBO2b0"

connection_string = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(connection_string)

tables_to_check = [
    'analytics_prod_gold.executive_summary',
    'analytics_prod_gold.headline_metrics',
    'analytics_prod_gold.daily_active_users',
    'analytics_prod_gold.weekly_active_users',
    'analytics_prod_gold.monthly_active_users',
    'analytics_prod_gold.user_acquisition_funnel',
    'analytics_prod_gold.dextr_performance',
    'analytics_prod_gold.supplier_performance'
]

print("Checking which tables exist...\n")

with engine.connect() as conn:
    for table in tables_to_check:
        try:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"✅ {table}: {count} rows")
        except Exception as e:
            print(f"❌ {table}: DOES NOT EXIST")

print("\n--- Done ---")
