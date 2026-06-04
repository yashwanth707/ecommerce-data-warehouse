import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    role=os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'ECOMMERCE_WH'),
    database=os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DW'),
)
cur = conn.cursor()

print('\n--- Row Counts ---')
for table in ['ANALYTICS.DIM_CUSTOMERS', 'ANALYTICS.DIM_PRODUCTS', 'ANALYTICS.FACT_ORDERS']:
    try:
        cur.execute(f'SELECT COUNT(*) FROM {table}')
        print(f'{table}: {cur.fetchone()[0]} rows')
    except Exception as e:
        print(f'{table}: ERROR - {e}')

print('\n--- Freshness ---')
try:
    cur.execute("SELECT DATEDIFF('hour', MAX(dbt_updated_at), CURRENT_TIMESTAMP()) FROM ANALYTICS.FACT_ORDERS")
    print(f'Hours since last update: {cur.fetchone()[0]}')
except Exception as e:
    print(f'Freshness Error: {e}')

print('\n--- Referential Integrity ---')
try:
    cur.execute("""
        SELECT COUNT(*)
        FROM ANALYTICS.FACT_ORDERS f
        LEFT JOIN ANALYTICS.DIM_CUSTOMERS c ON f.customer_key = c.customer_key
        WHERE c.customer_key IS NULL
    """)
    print(f'Orphan records: {cur.fetchone()[0]}')
except Exception as e:
    print(f'Ref Integrity Error: {e}')
