import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

def restore_snowflake_data():
    # Load environment variables
    load_dotenv()
    
    # Establish connection
    print("Connecting to new Snowflake account...")
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'ECOMMERCE_WH'),
            role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        )
        cur = conn.cursor()
    except Exception as e:
        print(f"Failed to connect to Snowflake: {e}")
        return

    # Basic infrastructure setup
    print("Setting up database and schema...")
    cur.execute("CREATE WAREHOUSE IF NOT EXISTS ECOMMERCE_WH WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;")
    cur.execute("CREATE DATABASE IF NOT EXISTS ECOMMERCE_DW;")
    cur.execute("USE DATABASE ECOMMERCE_DW;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS ANALYTICS;")
    cur.execute("USE SCHEMA ANALYTICS;")

    # Defined tables backed up
    tables_to_restore = [
        'DIM_CUSTOMERS',
        'DIM_PRODUCTS',
        'DIM_SELLERS',
        'DIM_TIME',
        'FACT_ORDERS',
        'FACT_ORDER_ITEMS',
        'CUSTOMER_CLV'
    ]

    export_dir = 'data/export'
    
    print(f"Restoring data from {os.path.abspath(export_dir)} to ECOMMERCE_DW.ANALYTICS...")

    # Restore each table
    for table_name in tables_to_restore:
        csv_file = os.path.join(export_dir, f"{table_name}.csv")
        if not os.path.exists(csv_file):
            print(f"  [WARN] Backup not found for {table_name}: {csv_file}")
            continue
            
        print(f"Restoring {table_name}...")
        try:
            # Read CSV
            df = pd.read_csv(csv_file)
            
            # Since Pandas infers object for string columns, we can just let write_pandas handle 
            # the table creation implicitly by using auto_create_table=True
            success, nchunks, nrows, _ = write_pandas(
                conn, 
                df, 
                table_name.upper(), 
                auto_create_table=True, 
                overwrite=True
            )
            print(f"  ✅ Uploaded {nrows} rows to {table_name}")
            
        except Exception as e:
            print(f"  ❌ Error restoring {table_name}: {e}")

    conn.close()
    print("\nRestore complete! Your new Snowflake account is fully populated for Power BI.")

if __name__ == "__main__":
    restore_snowflake_data()
