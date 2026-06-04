import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

def export_snowflake_data():
    # Load environment variables
    load_dotenv()
    
    # Establish connection
    print("Connecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'ECOMMERCE_WH'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DW'),
            schema='ANALYTICS',
            role=os.getenv('SNOWFLAKE_ROLE')
        )
    except Exception as e:
        print(f"Failed to connect to Snowflake: {e}")
        return

    # Define the tables we need to backup
    tables_to_export = [
        'DIM_CUSTOMERS',
        'DIM_PRODUCTS',
        'DIM_SELLERS',
        'DIM_TIME',
        'FACT_ORDERS',
        'FACT_ORDER_ITEMS',
        'CUSTOMER_CLV'
    ]

    # Create export directory
    export_dir = 'data/export'
    os.makedirs(export_dir, exist_ok=True)
    print(f"Exporting data to {os.path.abspath(export_dir)}...")

    # Export each table
    for table in tables_to_export:
        print(f"Exporting {table}...")
        query = f"SELECT * FROM {table}"
        try:
            # Load into pandas DataFrame
            df = pd.read_sql(query, conn)
            
            # Save to CSV
            output_file = os.path.join(export_dir, f"{table}.csv")
            df.to_csv(output_file, index=False)
            print(f"  Saved {len(df)} rows to {output_file}")
            
        except Exception as e:
            print(f"  Error exporting {table}: {e}")

    conn.close()
    print("\nBackup complete! Your Power BI data is safe.")

if __name__ == "__main__":
    export_snowflake_data()
