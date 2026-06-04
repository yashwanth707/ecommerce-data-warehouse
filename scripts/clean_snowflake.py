"""
Clean Snowflake Database
========================
Wipes the ECOMMERCE_DW database clean so we can start fresh.
Warning: This drops all schemas, tables, and views.
"""
import os
import snowflake.connector
from dotenv import load_dotenv

# Load credentials
load_dotenv()

def clean_database():
    print("Initiating Snowflake Cleanup...")
    
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMMERCE_WH")
    )
    
    db = os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DW")
    
    try:
        cur = conn.cursor()
        print(f"Dropping database {db}...")
        cur.execute(f"DROP DATABASE IF EXISTS {db}")
        
        print(f"Recreating database {db}...")
        cur.execute(f"CREATE DATABASE {db}")
        
        print("Clean complete. Database is now empty and ready for fresh ingestion.")
    except Exception as e:
        print(f"Error during cleanup: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    confirm = input("Are you sure you want to drop and recreate the Snowflake database? (y/n): ")
    if confirm.lower() == 'y':
        clean_database()
    else:
        print("Cleanup aborted.")
