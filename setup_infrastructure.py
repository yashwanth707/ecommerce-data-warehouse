import os
import glob
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import snowflake.connector

# Load credentials from .env
load_dotenv()

# AWS Configurations
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_PREFIX = os.getenv('S3_PREFIX', 'olist/')

# Snowflake Configurations
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'ECOMMERCE_WH')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DW')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'RAW')

DATA_DIR = r"d:\1. Warehouse_Project\Olist_ETL_Project_Data"

def setup_s3():
    print("Setting up AWS S3...")
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    # 1. Create S3 Bucket if it doesn't exist
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f"Bucket '{S3_BUCKET}' already exists and is accessible.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Bucket '{S3_BUCKET}' does not exist. Creating it...")
            if AWS_REGION == 'us-east-1':
                s3_client.create_bucket(Bucket=S3_BUCKET)
            else:
                s3_client.create_bucket(
                    Bucket=S3_BUCKET,
                    CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                )
            print(f"Bucket '{S3_BUCKET}' created successfully.")
        else:
            print(f"Error checking bucket: {e}")
            raise

    # 2. Upload files
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in '{DATA_DIR}'. Check the path!")
        return

    print(f"Found {len(csv_files)} CSV files. Starting upload to s3://{S3_BUCKET}/{S3_PREFIX}...")

    for file_path in csv_files:
        filename = os.path.basename(file_path)
        
        # Determine specific prefix based on existing project structure logic:
        # We look at the BRONZE_TABLES dictionary in s3_to_snowflake.py
        # It expects prefixes like 'orders/', 'customers/' etc.
        # But wait! s3_to_snowflake.py logic:
        # "orders": {"s3_prefix": "orders/", "file_pattern": "olist_orders_dataset"}
        # So we need to correctly prefix the S3 keys.
        sub_prefix = ""
        if "olist_orders_dataset" in filename: sub_prefix = "orders/"
        elif "olist_customers" in filename: sub_prefix = "customers/"
        elif "olist_products" in filename: sub_prefix = "products/"
        elif "olist_order_items" in filename: sub_prefix = "order_items/"
        elif "olist_order_payments" in filename: sub_prefix = "payments/"
        elif "olist_order_reviews" in filename: sub_prefix = "reviews/"
        elif "olist_geolocation" in filename: sub_prefix = "geolocation/"
        elif "olist_sellers" in filename: sub_prefix = "sellers/"
        else: sub_prefix = "other/"

        s3_key = f"{S3_PREFIX}{sub_prefix}{filename}"
        
        print(f"  Uploading {filename} to {s3_key}")
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)

    print("AWS S3 setup completed successfully.\n")

def setup_snowflake():
    print("Setting up Snowflake infrastructure...")
    
    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            role=SNOWFLAKE_ROLE
        )
        cur = conn.cursor()
        
        # Create Warehouse
        print(f"  Ensuring Warehouse '{SNOWFLAKE_WAREHOUSE}' exists...")
        cur.execute(f"CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE} "
                    f"WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;")

        # Create Database
        print(f"  Ensuring Database '{SNOWFLAKE_DATABASE}' exists...")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE};")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE};")

        # Create Schemas
        print("  Ensuring Schemas 'RAW', 'ANALYTICS', 'CLEANSED' exist...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS RAW;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS CLEANSED;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS ANALYTICS;")

        conn.commit()
        print("Snowflake infrastructure setup completed successfully.\n")

    except Exception as e:
        print(f"Failed to setup Snowflake: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    try:
        setup_s3()
        setup_snowflake()
        print("Infrastructure setup script completed.")
    except Exception as e:
        print(f"An error occurred: {e}")
