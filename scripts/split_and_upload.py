"""
Dataset Batch Splitter & S3 Uploader
=====================================
Splits the Olist dataset into 4 clean batches + 1 error batch.

Batch plan:
  - batch_1: ~25% of data (load now for pipeline run 1)
  - batch_2: ~25% of data (load now for pipeline run 2)
  - batch_3: ~25% of data (save for live demo at evaluation)
  - batch_4: ~25% of data (save for live demo at evaluation)
  - batch_error: Corrupted data (showcase error handling & notifications)

Usage:
  python scripts/split_and_upload.py --split              # Split CSVs into batches
  python scripts/split_and_upload.py --upload batch_1     # Upload batch_1 to S3
  python scripts/split_and_upload.py --upload batch_error # Upload error batch to S3
  python scripts/split_and_upload.py --upload batch_success # Upload success batch to S3
  python scripts/split_and_upload.py --clear              # Clear all data from S3
  python scripts/split_and_upload.py --list               # List current S3 files
  python scripts/split_and_upload.py --upload_full        # Upload FULL datasets to S3
"""

import os
import sys
import csv
import json
import random
import shutil
import argparse
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

# Config
DATA_DIR = Path("D:/1. Warehouse_Project/Olist_ETL_Project_Data")
OUTPUT_DIR = PROJECT_ROOT / "data" / "batches"

S3_BUCKET = os.getenv("S3_BUCKET", "ecommerce-raw-data-olist")
S3_PREFIX = os.getenv("S3_PREFIX", "olist/")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

NUM_BATCHES = 4  # Clean batches

# Table → S3 subfolder mapping (must match ingestion config)
TABLE_MAP = {
    "olist_orders_dataset.csv": "orders",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "payments",
    "olist_order_reviews_dataset.csv": "reviews",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_sellers_dataset.csv": "sellers",
}


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=AWS_REGION,
    )


# 1. SPLIT
def split_data():
    """Split each CSV into 4 clean batches + 1 error batch."""
    if OUTPUT_DIR.exists():
        print(f"🧹 Clearing existing batches in {OUTPUT_DIR}...")
        shutil.rmtree(OUTPUT_DIR)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for csv_file, table_name in TABLE_MAP.items():
        filepath = DATA_DIR / csv_file
        if not filepath.exists():
            print(f"Skipping {csv_file}: file not found")
            continue

        df = pd.read_csv(filepath)
        total = len(df)
        batch_size = total // NUM_BATCHES

        print(f"\nSplitting {csv_file} ({total:,} rows) -> {NUM_BATCHES} batches of ~{batch_size:,}")

        # Ensure no shuffling so referential integrity is preserved sequentially
        df = df.reset_index(drop=True)

        for i in range(NUM_BATCHES):
            start = i * batch_size
            end = (i + 1) * batch_size if i < NUM_BATCHES - 1 else total
            batch_df = df.iloc[start:end]

            batch_dir = OUTPUT_DIR / f"batch_{i + 1}" / table_name
            batch_dir.mkdir(parents=True, exist_ok=True)
            new_filename = f"batch_{i+1}_{csv_file}"
            out_path = batch_dir / new_filename
            batch_df.to_csv(out_path, index=False)
            print(f"   batch_{i + 1}/{table_name}: {len(batch_df):,} rows -> {out_path.name}")

    # Create error batch
    _create_error_batch()

    print("\n" + "=" * 60)
    print("Clean Splitting complete! Batches saved to:", OUTPUT_DIR)
    print("=" * 60)


def _create_error_batch():
    """Create a batch with intentionally corrupted data for demo."""
    print("\nCreating ERROR batch (corrupted data for demo)...")

    error_dir = OUTPUT_DIR / "batch_error"
    error_dir.mkdir(parents=True, exist_ok=True)

    # --- 1. Corrupted Orders: missing columns, bad dates, null IDs ---
    orders_path = DATA_DIR / "olist_orders_dataset.csv"
    if orders_path.exists():
        df = pd.read_csv(orders_path).head(500)

        # Corrupt 30% of customer_ids to NULL
        null_mask = df.sample(frac=0.3, random_state=99).index
        df.loc[null_mask, "customer_id"] = None

        # Add invalid dates
        df.loc[df.index[:50], "order_purchase_timestamp"] = "INVALID-DATE"

        # Add duplicate order_ids (violates uniqueness)
        dupes = df.head(20).copy()
        df = pd.concat([df, dupes], ignore_index=True)

        # Add impossible status values
        df.loc[df.index[-30:], "order_status"] = "EXPLODED"

        out_dir = error_dir / "orders"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_filename = "batch_error_olist_orders_dataset.csv"
        df.to_csv(out_dir / out_filename, index=False)
        print(f"   error/orders: {len(df)} rows (nulls, bad dates, dupes, invalid status)")

    # --- 2. Corrupted Order Items: negative prices, missing FKs ---
    items_path = DATA_DIR / "olist_order_items_dataset.csv"
    if items_path.exists():
        df = pd.read_csv(items_path).head(500)

        # Negative prices
        df.loc[df.index[:100], "price"] = -999.99

        # NULL product_id (FK violation)
        df.loc[df.index[100:200], "product_id"] = None

        # Absurdly high freight
        df.loc[df.index[200:250], "freight_value"] = 999999.99

        out_dir = error_dir / "order_items"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_filename = "batch_error_olist_order_items_dataset.csv"
        df.to_csv(out_dir / out_filename, index=False)
        print(f"   error/order_items: {len(df)} rows (negative prices, null FKs, absurd freight)")

    # --- 3. Corrupted Customers: malformed zip codes ---
    cust_path = DATA_DIR / "olist_customers_dataset.csv"
    if cust_path.exists():
        df = pd.read_csv(cust_path).head(300)

        # Malformed zip codes
        df.loc[df.index[:100], "customer_zip_code_prefix"] = "XXXXX"

        # Empty state
        df.loc[df.index[100:150], "customer_state"] = ""

        out_dir = error_dir / "customers"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_filename = "batch_error_olist_customers_dataset.csv"
        df.to_csv(out_dir / out_filename, index=False)
        print(f"   error/customers: {len(df)} rows (bad zips, empty states)")

    # --- 4. Completely malformed CSV (will cause parse failure) ---
    broken_dir = error_dir / "payments"
    broken_dir.mkdir(parents=True, exist_ok=True)
    with open(broken_dir / "batch_error_olist_order_payments_dataset.csv", "w") as f:
        f.write("THIS IS NOT A CSV FILE\n")
        f.write("{{{{CORRUPTED JSON}}}}\n")
        f.write("random,garbage,with,wrong,columns\n")
        f.write("1,2,3,4,5\n")
    print("   error/payments: Completely malformed file (parse failure)")

    print("   Error batch created!")


# 2. UPLOAD
def upload_batch(batch_name: str):
    """Upload a specific batch to S3."""
    batch_dir = OUTPUT_DIR / batch_name
    if not batch_dir.exists():
        print(f"❌ Batch '{batch_name}' not found at {batch_dir}")
        print(f"   Available: {[d.name for d in OUTPUT_DIR.iterdir() if d.is_dir()]}")
        return

    s3 = get_s3_client()
    uploaded = 0

    for table_dir in batch_dir.iterdir():
        if not table_dir.is_dir():
            continue

        table_name = table_dir.name  # e.g., "orders"

        for csv_file in table_dir.glob("*.csv"):
            s3_key = f"{S3_PREFIX}{table_name}/{csv_file.name}"

            print(f"   Uploading {csv_file.name} -> s3://{S3_BUCKET}/{s3_key}")
            s3.upload_file(str(csv_file), S3_BUCKET, s3_key)
            uploaded += 1

    print(f"\nUploaded {uploaded} files from '{batch_name}' to S3")


# 2b. UPLOAD FULL (no batching)
def upload_full():
    """Upload the complete original CSV files directly to S3 (no splitting)."""
    s3 = get_s3_client()
    uploaded = 0

    print(f"\nUploading FULL datasets to s3://{S3_BUCKET}/{S3_PREFIX}")
    print("=" * 60)

    for csv_file, table_name in TABLE_MAP.items():
        filepath = DATA_DIR / csv_file
        if not filepath.exists():
            print(f"   Skipping {csv_file}: file not found at {filepath}")
            continue

        s3_key = f"{S3_PREFIX}{table_name}/{csv_file}"
        file_size_mb = filepath.stat().st_size / (1024 * 1024)

        print(f"   {csv_file} ({file_size_mb:.1f} MB) -> s3://{S3_BUCKET}/{s3_key}")
        s3.upload_file(str(filepath), S3_BUCKET, s3_key)
        uploaded += 1

    print("=" * 60)
    print(f"Uploaded {uploaded} FULL files to S3 (no batching)")
    print("   Next steps:")
    print("   1. Run: .venv\\Scripts\\python.exe src/ingestion/s3_to_snowflake.py")
    print("   2. Run: cd src/dbt && dbt run --full-refresh && dbt test")


# 3. CLEAR
def clear_s3():
    """Remove all data files from the S3 prefix."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    deleted = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
            print(f"   Deleted: {obj['Key']}")
            deleted += 1

    print(f"\nDeleted {deleted} files from s3://{S3_BUCKET}/{S3_PREFIX}")


# 4. LIST
def list_s3():
    """List all files in the S3 prefix."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    print(f"\nFiles in s3://{S3_BUCKET}/{S3_PREFIX}")
    print("-" * 70)

    total = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            size_kb = obj["Size"] / 1024
            print(f"   {obj['Key']:55s} {size_kb:8.1f} KB")
            total += 1

    print("-" * 70)
    print(f"Total: {total} files")


# CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split Olist data & upload to S3")
    parser.add_argument("--split", action="store_true", help="Split CSVs into batches")
    parser.add_argument("--upload", type=str, help="Upload a batch (e.g., batch_1, batch_error)")
    parser.add_argument("--upload-full", action="store_true", help="Upload FULL CSVs (no batching)")
    parser.add_argument("--clear", action="store_true", help="Clear all S3 data")
    parser.add_argument("--list", action="store_true", help="List S3 files")
    args = parser.parse_args()

    if args.split:
        split_data()
    elif args.upload:
        upload_batch(args.upload)
    elif args.upload_full:
        upload_full()
    elif args.clear:
        clear_s3()
    elif args.list:
        list_s3()
    else:
        parser.print_help()
