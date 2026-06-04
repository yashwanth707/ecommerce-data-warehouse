"""
Bronze Ingestion Runner
=======================
Loads .env, creates Bronze tables, ingests all data from S3,
validates the load, and prints a summary.
"""

import os
import sys
import json
from pathlib import Path

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Load .env file manually (avoid dotenv dependency issues)
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ[key.strip()] = value.strip()
    print("[OK] Loaded environment from", env_path)
else:
    print("[WARN] .env file not found, using existing environment variables")

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from ingestion.s3_to_snowflake import DataIngestion
from loguru import logger

def main():
    print("=" * 60)
    print("  Bronze Layer Ingestion - E-Commerce Data Warehouse")
    print("=" * 60)

    # Initialize ingestion client
    print("\n[1/3] Connecting to Snowflake and AWS S3...")
    ingestion = DataIngestion()

    try:
        # Step 1: Create Bronze tables
        print("\n[STEP 1] Creating Bronze tables...")
        ingestion.create_bronze_tables()
        print("   [OK] All Bronze tables created/verified")

        # Step 2: Ingest data from S3
        print("\n[STEP 2] Ingesting data from S3 to Bronze layer...")
        print("   This may take a few minutes for large files (geolocation)...\n")
        results = ingestion.ingest_to_bronze()

        # Print results
        print("\n" + "=" * 60)
        print("  INGESTION RESULTS")
        print("=" * 60)
        total_rows = 0
        for table, result in results.items():
            status = result.get("status", "UNKNOWN")
            rows = result.get("rows_loaded", 0)
            total_rows += rows if rows else 0
            if status == "SUCCESS":
                print(f"  [OK] bronze_{table}: {rows:,} rows loaded")
            else:
                error = result.get("error", "Unknown error")
                print(f"  [FAIL] bronze_{table}: FAILED - {error[:200]}")

        print(f"\n  Total rows loaded: {total_rows:,}")

        # Step 3: Validate
        print("\n[STEP 3] Validating Bronze layer...")
        validation = ingestion.validate_load()

        print("\n" + "=" * 60)
        print("  VALIDATION RESULTS")
        print("=" * 60)
        all_valid = True
        for table, val in validation.items():
            valid = val["is_valid"]
            all_valid = all_valid and valid
            tag = "[PASS]" if valid else "[FAIL]"
            print(
                f"  {tag} bronze_{table}: "
                f"{val['row_count']:,} rows | "
                f"Nulls(data={val['null_raw_data']}, "
                f"file={val['null_source_file']}) | "
                f"Latest: {val.get('latest_ingestion', 'N/A')}"
            )

        print("\n" + "=" * 60)
        if all_valid:
            print("  ALL VALIDATIONS PASSED - Bronze layer is ready!")
        else:
            print("  Some validations failed - check above for details")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        ingestion.close()


if __name__ == "__main__":
    main()
