"""
Handles ingestion of raw Olist CSVs from S3 into Snowflake's Bronze layer.
We use schema-on-read (VARIANT) and keep track of file-level lineage.
"""

import os
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

import yaml
import boto3
import snowflake.connector
from snowflake.connector import DictCursor
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


class DataIngestion:
    """
    Ingests S3 files into Snowflake using VARIANT columns for schema evolution.
    Keeps track of loaded files to support incremental processing.
    """

    # Bronze table definitions - each maps to one Olist CSV
    BRONZE_TABLES = {
        "orders": {
            "s3_prefix": "orders/",
            "file_pattern": "olist_orders_dataset",
        },
        "customers": {
            "s3_prefix": "customers/",
            "file_pattern": "olist_customers_dataset",
        },
        "products": {
            "s3_prefix": "products/",
            "file_pattern": "olist_products_dataset",
        },
        "order_items": {
            "s3_prefix": "order_items/",
            "file_pattern": "olist_order_items_dataset",
        },
        "payments": {
            "s3_prefix": "payments/",
            "file_pattern": "olist_order_payments_dataset",
        },
        "reviews": {
            "s3_prefix": "reviews/",
            "file_pattern": "olist_order_reviews_dataset",
        },
        "geolocation": {
            "s3_prefix": "geolocation/",
            "file_pattern": "olist_geolocation_dataset",
        },
        "sellers": {
            "s3_prefix": "sellers/",
            "file_pattern": "olist_sellers_dataset",
        },
    }

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the DataIngestion client.

        Args:
            config_path: Path to YAML configuration file.
                         Falls back to environment variables if not provided.
        """
        self.config = self._load_config(config_path)
        self.sf_conn = None
        self.s3_client = None
        self._connect()

    # Configuration & Connection

    def _load_config(self, config_path: Optional[str]) -> dict:
        """Load configuration from YAML file or environment variables."""
        if config_path and os.path.exists(config_path):
            with open(config_path, "r") as f:
                return yaml.safe_load(f)

        # Fallback to environment variables
        return {
            "snowflake": {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMMERCE_WH"),
                "database": os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DW"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
            },
            "aws": {
                "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
                "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "region": os.getenv("AWS_REGION", "us-east-1"),
                "s3_bucket": os.getenv("S3_BUCKET", "ecommerce-raw-data"),
                "s3_prefix": os.getenv("S3_PREFIX", "olist/"),
            },
            "ingestion": {
                "batch_size": int(os.getenv("INGESTION_BATCH_SIZE", "10000")),
                "max_retries": int(os.getenv("INGESTION_MAX_RETRIES", "3")),
            },
        }

    def _connect(self):
        """Establish connections to Snowflake and AWS S3."""
        sf_cfg = self.config["snowflake"]
        self.sf_conn = snowflake.connector.connect(
            account=sf_cfg["account"],
            user=sf_cfg["user"],
            password=sf_cfg["password"],
            role=sf_cfg["role"],
            warehouse=sf_cfg["warehouse"],
            database=sf_cfg["database"],
            schema=sf_cfg["schema"],
        )
        logger.info(
            f"Connected to Snowflake: {sf_cfg['database']}.{sf_cfg['schema']}"
        )

        aws_cfg = self.config["aws"]
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_cfg["access_key_id"],
            aws_secret_access_key=aws_cfg["secret_access_key"],
            region_name=aws_cfg["region"],
        )
        logger.info(f"Connected to AWS S3: {aws_cfg['s3_bucket']}")

    def close(self):
        """Close Snowflake connection."""
        if self.sf_conn:
            self.sf_conn.close()
            logger.info("Snowflake connection closed.")

    # Bronze Table Management

    def create_bronze_tables(self):
        """
        Create Bronze layer tables with VARIANT storage and metadata columns.
        Also creates the file lineage tracking table.
        """
        cur = self.sf_conn.cursor()
        db = self.config["snowflake"]["database"]
        schema = self.config["snowflake"]["schema"]

        try:
            # Ensure database and schema exist
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")
            cur.execute(f"USE SCHEMA {db}.{schema}")

            # Create Bronze tables with VARIANT column + metadata
            for table_name in self.BRONZE_TABLES:
                ddl = f"""
                CREATE TABLE IF NOT EXISTS bronze_{table_name} (
                    raw_data        VARIANT         COMMENT 'Raw record as semi-structured data',
                    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                        COMMENT 'When this record was ingested',
                    source_file     VARCHAR(500)    COMMENT 'S3 key of the source file',
                    file_timestamp  TIMESTAMP_NTZ   COMMENT 'Last modified time of source file',
                    checksum        VARCHAR(64)     COMMENT 'SHA-256 hash of the raw record'
                )
                DATA_RETENTION_TIME_IN_DAYS = 1
                COMMENT = 'Bronze layer: raw {table_name} data (schema-on-read)'
                """
                cur.execute(ddl)
                logger.info(f"Created/verified table: bronze_{table_name}")

            # File lineage tracking table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS _file_lineage (
                    lineage_id          VARCHAR(64)     PRIMARY KEY,
                    source_file         VARCHAR(500)    NOT NULL,
                    target_table        VARCHAR(100)    NOT NULL,
                    file_size_bytes     INTEGER,
                    file_checksum       VARCHAR(64),
                    rows_loaded         INTEGER,
                    ingestion_status    VARCHAR(20)     DEFAULT 'PENDING',
                    ingestion_start     TIMESTAMP_NTZ,
                    ingestion_end       TIMESTAMP_NTZ,
                    error_message       VARCHAR(5000),
                    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
                )
                COMMENT = 'File-level lineage tracking for Bronze ingestion'
            """)
            logger.info("Created/verified table: _file_lineage")

            # Schema drift tracking table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS _schema_drift_log (
                    drift_id            VARCHAR(64)     PRIMARY KEY,
                    table_name          VARCHAR(100)    NOT NULL,
                    source_file         VARCHAR(500),
                    previous_columns    VARIANT,
                    current_columns     VARIANT,
                    drift_type          VARCHAR(50),
                    detected_at         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
                    acknowledged        BOOLEAN         DEFAULT FALSE
                )
                COMMENT = 'Schema drift detection log'
            """)
            logger.info("Created/verified table: _schema_drift_log")

            self.sf_conn.commit()
            logger.success("All Bronze tables created successfully.")

        except Exception as e:
            logger.error(f"Error creating Bronze tables: {e}")
            raise
        finally:
            cur.close()

    # Data Ingestion

    def ingest_to_bronze(self, table_name: Optional[str] = None):
        """
        Ingest data from S3 into Bronze layer.

        Args:
            table_name: Specific table to ingest. If None, ingest all tables.
        """
        tables = (
            {table_name: self.BRONZE_TABLES[table_name]}
            if table_name
            else self.BRONZE_TABLES
        )

        results = {}
        for tbl_name, tbl_config in tables.items():
            try:
                logger.info(f"Starting ingestion for: bronze_{tbl_name}")
                rows_loaded = self._ingest_table(tbl_name, tbl_config)
                results[tbl_name] = {
                    "status": "SUCCESS",
                    "rows_loaded": rows_loaded,
                }
                logger.success(
                    f"Ingested {rows_loaded} rows into bronze_{tbl_name}"
                )
            except Exception as e:
                results[tbl_name] = {"status": "FAILED", "error": str(e)}
                logger.error(
                    f"Failed to ingest bronze_{tbl_name}: {e}"
                )
                # Continue on individual file failures
                continue

        return results

    def _ingest_table(self, table_name: str, config: dict) -> int:
        """
        Ingest a single table from S3 to Bronze.

        Strategy:
        1. List files in S3 prefix
        2. Get pending files (partially loaded or entirely new)
        3. For each file, parse CSV and insert a batch of up to `daily_batch_size` rows
        4. Track lineage (PARTIAL or SUCCESS)
        """
        aws_cfg = self.config["aws"]
        bucket = aws_cfg["s3_bucket"]
        prefix = aws_cfg["s3_prefix"] + config["s3_prefix"]

        # List S3 files
        s3_files = self._list_s3_files(bucket, prefix, config["file_pattern"])
        if not s3_files:
            logger.warning(f"No files found for {table_name} at s3://{bucket}/{prefix}")
            return 0

        # Get pending files (handles partial daily loads)
        pending_files = self._get_files_to_process(s3_files, table_name)
        if not pending_files:
            logger.info(f"No new or partial files to ingest for {table_name}")
            return 0

        logger.info(f"Found {len(pending_files)} files with pending data for {table_name}")

        total_rows = 0
        cur = self.sf_conn.cursor()
        
        # Max rows to process per script run (simulate daily arrival)
        daily_limit = self.config.get("ingestion", {}).get("daily_batch_size", 1000000)

        try:
            for s3_file in pending_files:
                lineage_id = hashlib.sha256(
                    f"{s3_file['key']}_{datetime.now(timezone.utc).isoformat()}".encode()
                ).hexdigest()[:16]

                # Track lineage start
                self._record_lineage_start(
                    cur, lineage_id, s3_file, table_name
                )
                
                rows_inserted_this_run = 0
                status = "FAILED"

                try:
                    # Download and parse full file
                    records = self._download_and_parse(bucket, s3_file)
                    
                    start_idx = int(s3_file.get("rows_loaded", 0))

                    # Detect schema drift only when starting a new file
                    if start_idx == 0:
                        self._detect_schema_drift(
                            cur, table_name, s3_file["key"], records
                        )

                    # Slice for this batch run
                    end_idx = min(start_idx + daily_limit, len(records))
                    records_to_insert = records[start_idx:end_idx]
                    
                    if not records_to_insert:
                        # Fully loaded
                        self._record_lineage_end(cur, lineage_id, 0, "SUCCESS")
                        continue

                    # Insert records as VARIANT
                    rows_inserted_this_run = self._insert_records(
                        cur, table_name, s3_file, records_to_insert
                    )
                    total_rows += rows_inserted_this_run
                    
                    # If we reached the end of the file, status is 'SUCCESS', else 'PARTIAL'
                    if end_idx >= len(records):
                        status = "SUCCESS"
                    else:
                        status = "PARTIAL"
                        logger.info(f"  Reached daily limit ({daily_limit}). Paused at row {end_idx}.")

                    # Track lineage success/partial
                    self._record_lineage_end(
                        cur, lineage_id, rows_inserted_this_run, status
                    )

                except Exception as e:
                    self._record_lineage_end(
                        cur, lineage_id, rows_inserted_this_run, "FAILED", str(e)
                    )
                    logger.error(f"Error processing {s3_file['key']}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue

            self.sf_conn.commit()

        finally:
            cur.close()

        return total_rows

    def _list_s3_files(
        self, bucket: str, prefix: str, pattern: str
    ) -> list[dict]:
        """List files in S3 matching the given prefix and pattern."""
        files = []
        paginator = self.s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if pattern in obj["Key"]:
                    files.append(
                        {
                            "key": obj["Key"],
                            "size": obj["Size"],
                            "last_modified": obj["LastModified"],
                            "etag": obj["ETag"].strip('"'),
                        }
                    )

        return files

    def _get_files_to_process(
        self, s3_files: list[dict], table_name: str
    ) -> list[dict]:
        """Return files that need ingestion, checking the ACTUAL bronze table."""
        cur = self.sf_conn.cursor(DictCursor)
        tbl_full = f"bronze_{table_name}"
        
        try:
            # Check if the table exists first
            cur.execute(f"SHOW TABLES LIKE '{tbl_full}'")
            if not cur.fetchone():
                return [{**f, "rows_loaded": 0} for f in s3_files]

            # Query the actual number of rows loaded per file
            cur.execute(f"""
                SELECT source_file, COUNT(*) as actual_rows
                FROM {tbl_full}
                GROUP BY source_file
            """)
            
            loaded_counts = {
                row["SOURCE_FILE"]: row["ACTUAL_ROWS"]
                for row in cur.fetchall()
            }
        except Exception as e:
            logger.warning(f"Could not query {tbl_full} state (might be empty): {e}")
            loaded_counts = {}
        finally:
            cur.close()

        files_to_process = []
        for f in s3_files:
            # Use the S3 Key
            source_key = f["key"]
            rows_already_loaded = loaded_counts.get(source_key, 0)
            
            # If we don't know the exact total file rows until download,
            # we consider it "pending" if rows_loaded is less than a large number
            # But during download_and_parse we will slice from `rows_already_loaded`
            files_to_process.append({
                **f,
                "rows_loaded": rows_already_loaded
            })
            
        return files_to_process

    def _download_and_parse(
        self, bucket: str, s3_file: dict
    ) -> list[dict]:
        """Download a CSV file from S3 and parse into list of dicts."""
        import csv
        import io

        response = self.s3_client.get_object(
            Bucket=bucket, Key=s3_file["key"]
        )
        content = response["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)

    def _insert_records(
        self,
        cur,
        table_name: str,
        s3_file: dict,
        records: list[dict],
    ) -> int:
        """Insert parsed records into Bronze table as VARIANT using manual batching."""
        if not records:
            return 0

        # Use explicitly 5000 to keep query string size safe
        batch_size = 5000
        total_inserted = 0
        source_key = s3_file['key'].replace("'", "''")
        file_ts = s3_file['last_modified'].isoformat()

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            values_list = []

            for record in batch:
                raw_json = json.dumps(record)
                # Protect against existing $$ in JSON string
                raw_json = raw_json.replace("$$", "\\$\\$")
                checksum = hashlib.sha256(raw_json.encode()).hexdigest()
                
                values_list.append(
                    f"($${raw_json}$$, "
                    f"'{source_key}', "
                    f"'{file_ts}', "
                    f"'{checksum}')"
                )

            insert_sql = f"""
                INSERT INTO bronze_{table_name}
                    (raw_data, ingestion_timestamp, source_file,
                     file_timestamp, checksum)
                SELECT 
                    PARSE_JSON(column1), 
                    CURRENT_TIMESTAMP(), 
                    column2, 
                    column3, 
                    column4
                FROM VALUES 
                    {', '.join(values_list)}
            """

            # Perform string-interpolated bulk insert
            cur.execute(insert_sql)

            total_inserted += len(batch)
            logger.info(
                f"  Inserted batch {i // batch_size + 1}: "
                f"{len(batch)} rows into bronze_{table_name}"
            )

        return total_inserted

    # Schema Drift Detection

    def _detect_schema_drift(
        self,
        cur,
        table_name: str,
        source_file: str,
        records: list[dict],
    ):
        """
        Detect schema changes by comparing current file columns
        against previously seen columns for this table.
        """
        if not records:
            return

        current_columns = sorted(records[0].keys())

        cur.execute(
            """
            SELECT current_columns
            FROM _schema_drift_log
            WHERE table_name = %s
            ORDER BY detected_at DESC
            LIMIT 1
        """,
            (f"bronze_{table_name}",),
        )
        row = cur.fetchone()

        if row is not None:
            previous_columns = sorted(json.loads(row[0]))
            if current_columns != previous_columns:
                added = set(current_columns) - set(previous_columns)
                removed = set(previous_columns) - set(current_columns)
                drift_type = []
                if added:
                    drift_type.append(f"ADDED: {added}")
                if removed:
                    drift_type.append(f"REMOVED: {removed}")

                drift_id = hashlib.sha256(
                    f"{table_name}_{datetime.now(timezone.utc).isoformat()}".encode()
                ).hexdigest()[:16]

                cur.execute(
                    """
                    INSERT INTO _schema_drift_log
                        (drift_id, table_name, source_file,
                         previous_columns, current_columns, drift_type)
                    SELECT %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s
                """,
                    (
                        drift_id,
                        f"bronze_{table_name}",
                        source_file,
                        json.dumps(previous_columns),
                        json.dumps(current_columns),
                        "; ".join(drift_type)[:50],
                    ),
                )
                logger.warning(
                    f"Schema drift detected for {table_name}: "
                    f"{'; '.join(drift_type)}"
                )
        else:
            # First time - record baseline schema
            drift_id = hashlib.sha256(
                f"{table_name}_baseline".encode()
            ).hexdigest()[:16]
            cur.execute(
                """
                INSERT INTO _schema_drift_log
                    (drift_id, table_name, source_file,
                     previous_columns, current_columns, drift_type)
                SELECT %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s
            """,
                (
                    drift_id,
                    f"bronze_{table_name}",
                    source_file,
                    json.dumps(current_columns),
                    json.dumps(current_columns),
                    "BASELINE",
                ),
            )

    # Lineage Tracking

    def _record_lineage_start(
        self, cur, lineage_id: str, s3_file: dict, table_name: str
    ):
        """Record the start of a file ingestion in the lineage table."""
        cur.execute(
            """
            INSERT INTO _file_lineage
                (lineage_id, source_file, target_table,
                 file_size_bytes, file_checksum,
                 ingestion_status, ingestion_start)
            VALUES (%s, %s, %s, %s, %s, 'IN_PROGRESS', CURRENT_TIMESTAMP())
        """,
            (
                lineage_id,
                s3_file["key"],
                f"bronze_{table_name}",
                s3_file["size"],
                s3_file["etag"],
            ),
        )

    def _record_lineage_end(
        self,
        cur,
        lineage_id: str,
        rows_loaded: int,
        status: str,
        error_message: str = None,
    ):
        """Record the completion of a file ingestion."""
        cur.execute(
            """
            UPDATE _file_lineage
            SET rows_loaded = %s,
                ingestion_status = %s,
                ingestion_end = CURRENT_TIMESTAMP(),
                error_message = %s
            WHERE lineage_id = %s
        """,
            (rows_loaded, status, error_message, lineage_id),
        )

    # Validation

    def validate_load(self, table_name: Optional[str] = None) -> dict:
        """
        Validate the Bronze layer load by checking:
        - Row counts per table
        - Null check on critical metadata columns
        - Freshness (last ingestion timestamp)

        Args:
            table_name: Specific table to validate. If None, validate all.

        Returns:
            Dictionary of validation results per table.
        """
        tables = [table_name] if table_name else list(self.BRONZE_TABLES.keys())
        results = {}
        cur = self.sf_conn.cursor(DictCursor)

        try:
            for tbl in tables:
                tbl_full = f"bronze_{tbl}"
                validations = {}

                # Row count
                cur.execute(f"SELECT COUNT(*) AS cnt FROM {tbl_full}")
                validations["row_count"] = cur.fetchone()["CNT"]

                # Null metadata check
                cur.execute(
                    f"""
                    SELECT
                        COUNT_IF(raw_data IS NULL) AS null_raw_data,
                        COUNT_IF(source_file IS NULL) AS null_source_file,
                        COUNT_IF(checksum IS NULL) AS null_checksum
                    FROM {tbl_full}
                """
                )
                null_check = cur.fetchone()
                validations["null_raw_data"] = null_check["NULL_RAW_DATA"]
                validations["null_source_file"] = null_check["NULL_SOURCE_FILE"]
                validations["null_checksum"] = null_check["NULL_CHECKSUM"]

                # Freshness
                cur.execute(
                    f"""
                    SELECT MAX(ingestion_timestamp) AS latest
                    FROM {tbl_full}
                """
                )
                latest = cur.fetchone()["LATEST"]
                validations["latest_ingestion"] = (
                    latest.isoformat() if latest else None
                )

                # Overall pass/fail
                validations["is_valid"] = (
                    validations["row_count"] > 0
                    and validations["null_raw_data"] == 0
                    and validations["null_source_file"] == 0
                )

                results[tbl] = validations
                status = "PASS" if validations["is_valid"] else "FAIL"
                logger.info(
                    f"Validation {status}: {tbl_full} "
                    f"({validations['row_count']} rows)"
                )

        finally:
            cur.close()

        return results

    # Data Profiling

    def profile_table(self, table_name: str) -> dict:
        """
        Generate a data profile for a Bronze table.
        Extracts column names, types, null rates, and cardinality from VARIANT data.
        """
        cur = self.sf_conn.cursor(DictCursor)
        tbl_full = f"bronze_{table_name}"

        try:
            # Sample 1000 rows to detect schema
            cur.execute(
                f"""
                SELECT DISTINCT f.key AS column_name
                FROM {tbl_full},
                     LATERAL FLATTEN(input => raw_data) f
                LIMIT 1000
            """
            )
            columns = [row["COLUMN_NAME"] for row in cur.fetchall()]

            profile = {"table": tbl_full, "columns": {}}

            for col in columns:
                cur.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT_IF(raw_data:{col}::STRING IS NULL) AS null_count,
                        COUNT(DISTINCT raw_data:{col}::STRING) AS distinct_count,
                        MIN(LEN(raw_data:{col}::STRING)) AS min_length,
                        MAX(LEN(raw_data:{col}::STRING)) AS max_length
                    FROM {tbl_full}
                """
                )
                stats = cur.fetchone()
                total = stats["TOTAL_ROWS"] or 1
                profile["columns"][col] = {
                    "total_rows": total,
                    "null_count": stats["NULL_COUNT"],
                    "null_rate": round(stats["NULL_COUNT"] / total * 100, 2),
                    "distinct_count": stats["DISTINCT_COUNT"],
                    "min_length": stats["MIN_LENGTH"],
                    "max_length": stats["MAX_LENGTH"],
                }

            return profile

        finally:
            cur.close()


# CLI Entry Point

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest data from S3 into Snowflake Bronze layer"
    )
    parser.add_argument(
        "--config", type=str, default=None, help="Path to config YAML"
    )
    parser.add_argument(
        "--table", type=str, default=None, help="Specific table to ingest"
    )
    parser.add_argument(
        "--create-tables",
        action="store_true",
        help="Create Bronze tables before ingestion",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run validation after ingestion",
    )
    parser.add_argument(
        "--profile",
        type=str,
        default=None,
        help="Profile a specific table",
    )
    args = parser.parse_args()

    ingestion = DataIngestion(config_path=args.config)

    try:
        if args.create_tables:
            ingestion.create_bronze_tables()

        if args.table or not (args.create_tables or args.validate or args.profile):
            results = ingestion.ingest_to_bronze(table_name=args.table)
            logger.info(f"Ingestion results: {json.dumps(results, indent=2)}")

        if args.validate:
            validation = ingestion.validate_load(table_name=args.table)
            logger.info(
                f"Validation results: {json.dumps(validation, indent=2, default=str)}"
            )

        if args.profile:
            profile = ingestion.profile_table(args.profile)
            logger.info(
                f"Profile for {args.profile}:\n"
                f"{json.dumps(profile, indent=2)}"
            )
    finally:
        ingestion.close()
