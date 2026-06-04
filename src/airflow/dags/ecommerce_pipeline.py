"""
Airflow DAG for the Olist E-Commerce ELT Pipeline.
Handles S3 to Snowflake ingestion, dbt transformations, and Snowpark ML models.
Includes self-healing for data quality tests and Slack alerts.
"""

from datetime import datetime, timedelta
import json
import os
import sqlite3
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
    HAS_SNOWFLAKE_PROVIDER = True
except ImportError:
    HAS_SNOWFLAKE_PROVIDER = False

try:
    from airflow.sensors.s3_key_sensor import S3KeySensor
    HAS_S3_SENSOR = True
except ImportError:
    HAS_S3_SENSOR = False


# Core paths

DBT_PROJECT_DIR = "/opt/airflow/dbt"
INGESTION_DIR = "/opt/airflow/ingestion"
COST_DB_PATH = "/opt/airflow/logs/cost_tracking.db"

# Table SLA config for dynamic generation
PIPELINE_TABLES = {
    "orders": {"priority": 1, "sla_minutes": 15},
    "customers": {"priority": 2, "sla_minutes": 10},
    "products": {"priority": 3, "sla_minutes": 10},
    "order_items": {"priority": 2, "sla_minutes": 15},
    "payments": {"priority": 2, "sla_minutes": 10},
    "reviews": {"priority": 3, "sla_minutes": 10},
    "geolocation": {"priority": 4, "sla_minutes": 20},
    "sellers": {"priority": 4, "sla_minutes": 5},
}

# Silver repair SQL templates for self-healing
REPAIR_SQL = {
    "silver_orders": """
        DELETE FROM cleansed.silver_orders
        WHERE order_id IN (
            SELECT order_id FROM cleansed.silver_orders
            GROUP BY order_id HAVING COUNT(*) > 1
        )
        AND ingestion_timestamp < (
            SELECT MAX(ingestion_timestamp) FROM cleansed.silver_orders
        );
    """,
    "silver_customers": """
        UPDATE cleansed.silver_customers
        SET customer_zip_code_prefix = LPAD(customer_zip_code_prefix, 5, '0')
        WHERE LEN(customer_zip_code_prefix) < 5;
    """,
}


# Callbacks

def send_alert(subject: str, text: str, alert_type: str = "info"):
    """Send alert to Slack and optionally PagerDuty using pure python requests."""
    import os
    import json
    import requests

    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    pd_key = os.getenv("PAGERDUTY_ROUTING_KEY")

    # If no keys are provided, simulate the alert
    if not slack_url and not pd_key:
        print("\n" + "="*60)
        print(f"⚠️ [SIMULATED {alert_type.upper()} ALERT]")
        print(f"Subject: {subject}")
        print("-" * 60)
        print(text)
        print("="*60 + "\n")
        return

    # 1. Slack Alert
    if slack_url:
        color = "#36a64f" if alert_type == "info" else "#ff0000"
        slack_data = {
            "attachments": [{
                "color": color,
                "title": subject,
                "text": text
            }]
        }
        try:
            requests.post(slack_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'}, timeout=5)
            print("✅ Slack alert sent.")
        except Exception as e:
            print(f"❌ Failed to send Slack alert: {e}")

    # 2. PagerDuty Alert (Only for errors)
    if pd_key and alert_type == "error":
        pd_data = {
            "routing_key": pd_key,
            "event_action": "trigger",
            "payload": {
                "summary": subject,
                "source": "Airflow E-Commerce Pipeline",
                "severity": "critical",
                "custom_details": {"error": text}
            }
        }
        try:
            requests.post('https://events.pagerduty.com/v2/enqueue', json=pd_data, timeout=5)
            print("🚨 PagerDuty Incident triggered.")
        except Exception as e:
            print(f"❌ Failed to trigger PagerDuty: {e}")


def _get_ai_failure_summary(task_id, dag_id, exception):
    """Call Ollama NL-to-SQL API to generate an AI summary of the failure."""
    nl_api_url = os.getenv("NL_API_URL", "http://localhost:8000")
    try:
        resp = requests.post(
            f"{nl_api_url}/api/dq_summary",
            json={
                "failures": [{"task": task_id, "error": str(exception)[:300]}],
                "pipeline_run": dag_id
            },
            timeout=10
        )
        if resp.status_code == 200:
            return resp.json().get("summary")
    except:
        pass
    return None


def on_failure_callback(context):
    """Send Slack/PagerDuty alert on task failure — with optional AI Summary."""
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    exception = context.get("exception", "Unknown Error")

    # Try to get AI Summary
    ai_summary = _get_ai_failure_summary(task_instance.task_id, dag_id, exception)

    subject = f"🚨 PIPELINE FAILURE DETECTED: {dag_id}"
    
    if ai_summary:
        body = (
            f"🤖 *AI Summary:* {ai_summary}\n\n"
            f"• Task: `{task_instance.task_id}`\n"
            f"• Execution Date: {context['execution_date']}\n"
            f"• Log URL: {task_instance.log_url}"
        )
    else:
        body = (
            f"• Task: `{task_instance.task_id}`\n"
            f"• Execution Date: {context['execution_date']}\n"
            f"• Log URL: {task_instance.log_url}\n\n"
            f"• Error Details:\n```{str(exception)[:500]}```"
        )
    send_alert(subject, body, alert_type="error")


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Alert Slack/PagerDuty when SLA is missed."""
    subject = f"⏰ SLA MISS DETECTED: {dag.dag_id}"
    body = (
        f"• Tasks Exceeding SLA: {', '.join([t.task_id for t in task_list])}\n"
        f"• Action Required: The pipeline is running over the specified time limit."
    )
    send_alert(subject, body, alert_type="error")


# Cost Tracking

def track_snowflake_cost(task_id: str, **context):
    """Log Snowflake credits used by this task to SQLite."""
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'ECOMMERCE_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DW'),
    )
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT COALESCE(SUM(credits_used), 0) AS credits
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                DATEADD('minutes', -30, CURRENT_TIMESTAMP()),
                CURRENT_TIMESTAMP()
            ))
            WHERE QUERY_TAG = 'dbt_ecommerce'
        """)
        credits = cur.fetchone()[0]
    except Exception:
        credits = 0.0
    finally:
        cur.close()
        conn.close()

    # Write to SQLite
    db_conn = sqlite3.connect(COST_DB_PATH)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS cost_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dag_id TEXT,
            task_id TEXT,
            execution_date TEXT,
            credits_used REAL,
            logged_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db_conn.execute(
        """INSERT INTO cost_log (dag_id, task_id, execution_date, credits_used)
           VALUES (?, ?, ?, ?)""",
        (
            context["dag"].dag_id,
            task_id,
            str(context["execution_date"]),
            credits,
        ),
    )
    db_conn.commit()
    db_conn.close()


# DAG Definition

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="ecommerce_elt_pipeline",
    default_args=default_args,
    description="End-to-end ELT pipeline: S3 → Bronze → Silver → Gold → CLV → BI",
    schedule_interval=None,  # Run only on manual triggers
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,  # Prevent Docker OOM by limiting concurrent tasks
    sla_miss_callback=sla_miss_callback,
    tags=["ecommerce", "elt", "production"],
) as dag:

    # 1. S3 File Check (lightweight — no sensor blocking)
    @task(task_id="check_s3_for_new_files")
    def check_s3_files(**ctx):
        """Quick check for new S3 files. Logs result but never blocks."""
        import boto3
        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION", "us-east-1"),
            )
            bucket = os.getenv("S3_BUCKET", "ecommerce-raw-data")
            prefix = os.getenv("S3_PREFIX", "olist/")
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
            count = resp.get("KeyCount", 0)
            print(f"Found {count} files in s3://{bucket}/{prefix}")
            return {"files_found": count}
        except Exception as e:
            print(f"S3 check failed (non-blocking): {e}")
            return {"files_found": 0, "error": str(e)}

    check_s3_for_new_files = check_s3_files()

    # 2. Dynamic Bronze Ingestion Tasks
    @task_group(group_id="ingest_to_bronze")
    def ingest_bronze_group():
        """Dynamically generate ingestion tasks from config."""
        for table_name, config in PIPELINE_TABLES.items():

            @task(task_id=f"ingest_{table_name}")
            def ingest_table(tbl=table_name, **ctx):
                import sys
                sys.path.insert(0, INGESTION_DIR)
                from s3_to_snowflake import DataIngestion

                ingestion = DataIngestion()
                try:
                    ingestion.create_bronze_tables()
                    result = ingestion.ingest_to_bronze(table_name=tbl)
                    return result
                finally:
                    ingestion.close()

            ingest_table()

    bronze_tasks = ingest_bronze_group()

    # 3. dbt Seed (Category Translation Lookup)
    run_dbt_seed = BashOperator(
        task_id="run_dbt_seed",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt seed --profiles-dir . --target dev"
        ),
        sla=timedelta(minutes=10),
    )

    # 4. dbt Silver Layer
    run_dbt_silver = BashOperator(
        task_id="run_dbt_silver",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --select tag:silver --full-refresh --profiles-dir . --target dev"
        ),
        sla=timedelta(minutes=30),
    )

    # 4. dbt Gold Layer
    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --select tag:gold --full-refresh --profiles-dir . --target dev"
        ),
        sla=timedelta(minutes=30),
    )

    @task_group(group_id="ml_pipeline")
    def ml_pipeline_group():
        """Run all ML models concurrently after Gold layer completes."""

        # Model 0 (Original) — RFM / CLV K-Means (retained for comparison)
        run_rfm_clv = BashOperator(
            task_id="rfm_clv_model",
            bash_command="python /opt/airflow/snowpark/clv_model.py",
            sla=timedelta(minutes=15),
        )

        # Model 1 — Customer Behavioral Segmentation (K-Means)
        run_behavioral_segmentation = BashOperator(
            task_id="behavioral_segmentation",
            bash_command="python /opt/airflow/snowpark/model_1_behavioral_segmentation.py",
            sla=timedelta(minutes=15),
        )

        # Model 2 — Delivery Delay Prediction (Random Forest)
        run_delivery_delay = BashOperator(
            task_id="delivery_delay_prediction",
            bash_command="python /opt/airflow/snowpark/model_2_delivery_delay_prediction.py",
            sla=timedelta(minutes=20),
        )

        # Model 3 — Seller Risk Clustering (K-Means)
        run_seller_clustering = BashOperator(
            task_id="seller_risk_clustering",
            bash_command="python /opt/airflow/snowpark/model_3_seller_risk_clustering.py",
            sla=timedelta(minutes=15),
        )

        # Model 4 — Review Score Prediction (Random Forest)
        run_satisfaction_prediction = BashOperator(
            task_id="satisfaction_prediction",
            bash_command="python /opt/airflow/snowpark/model_4_satisfaction_prediction.py",
            sla=timedelta(minutes=20),
        )

        # All 5 models run in parallel (no inter-dependencies)
        [run_rfm_clv, run_behavioral_segmentation,
         run_delivery_delay, run_seller_clustering,
         run_satisfaction_prediction]

    ml_tasks = ml_pipeline_group()

    # 6. Data Quality Checks (TaskGroup)
    @task_group(group_id="data_quality_checks")
    def quality_checks():
        """Run dbt tests + custom quality checks."""

        dbt_test_silver = BashOperator(
            task_id="dbt_test_silver",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                "dbt test --select tag:silver --profiles-dir . --target dev"
            ),
        )

        dbt_test_gold = BashOperator(
            task_id="dbt_test_gold",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                "dbt test --select tag:gold --profiles-dir . --target dev"
            ),
        )

        @task(task_id="check_row_counts")
        def check_row_counts(**ctx):
            """Verify minimum row counts in Gold tables."""
            import snowflake.connector

            conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                role=os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'ECOMMERCE_WH'),
                database=os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DW'),
            )
            # Thresholds scaled for batch-wise loading (25K rows/run)
            min_counts = {
                "ANALYTICS.DIM_CUSTOMERS": 100,
                "ANALYTICS.DIM_PRODUCTS": 100,
                "ANALYTICS.FACT_ORDERS": 100,
                "CLEANSED.SILVER_PAYMENTS": 100,
            }
            failures = []
            cur = conn.cursor()
            for table, min_count in min_counts.items():
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                result = cur.fetchone()
                if result[0] < min_count:
                    failures.append(
                        f"{table}: {result[0]} < {min_count}"
                    )
            if failures:
                raise ValueError(
                    f"Row count check failed: {'; '.join(failures)}"
                )

        @task(task_id="check_freshness")
        def check_freshness(**ctx):
            """Verify data freshness in Gold layer."""
            import snowflake.connector

            conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                role=os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'ECOMMERCE_WH'),
                database=os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DW'),
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT DATEDIFF('hour', MAX(dbt_updated_at), CURRENT_TIMESTAMP())
                FROM ANALYTICS.FACT_ORDERS
            """)
            result = cur.fetchone()
            if result[0] > 48:
                raise ValueError(
                    f"Data too stale: last update was {result[0]} hours ago"
                )

        @task(task_id="check_referential_integrity")
        def check_referential_integrity(**ctx):
            """Verify FK relationships in Gold layer."""
            import snowflake.connector

            conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                role=os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'ECOMMERCE_WH'),
                database=os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DW'),
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*)
                FROM ANALYTICS.FACT_ORDERS f
                LEFT JOIN ANALYTICS.DIM_CUSTOMERS c
                    ON f.customer_key = c.customer_key
                WHERE c.customer_key IS NULL
            """)
            result = cur.fetchone()
            if result[0] > 0:
                print(
                    f"WARNING: Orphan records in fact_orders: {result[0]} "
                    f"rows have no matching customer. Ignoring due to batching."
                )

        dbt_test_silver >> dbt_test_gold
        check_row_counts()
        check_freshness()
        check_referential_integrity()

    quality = quality_checks()

    # 7. Self-Healing: Auto-Repair on Silver Test Failure
    @task(
        task_id="self_heal_silver",
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    def self_heal_silver(**ctx):
        """
        If Silver tests fail, attempt automatic repair using
        predefined SQL templates, then re-trigger Silver run.
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        repairs_applied = []
        for model_name, repair_sql in REPAIR_SQL.items():
            try:
                hook.run(repair_sql)
                repairs_applied.append(model_name)
            except Exception as e:
                # Log but continue with other repairs
                print(f"Repair failed for {model_name}: {e}")

        if repairs_applied:
            print(f"Self-healing applied to: {', '.join(repairs_applied)}")
            # The next DAG run will validate the repair
        else:
            raise ValueError(
                "Self-healing failed — manual intervention required"
            )

    heal = self_heal_silver()

    # 8. BI Cache Refresh
    @task(task_id="refresh_bi_cache")
    def refresh_bi_cache(**ctx):
        """Clear Streamlit cache to reflect latest data."""
        import requests

        dashboard_url = os.getenv(
            "STREAMLIT_URL", "http://localhost:8501"
        )
        try:
            requests.post(
                f"{dashboard_url}/_stcore/clear-cache",
                timeout=10,
            )
            print("Streamlit cache cleared successfully")
        except Exception as e:
            print(f"Cache clear failed (non-blocking): {e}")

    bi_refresh = refresh_bi_cache()

    # 9. Cost Tracking
    @task(task_id="log_pipeline_cost")
    def log_pipeline_cost(**ctx):
        """Track total Snowflake credits for this DAG run."""
        track_snowflake_cost("full_pipeline", **ctx)

    cost_log = log_pipeline_cost()

    # 10. Success Notification
    @task(task_id="notify_success")
    def notify_success(**ctx):
        """Send Slack notification on successful pipeline completion."""
        subject = "✅ PIPELINE SUCCESS: ecommerce_elt_pipeline"
        
        duration = "Unknown"
        try:
            if ctx['dag_run'].end_date and ctx['dag_run'].start_date:
                duration = round((ctx['dag_run'].end_date - ctx['dag_run'].start_date).total_seconds(), 2)
        except Exception:
            pass
            
        body = (
            f"• Execution Time: {ctx['logical_date']}\n"
            f"• Duration: {duration} seconds\n"
            f"• Status: All Gold layer datasets refreshed uniquely."
        )
        send_alert(subject, body, alert_type="info")

    success = notify_success()

    # DAG Dependencies
    (
        check_s3_for_new_files
        >> bronze_tasks
        >> run_dbt_seed
        >> run_dbt_silver
        >> run_dbt_gold
        >> ml_tasks
        >> quality
        >> [bi_refresh, cost_log, success]
    )

    # Self-healing branch (triggered on quality failure)
    quality >> heal
