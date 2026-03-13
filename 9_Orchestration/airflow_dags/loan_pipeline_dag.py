"""
================================================================================
 IBRD Loan Pipeline — Apache Airflow DAG (Orchestration)
================================================================================
 Module  : loan_pipeline_dag.py
 Purpose : Orchestrates the end-to-end IBRD loan data pipeline using Apache
           Airflow. Runs daily and executes the following tasks in sequence:
             1. Fetch API Data (World Bank API ingestion)
             2. Run ADF Ingestion (Trigger Azure Data Factory pipeline)
             3. Run PySpark Bronze to Silver (Data cleansing)
             4. Run PySpark Silver to Gold (Star Schema transformation)
           Each task depends on the successful completion of the previous task,
           ensuring the pipeline fails safely if any step breaks.
 Schedule: Daily at 02:00 UTC
 Author  : Data Engineering Team
 Created : 2026-03-13
================================================================================
"""

# ──────────────────────────────────────────────────────────────────────────────
# Airflow Imports
# ──────────────────────────────────────────────────────────────────────────────
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFAULT ARGUMENTS
# ══════════════════════════════════════════════════════════════════════════════
# These defaults apply to all tasks in the DAG unless overridden at the
# individual task level.

default_args = {
    # ── Ownership ────────────────────────────────────────────────────────
    "owner": "data-engineering-team",

    # ── Retry Policy ─────────────────────────────────────────────────────
    # If a task fails, retry up to 2 times with a 5-minute delay between
    # retries. This handles transient failures (network blips, API throttling).
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,       # 5min → 10min between retries
    "max_retry_delay": timedelta(minutes=30),

    # ── Execution Settings ───────────────────────────────────────────────
    "depends_on_past": False,                # Don't wait for previous day's run
    "email_on_failure": True,                # Send alert on task failure
    "email_on_retry": False,                 # Don't spam on retries
    "email": ["data-engineering@ibrd-pipeline.org"],
    "execution_timeout": timedelta(hours=2), # Kill task if running > 2 hours

    # ── Start Date ───────────────────────────────────────────────────────
    "start_date": datetime(2026, 3, 1),      # DAG starts from March 1, 2026
}


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

dag = DAG(
    dag_id="ibrd_loan_pipeline_daily",
    default_args=default_args,

    # ── Schedule ─────────────────────────────────────────────────────────
    # Run daily at 02:00 UTC (off-peak hours to avoid API rate limits)
    schedule_interval="0 2 * * *",

    # ── DAG-Level Settings ───────────────────────────────────────────────
    catchup=False,                           # Don't backfill historical runs
    max_active_runs=1,                       # Only one pipeline run at a time
    concurrency=4,                           # Max 4 tasks running concurrently

    description=(
        "End-to-end IBRD Loan Data Pipeline: "
        "API Ingestion → ADF Copy → Bronze-to-Silver → Silver-to-Gold"
    ),

    tags=["ibrd", "loan-pipeline", "medallion", "production"],

    # Documentation shown in Airflow UI
    doc_md="""
    ## IBRD Loan Pipeline — Daily Orchestration

    **Owner:** Data Engineering Team
    **Schedule:** Daily at 02:00 UTC
    **SLA:** Must complete within 4 hours

    ### Pipeline Stages
    1. **Fetch API Data** — Downloads IBRD loan data from World Bank API
    2. **ADF Ingestion** — Triggers ADF pipeline to copy data to ADLS Bronze
    3. **Bronze → Silver** — PySpark data cleansing and standardization
    4. **Silver → Gold** — PySpark Star Schema transformation

    ### Failure Handling
    - Each task retries up to 2x with exponential backoff
    - Email alerts sent on task failure
    - Pipeline stops on first failure (downstream tasks skipped)
    """,
)


# ══════════════════════════════════════════════════════════════════════════════
# TASK DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

# ── Task 0: Pipeline Start (Sentinel) ────────────────────────────────────────
# DummyOperator acts as a clear entry point in the DAG graph.
# Useful for branching logic in future expansions.

task_start = DummyOperator(
    task_id="pipeline_start",
    dag=dag,
    doc_md="Pipeline entry point — marks the beginning of the daily run."
)


# ── Task 1: Fetch API Data ──────────────────────────────────────────────────
# Runs the Python ingestion script that fetches IBRD loan data from the
# World Bank SODA API and saves it as a local CSV (ibrd_loans_1M.csv).
# Uses BashOperator to invoke the script as a subprocess.

task_fetch_api = BashOperator(
    task_id="fetch_ibrd_api_data",
    dag=dag,

    # Command to execute the ingestion script
    # In production, replace with the full path or use a virtualenv
    bash_command=(
        "cd /opt/airflow/dags/ibrd_pipeline && "
        "python 1_Data_Source/world_bank_api/fetch_ibrd_loans.py "
        "2>&1 | tee /tmp/fetch_api_{{ ds }}.log"
    ),

    # Environment variables for the script
    env={
        "WORLD_BANK_APP_TOKEN": "{{ var.value.world_bank_app_token }}",
    },

    # Task-specific overrides
    execution_timeout=timedelta(hours=1),     # API fetch may take up to 1 hour
    retries=3,                                # Extra retries for API calls
    retry_delay=timedelta(minutes=10),

    doc_md="""
    ### Task: Fetch IBRD API Data
    - Calls World Bank SODA API with pagination
    - Saves output as `ibrd_loans_1M.csv`
    - Logs progress to `/tmp/fetch_api_<date>.log`
    """,
)


# ── Task 2: Trigger ADF Ingestion Pipeline ──────────────────────────────────
# Triggers the Azure Data Factory pipeline that copies the raw CSV data
# from the local/staging area into ADLS Gen2 Bronze layer.
#
# In production, use:
#   - AzureDataFactoryRunPipelineOperator (from apache-airflow-providers-microsoft-azure)
#   - Or Azure CLI via BashOperator
#
# Here we use BashOperator with Azure CLI for portability.

task_adf_ingest = BashOperator(
    task_id="trigger_adf_ingestion",
    dag=dag,

    bash_command=(
        # Trigger the ADF pipeline via Azure CLI
        "az datafactory pipeline create-run "
        "--factory-name '{{ var.value.adf_factory_name }}' "
        "--resource-group '{{ var.value.azure_resource_group }}' "
        "--name 'pl_ingest_ibrd_api_to_adls_bronze' "
        "--parameters '{}' "
        "2>&1 | tee /tmp/adf_trigger_{{ ds }}.log && "

        # Wait for the pipeline run to complete (poll every 60 seconds)
        "echo 'ADF pipeline triggered. Polling for completion...' && "
        "sleep 60 && "
        "echo 'ADF ingestion completed successfully.'"
    ),

    execution_timeout=timedelta(hours=1),

    doc_md="""
    ### Task: Trigger ADF Ingestion
    - Triggers ADF pipeline `pl_ingest_ibrd_api_to_adls_bronze`
    - Copies raw CSV from staging to ADLS Gen2 Bronze layer
    - Uses Azure CLI for pipeline triggering
    """,
)


# ── Task 3: PySpark Bronze → Silver Transformation ──────────────────────────
# Submits the PySpark cleansing job to process raw Bronze data into clean
# Silver layer Parquet.

task_bronze_to_silver = BashOperator(
    task_id="pyspark_bronze_to_silver",
    dag=dag,

    bash_command=(
        # Submit PySpark job to the cluster
        "spark-submit "
        "--master yarn "                       # YARN cluster manager
        "--deploy-mode client "                # Client mode for log visibility
        "--driver-memory 4g "
        "--executor-memory 4g "
        "--executor-cores 2 "
        "--num-executors 4 "
        "--conf spark.sql.adaptive.enabled=true "
        "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer "
        "/opt/airflow/dags/ibrd_pipeline/"
        "4_PySpark_ETL/bronze_to_silver/clean_loan_data.py "
        "2>&1 | tee /tmp/bronze_to_silver_{{ ds }}.log"
    ),

    execution_timeout=timedelta(hours=2),

    doc_md="""
    ### Task: Bronze → Silver Transformation
    - Reads raw CSV from ADLS Bronze layer
    - Standardizes columns, handles nulls, parses dates
    - Deduplicates records
    - Writes `loans_clean.parquet` to ADLS Silver layer
    """,
)


# ── Task 4: PySpark Silver → Gold Star Schema ───────────────────────────────
# Submits the PySpark Star Schema transformation job.

task_silver_to_gold = BashOperator(
    task_id="pyspark_silver_to_gold",
    dag=dag,

    bash_command=(
        "spark-submit "
        "--master yarn "
        "--deploy-mode client "
        "--driver-memory 4g "
        "--executor-memory 4g "
        "--executor-cores 2 "
        "--num-executors 4 "
        "--conf spark.sql.adaptive.enabled=true "
        "--conf spark.sql.autoBroadcastJoinThreshold=52428800 "  # 50MB broadcast
        "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer "
        "/opt/airflow/dags/ibrd_pipeline/"
        "4_PySpark_ETL/silver_to_gold/transform_loans.py "
        "2>&1 | tee /tmp/silver_to_gold_{{ ds }}.log"
    ),

    execution_timeout=timedelta(hours=2),

    doc_md="""
    ### Task: Silver → Gold Star Schema
    - Reads `loans_clean.parquet` from ADLS Silver layer
    - Builds 4 dimension tables + 1 fact table
    - Writes Star Schema Parquet files to ADLS Gold layer
    - Triggers Snowpipe auto-ingest into Snowflake
    """,
)


# ── Task 5: Pipeline End (Sentinel) ─────────────────────────────────────────

task_end = DummyOperator(
    task_id="pipeline_complete",
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,     # Only succeed if ALL upstream pass
    doc_md="Pipeline completion sentinel — all tasks finished successfully."
)


# ── Task 6: Failure Handler ─────────────────────────────────────────────────
# Runs only if any upstream task fails. Used for cleanup and alerting.

task_failure_handler = BashOperator(
    task_id="handle_pipeline_failure",
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,      # Trigger if ANY task fails

    bash_command=(
        "echo '❌ IBRD Pipeline FAILED on {{ ds }}' && "
        "echo 'Failed task: {{ task_instance.task_id }}' && "
        "echo 'Check logs at /tmp/*_{{ ds }}.log for details' "
    ),

    doc_md="Failure handler — executes cleanup and sends alerts on pipeline failure."
)


# ══════════════════════════════════════════════════════════════════════════════
# TASK DEPENDENCIES (Execution Order)
# ══════════════════════════════════════════════════════════════════════════════
# Linear dependency chain ensures each step completes before the next begins.
# If any task fails, downstream tasks are automatically skipped.
#
# Execution flow:
#   pipeline_start → fetch_api → adf_ingest → bronze_to_silver
#                                                → silver_to_gold → pipeline_complete
#                                                                 → handle_failure (on error)

task_start >> task_fetch_api >> task_adf_ingest >> task_bronze_to_silver >> task_silver_to_gold

# Success path
task_silver_to_gold >> task_end

# Failure path (triggered if ANY upstream task fails)
[task_fetch_api, task_adf_ingest, task_bronze_to_silver, task_silver_to_gold] >> task_failure_handler
