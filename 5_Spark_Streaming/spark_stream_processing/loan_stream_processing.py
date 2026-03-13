"""
================================================================================
 IBRD Loan Pipeline — Spark Structured Streaming Processor
================================================================================
 Module  : loan_stream_processing.py
 Layer   : Real-Time / Streaming
 Purpose : Monitors an input directory for incoming CSV files containing
           IBRD loan data, applies a strict schema, filters for high-risk
           cancellations (> $1,000,000), and outputs flagged records to
           the console for real-time monitoring. Checkpointed for exactly-
           once processing guarantees.
 Input   : 5_Spark_Streaming/streaming_input/  (CSV files landing here)
 Output  : Console sink + checkpoint directory
 Author  : Data Engineering Team
 Created : 2026-03-13
================================================================================
"""

import os, sys, logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType
)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
# Streaming input directory — new CSV files landing here trigger processing
STREAMING_INPUT_DIR = "5_Spark_Streaming/streaming_input/"

# Checkpoint directory — ensures exactly-once processing after restarts
CHECKPOINT_DIR = "5_Spark_Streaming/checkpoints/streaming_checkpoint/"

# High-risk cancellation threshold (USD)
HIGH_RISK_THRESHOLD = 1_000_000.0

# Trigger interval for micro-batches (e.g., "30 seconds", "1 minute")
TRIGGER_INTERVAL = "30 seconds"

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════
def _get_logger():
    lgr = logging.getLogger("loan_stream")
    lgr.setLevel(logging.INFO)
    if not lgr.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(
            "%(asctime)s │ %(levelname)-8s │ %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        lgr.addHandler(h)
    return lgr

logger = _get_logger()

# ══════════════════════════════════════════════════════════════════════════════
# STRICT SCHEMA DEFINITION
# ══════════════════════════════════════════════════════════════════════════════
# Define the expected schema for incoming IBRD loan CSV files.
# Using explicit schema (vs inferSchema) prevents:
#   - Schema drift between batches
#   - Type mismatches causing silent data loss
#   - Performance overhead of schema inference in streaming

LOAN_SCHEMA = StructType([
    # ── Identifiers ──────────────────────────────────────────────────────
    StructField("loan_number",             StringType(),  nullable=True),
    StructField("loan_type",               StringType(),  nullable=True),
    StructField("loan_status",             StringType(),  nullable=True),

    # ── Geography ────────────────────────────────────────────────────────
    StructField("region",                  StringType(),  nullable=True),
    StructField("country_code",            StringType(),  nullable=True),
    StructField("country",                 StringType(),  nullable=True),
    StructField("borrower",                StringType(),  nullable=True),
    StructField("guarantor_country_code",  StringType(),  nullable=True),
    StructField("guarantor",               StringType(),  nullable=True),

    # ── Project Information ──────────────────────────────────────────────
    StructField("project_id",              StringType(),  nullable=True),
    StructField("project_name",            StringType(),  nullable=True),

    # ── Financial Metrics (USD) ──────────────────────────────────────────
    StructField("original_principal_amount", DoubleType(), nullable=True),
    StructField("cancelled_amount",          DoubleType(), nullable=True),
    StructField("undisbursed_amount",        DoubleType(), nullable=True),
    StructField("disbursed_amount",          DoubleType(), nullable=True),
    StructField("repaid_to_ibrd",            DoubleType(), nullable=True),
    StructField("due_to_ibrd",               DoubleType(), nullable=True),
    StructField("borrowers_obligation",      DoubleType(), nullable=True),

    # ── Rates ────────────────────────────────────────────────────────────
    StructField("interest_rate",           DoubleType(),  nullable=True),
    StructField("service_charge_rate",     DoubleType(),  nullable=True),

    # ── Dates (read as strings, parsed downstream) ───────────────────────
    StructField("board_approval_date",     StringType(),  nullable=True),
    StructField("effective_date_most_recent", StringType(), nullable=True),
    StructField("closed_date_most_recent", StringType(),  nullable=True),
    StructField("agreement_signing_date",  StringType(),  nullable=True),
    StructField("first_repayment_date",    StringType(),  nullable=True),
    StructField("last_repayment_date",     StringType(),  nullable=True),
    StructField("last_disbursement_date",  StringType(),  nullable=True),

    # ── Currency ─────────────────────────────────────────────────────────
    StructField("currency_of_commitment",  StringType(),  nullable=True),
    StructField("end_of_period",           StringType(),  nullable=True),
])

# ══════════════════════════════════════════════════════════════════════════════
# SPARK SESSION
# ══════════════════════════════════════════════════════════════════════════════
def create_spark_session() -> SparkSession:
    """Creates a SparkSession configured for Structured Streaming."""
    logger.info("Initializing SparkSession for Structured Streaming...")
    spark = (
        SparkSession.builder
        .appName("IBRD_Loan_Stream_Processing")
        .master("local[*]")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialized.")
    return spark

# ══════════════════════════════════════════════════════════════════════════════
# STREAMING PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def run_streaming_pipeline(spark: SparkSession):
    """
    Runs the Structured Streaming pipeline:
      1. Reads CSV files from the streaming input directory
      2. Applies strict schema enforcement
      3. Filters high-risk cancellations (> $1M)
      4. Enriches with risk flags and timestamps
      5. Outputs to console sink with checkpointing
    """
    logger.info("=" * 60)
    logger.info("SPARK STRUCTURED STREAMING PIPELINE STARTED")
    logger.info(f"  Input Dir   : {STREAMING_INPUT_DIR}")
    logger.info(f"  Checkpoint  : {CHECKPOINT_DIR}")
    logger.info(f"  Threshold   : ${HIGH_RISK_THRESHOLD:,.0f}")
    logger.info(f"  Trigger     : {TRIGGER_INTERVAL}")
    logger.info("=" * 60)

    # Ensure input directory exists (for local development)
    os.makedirs(STREAMING_INPUT_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    try:
        # ── Step 1: Read streaming source ────────────────────────────────
        # readStream monitors the directory for new CSV files
        logger.info("[1/4] Setting up streaming source...")
        raw_stream = (
            spark.readStream
            .format("csv")
            .schema(LOAN_SCHEMA)                # Strict schema enforcement
            .option("header", "true")           # First row is header
            .option("maxFilesPerTrigger", 5)    # Process max 5 files per batch
            .option("mode", "PERMISSIVE")       # Don't fail on bad records
            .option("cleanSource", "archive")   # Archive processed files (optional)
            .load(STREAMING_INPUT_DIR)
        )
        logger.info("  Streaming source configured.")

        # ── Step 2: Add processing metadata ──────────────────────────────
        logger.info("[2/4] Adding processing metadata...")
        enriched_stream = (
            raw_stream
            # Add ingestion timestamp for audit trail
            .withColumn("ingestion_timestamp", F.current_timestamp())
            # Add source file name for lineage tracking
            .withColumn("source_file", F.input_file_name())
        )

        # ── Step 3: Filter high-risk cancellations ───────────────────────
        logger.info("[3/4] Applying high-risk cancellation filter...")
        high_risk_stream = (
            enriched_stream
            .where(
                # Primary filter: cancelled amount exceeds threshold
                F.col("cancelled_amount") > HIGH_RISK_THRESHOLD
            )
            .withColumn(
                # Add risk severity categorization
                "risk_level",
                F.when(F.col("cancelled_amount") > 10_000_000, "CRITICAL")
                 .when(F.col("cancelled_amount") > 5_000_000, "HIGH")
                 .otherwise("ELEVATED")
            )
            .withColumn(
                # Calculate cancellation ratio for context
                "cancellation_ratio",
                F.when(
                    F.col("original_principal_amount") > 0,
                    F.round(
                        F.col("cancelled_amount") / F.col("original_principal_amount") * 100,
                        2
                    )
                ).otherwise(0.0)
            )
            .select(
                "loan_number", "country", "borrower", "project_name",
                "original_principal_amount", "cancelled_amount",
                "cancellation_ratio", "risk_level", "loan_status",
                "board_approval_date", "ingestion_timestamp", "source_file"
            )
        )

        # ── Step 4: Write to console sink ────────────────────────────────
        logger.info("[4/4] Starting console output stream...")
        query = (
            high_risk_stream.writeStream
            .outputMode("append")               # Append new records only
            .format("console")                  # Console sink for monitoring
            .option("truncate", "false")        # Show full column values
            .option("numRows", 50)              # Display up to 50 rows per batch
            .option(
                "checkpointLocation",
                CHECKPOINT_DIR                  # Fault-tolerant checkpointing
            )
            .trigger(processingTime=TRIGGER_INTERVAL)
            .queryName("ibrd_high_risk_cancellations")
            .start()
        )

        logger.info("🟢 Streaming query started. Waiting for data...")
        logger.info(f"   Query Name   : {query.name}")
        logger.info(f"   Query ID     : {query.id}")
        logger.info("   Press Ctrl+C to stop the streaming query.")

        # Block until query is terminated (manually or by error)
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Streaming query stopped by user (Ctrl+C).")

    except Exception as exc:
        logger.critical(f"Streaming pipeline error: {exc}", exc_info=True)
        raise

    finally:
        logger.info("Streaming pipeline shut down.")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    spark = None
    try:
        spark = create_spark_session()
        run_streaming_pipeline(spark)
    except Exception as exc:
        logger.critical(f"Fatal error: {exc}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
