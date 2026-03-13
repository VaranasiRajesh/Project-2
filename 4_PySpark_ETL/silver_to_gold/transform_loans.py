"""
================================================================================
 IBRD Loan Pipeline — Silver to Gold / Star Schema Transformation
================================================================================
 Module  : transform_loans.py
 Layer   : Silver → Gold (Medallion Architecture)
 Purpose : Reads cleaned IBRD loan Parquet from the Silver layer and creates
           a Star Schema (4 dimension tables + 1 fact table) in the Gold layer
           for optimized analytical queries and PowerBI consumption.
 Author  : Data Engineering Team
 Created : 2026-03-13
================================================================================
"""

import os, sys, logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.utils import AnalysisException

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
ADLS_ACCOUNT = os.environ.get("ADLS_ACCOUNT_NAME", "ibrdstorageaccount")
ADLS_CONTAINER = os.environ.get("ADLS_CONTAINER", "ibrd-datalake")
ADLS_BASE = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"

SILVER_PATH = f"{ADLS_BASE}/3_ADLS_Data_Lake/silver_layer/loans_clean.parquet"
GOLD_BASE = f"{ADLS_BASE}/3_ADLS_Data_Lake/gold_layer"
LOCAL_SILVER = "3_ADLS_Data_Lake/silver_layer/loans_clean.parquet"
LOCAL_GOLD = "3_ADLS_Data_Lake/gold_layer"

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════
def _get_logger():
    lgr = logging.getLogger("silver_to_gold")
    lgr.setLevel(logging.INFO)
    if not lgr.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s │ %(levelname)-8s │ %(message)s"))
        lgr.addHandler(h)
    return lgr

logger = _get_logger()

# ══════════════════════════════════════════════════════════════════════════════
# SPARK SESSION
# ══════════════════════════════════════════════════════════════════════════════
def create_spark_session() -> SparkSession:
    """Creates a SparkSession optimized for Gold layer transformations."""
    logger.info("Initializing SparkSession for Silver → Gold...")
    spark = (
        SparkSession.builder
        .appName("IBRD_Silver_to_Gold_StarSchema")
        .master("local[*]")
        .config(f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
                os.environ.get("ADLS_ACCOUNT_KEY", ""))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ══════════════════════════════════════════════════════════════════════════════
# SURROGATE KEY GENERATOR
# ══════════════════════════════════════════════════════════════════════════════
def add_surrogate_key(df: DataFrame, key_name: str) -> DataFrame:
    """
    Adds a monotonically increasing surrogate key column to a DataFrame.
    Uses monotonically_increasing_id() which is guaranteed unique but not
    necessarily consecutive — suitable for distributed environments.
    The key is cast to LongType for Snowflake compatibility.
    """
    return df.withColumn(key_name, F.monotonically_increasing_id().cast(LongType()))

# ══════════════════════════════════════════════════════════════════════════════
# DIMENSION TABLE BUILDERS
# ══════════════════════════════════════════════════════════════════════════════
def build_dim_country(df: DataFrame) -> DataFrame:
    """
    Creates the Country dimension table.

    Columns:
      - country_key      : Surrogate key (auto-generated)
      - country_code     : ISO country code (natural key)
      - country          : Full country name
      - region           : Geographic region (e.g., Latin America, South Asia)
      - guarantor_country_code : Guarantor country ISO code
      - guarantor        : Guarantor country name
    """
    logger.info("Building dim_country...")

    # Select distinct country-level attributes
    dim_country = (
        df.select(
            "country_code", "country", "region",
            "guarantor_country_code", "guarantor"
        )
        .dropDuplicates(["country_code"])          # One row per country
        .fillna({"country": "Unknown", "region": "Unknown",
                 "guarantor_country_code": "N/A", "guarantor": "N/A"})
        .orderBy("country_code")
    )

    # Add surrogate key
    dim_country = add_surrogate_key(dim_country, "country_key")

    # Reorder columns: key first
    dim_country = dim_country.select(
        "country_key", "country_code", "country", "region",
        "guarantor_country_code", "guarantor"
    )

    logger.info(f"  dim_country: {dim_country.count():,} rows")
    return dim_country


def build_dim_project(df: DataFrame) -> DataFrame:
    """
    Creates the Project dimension table.

    Columns:
      - project_key  : Surrogate key
      - project_id   : World Bank project ID (natural key)
      - project_name : Human-readable project name
    """
    logger.info("Building dim_project...")

    dim_project = (
        df.select("project_id", "project_name")
        .dropDuplicates(["project_id"])
        .fillna({"project_name": "Unknown Project"})
        .orderBy("project_id")
    )

    dim_project = add_surrogate_key(dim_project, "project_key")
    dim_project = dim_project.select("project_key", "project_id", "project_name")

    logger.info(f"  dim_project: {dim_project.count():,} rows")
    return dim_project


def build_dim_loan_type(df: DataFrame) -> DataFrame:
    """
    Creates the Loan Type dimension table.

    Columns:
      - loan_type_key          : Surrogate key
      - loan_type              : Type code (e.g., IBRD, IDA)
      - loan_status            : Current status (Approved, Disbursing, Repaying, etc.)
      - currency_of_commitment : Currency code (usually USD)
    """
    logger.info("Building dim_loan_type...")

    dim_loan_type = (
        df.select("loan_type", "loan_status", "currency_of_commitment")
        .dropDuplicates(["loan_type", "loan_status", "currency_of_commitment"])
        .fillna({
            "loan_type": "Unknown",
            "loan_status": "Unknown",
            "currency_of_commitment": "USD"
        })
        .orderBy("loan_type", "loan_status")
    )

    dim_loan_type = add_surrogate_key(dim_loan_type, "loan_type_key")
    dim_loan_type = dim_loan_type.select(
        "loan_type_key", "loan_type", "loan_status", "currency_of_commitment"
    )

    logger.info(f"  dim_loan_type: {dim_loan_type.count():,} rows")
    return dim_loan_type


def build_dim_borrower(df: DataFrame) -> DataFrame:
    """
    Creates the Borrower dimension table.

    Columns:
      - borrower_key : Surrogate key
      - borrower     : Borrowing entity name
      - country_code : Country of the borrower (for join reference)
    """
    logger.info("Building dim_borrower...")

    dim_borrower = (
        df.select("borrower", "country_code")
        .dropDuplicates(["borrower"])
        .fillna({"borrower": "Unknown Borrower", "country_code": "N/A"})
        .orderBy("borrower")
    )

    dim_borrower = add_surrogate_key(dim_borrower, "borrower_key")
    dim_borrower = dim_borrower.select("borrower_key", "borrower", "country_code")

    logger.info(f"  dim_borrower: {dim_borrower.count():,} rows")
    return dim_borrower


# ══════════════════════════════════════════════════════════════════════════════
# FACT TABLE BUILDER
# ══════════════════════════════════════════════════════════════════════════════
def build_fact_loans(
    df: DataFrame,
    dim_country: DataFrame,
    dim_project: DataFrame,
    dim_loan_type: DataFrame,
    dim_borrower: DataFrame
) -> DataFrame:
    """
    Creates the central Fact table containing measurable loan metrics
    and foreign keys referencing each dimension.

    Fact Measures:
      - original_principal_amount : Original loan amount (USD)
      - cancelled_amount          : Amount cancelled (USD)
      - undisbursed_amount        : Amount not yet disbursed (USD)
      - disbursed_amount          : Amount disbursed (USD)
      - repaid_to_ibrd            : Amount repaid to IBRD (USD)
      - due_to_ibrd               : Amount due to IBRD (USD)
      - borrowers_obligation      : Outstanding borrower obligation (USD)
      - interest_rate             : Loan interest rate (%)
      - service_charge_rate       : Service charge rate (%)

    Foreign Keys:
      - country_key   → dim_country
      - project_key   → dim_project
      - loan_type_key → dim_loan_type
      - borrower_key  → dim_borrower

    Degenerate Dimensions (kept in fact for drill-through):
      - loan_number, end_of_period
    """
    logger.info("Building fact_loans...")

    # ── Join dimensions to get surrogate keys ────────────────────────────
    # Broadcast smaller dimension tables for optimized joins
    fact = (
        df
        # Join dim_country on country_code
        .join(
            F.broadcast(dim_country.select("country_key", "country_code")),
            on="country_code", how="left"
        )
        # Join dim_project on project_id
        .join(
            F.broadcast(dim_project.select("project_key", "project_id")),
            on="project_id", how="left"
        )
        # Join dim_loan_type on composite key
        .join(
            F.broadcast(dim_loan_type.select(
                "loan_type_key", "loan_type", "loan_status", "currency_of_commitment"
            )),
            on=["loan_type", "loan_status", "currency_of_commitment"], how="left"
        )
        # Join dim_borrower on borrower name
        .join(
            F.broadcast(dim_borrower.select("borrower_key", "borrower")),
            on="borrower", how="left"
        )
    )

    # Add a fact surrogate key
    fact = add_surrogate_key(fact, "loan_fact_key")

    # ── Select final fact table columns ──────────────────────────────────
    fact_columns = [
        "loan_fact_key",
        # Foreign keys
        "country_key", "project_key", "loan_type_key", "borrower_key",
        # Degenerate dimensions
        "loan_number", "end_of_period",
        # Date keys (for time-based analysis)
        "board_approval_date", "effective_date_most_recent",
        "closed_date_most_recent", "agreement_signing_date",
        "first_repayment_date", "last_repayment_date",
        "last_disbursement_date",
        # Measures
        "original_principal_amount", "cancelled_amount",
        "undisbursed_amount", "disbursed_amount",
        "repaid_to_ibrd", "due_to_ibrd",
        "borrowers_obligation", "sold_3rd_party",
        "repaid_3rd_party", "due_3rd_party",
        "loans_held", "interest_rate", "service_charge_rate"
    ]

    # Only select columns that actually exist (graceful handling)
    available = set(fact.columns)
    selected = [c for c in fact_columns if c in available]
    fact = fact.select(*selected)

    logger.info(f"  fact_loans: {fact.count():,} rows, {len(selected)} columns")
    return fact


# ══════════════════════════════════════════════════════════════════════════════
# WRITER — GOLD LAYER PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════
def write_to_gold(df: DataFrame, table_name: str, gold_base: str) -> None:
    """
    Writes a dataframe to the Gold layer as optimized Parquet.
    Uses Snappy compression and coalesces small dimension tables.
    """
    output_path = f"{gold_base}/{table_name}"
    row_count = df.count()

    # Coalesce small dimension tables to avoid too many small files
    if row_count < 100_000:
        df = df.coalesce(1)

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )

    logger.info(f"  ✓ {table_name} → {output_path} ({row_count:,} rows)")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def run_silver_to_gold(spark: SparkSession, use_local: bool = False):
    """
    Orchestrates the full Silver → Gold Star Schema transformation.

    Steps:
      1. Read cleaned Parquet from Silver layer
      2. Build 4 dimension tables (country, project, loan_type, borrower)
      3. Build fact_loans with FK lookups via broadcast joins
      4. Write all tables to Gold layer as optimized Parquet
    """
    silver = LOCAL_SILVER if use_local else SILVER_PATH
    gold = LOCAL_GOLD if use_local else GOLD_BASE

    logger.info("=" * 60)
    logger.info("SILVER → GOLD STAR SCHEMA PIPELINE STARTED")
    logger.info(f"  Input  (Silver): {silver}")
    logger.info(f"  Output (Gold)  : {gold}")
    logger.info("=" * 60)

    try:
        # Step 1: Read Silver layer
        logger.info("[1/6] Reading Silver layer Parquet...")
        df = spark.read.parquet(silver)
        logger.info(f"  Loaded {df.count():,} records from Silver layer.")

        # Step 2: Build dimensions
        logger.info("[2/6] Building dimension tables...")
        dim_country = build_dim_country(df)
        dim_project = build_dim_project(df)
        dim_loan_type = build_dim_loan_type(df)
        dim_borrower = build_dim_borrower(df)

        # Step 3: Build fact table
        logger.info("[3/6] Building fact_loans table...")
        fact_loans = build_fact_loans(
            df, dim_country, dim_project, dim_loan_type, dim_borrower
        )

        # Steps 4-6: Write to Gold layer
        logger.info("[4/6] Writing dimension tables to Gold layer...")
        write_to_gold(dim_country, "dim_country", gold)
        write_to_gold(dim_project, "dim_project", gold)
        write_to_gold(dim_loan_type, "dim_loan_type", gold)
        write_to_gold(dim_borrower, "dim_borrower", gold)

        logger.info("[5/6] Writing fact table to Gold layer...")
        write_to_gold(fact_loans, "fact_loans", gold)

        # Summary
        logger.info("[6/6] Star Schema Summary:")
        logger.info(f"  dim_country   : {dim_country.count():,} rows")
        logger.info(f"  dim_project   : {dim_project.count():,} rows")
        logger.info(f"  dim_loan_type : {dim_loan_type.count():,} rows")
        logger.info(f"  dim_borrower  : {dim_borrower.count():,} rows")
        logger.info(f"  fact_loans    : {fact_loans.count():,} rows")
        logger.info("=" * 60)
        logger.info("✅ SILVER → GOLD STAR SCHEMA COMPLETE")
        logger.info("=" * 60)

    except AnalysisException as ae:
        logger.error(f"Spark AnalysisException: {ae}"); raise
    except Exception as exc:
        logger.critical(f"Fatal error: {exc}", exc_info=True); raise


if __name__ == "__main__":
    use_local = "--local" in sys.argv
    spark = None
    try:
        spark = create_spark_session()
        run_silver_to_gold(spark, use_local=use_local)
    except Exception as exc:
        logger.critical(f"Pipeline failed: {exc}"); sys.exit(1)
    finally:
        if spark: spark.stop()
