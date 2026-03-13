"""
================================================================================
 IBRD Loan Pipeline — Bronze to Silver Transformation
================================================================================
 Module  : clean_loan_data.py
 Layer   : Bronze → Silver (Medallion Architecture)
 Purpose : Reads raw IBRD loan data from the ADLS Bronze layer, applies
           data cleansing, and writes cleaned Parquet to the Silver layer.
 Author  : Data Engineering Team
 Created : 2026-03-13
================================================================================
"""

import os, sys, re, logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
ADLS_ACCOUNT = os.environ.get("ADLS_ACCOUNT_NAME", "ibrdstorageaccount")
ADLS_CONTAINER = os.environ.get("ADLS_CONTAINER", "ibrd-datalake")
ADLS_BASE = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"

BRONZE_PATH = f"{ADLS_BASE}/3_ADLS_Data_Lake/bronze_layer/loans_raw.csv"
SILVER_PATH = f"{ADLS_BASE}/3_ADLS_Data_Lake/silver_layer/loans_clean.parquet"
LOCAL_BRONZE = "3_ADLS_Data_Lake/bronze_layer/loans_raw.csv"
LOCAL_SILVER = "3_ADLS_Data_Lake/silver_layer/loans_clean.parquet"
PARTITION_COL = "country_code"

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════
def _get_logger():
    lgr = logging.getLogger("bronze_to_silver")
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
    """Creates SparkSession with ADLS Gen2 and performance configs."""
    logger.info("Initializing SparkSession...")
    spark = (
        SparkSession.builder
        .appName("IBRD_Bronze_to_Silver")
        .master("local[*]")
        .config(f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
                os.environ.get("ADLS_ACCOUNT_KEY", ""))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORMATION FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════
def standardize_column_names(df: DataFrame) -> DataFrame:
    """Converts all column names to clean snake_case."""
    logger.info("Standardizing column names to snake_case...")
    for col_name in df.columns:
        new = re.sub(r"[^a-zA-Z0-9_]", "_", col_name.strip())
        new = re.sub(r"_+", "_", new).lower().strip("_")
        if new != col_name:
            df = df.withColumnRenamed(col_name, new)
    logger.info(f"  {len(df.columns)} columns standardized.")
    return df

def cast_numeric_columns(df: DataFrame) -> DataFrame:
    """Casts monetary and rate columns to DoubleType."""
    logger.info("Casting numeric columns to DoubleType...")
    num_cols = [
        "original_principal_amount", "cancelled_amount", "undisbursed_amount",
        "disbursed_amount", "repaid_to_ibrd", "due_to_ibrd",
        "borrowers_obligation", "sold_3rd_party", "repaid_3rd_party",
        "due_3rd_party", "loans_held", "interest_rate", "service_charge_rate"
    ]
    existing = set(df.columns)
    for c in num_cols:
        if c in existing:
            df = df.withColumn(c, F.col(c).cast(DoubleType()))
    return df

def convert_date_columns(df: DataFrame) -> DataFrame:
    """Converts date strings to DateType using multi-format coalesce."""
    logger.info("Converting date columns to DateType...")
    date_cols = [
        "board_approval_date", "effective_date_most_recent",
        "closed_date_most_recent", "last_disbursement_date",
        "first_repayment_date", "last_repayment_date",
        "agreement_signing_date", "end_of_period"
    ]
    fmts = ["yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss",
            "MM/dd/yyyy", "yyyy-MM-dd"]
    existing = set(df.columns)
    for c in date_cols:
        if c in existing:
            exprs = [F.to_date(F.col(c), f) for f in fmts]
            df = df.withColumn(c, F.coalesce(*exprs))
    return df

def handle_missing_values(df: DataFrame) -> DataFrame:
    """Fills nulls: categorical→'Unknown', numeric→0.0."""
    logger.info("Handling missing values...")
    existing = set(df.columns)
    monetary = ["original_principal_amount", "cancelled_amount", "undisbursed_amount",
                "disbursed_amount", "repaid_to_ibrd", "due_to_ibrd",
                "borrowers_obligation", "sold_3rd_party", "repaid_3rd_party",
                "due_3rd_party", "loans_held", "interest_rate", "service_charge_rate"]
    categorical = ["region", "country_code", "country", "borrower",
                   "guarantor_country_code", "guarantor", "loan_type",
                   "loan_status", "project_id", "project_name",
                   "currency_of_commitment"]
    df = df.fillna({c: 0.0 for c in monetary if c in existing})
    df = df.fillna({c: "Unknown" for c in categorical if c in existing})
    return df

def deduplicate_records(df: DataFrame) -> DataFrame:
    """Removes exact duplicates, then keeps latest per loan_number."""
    logger.info("Deduplicating records...")
    initial = df.count()
    df = df.dropDuplicates()
    logger.info(f"  Exact dupes removed: {initial - df.count():,}")

    if all(c in df.columns for c in ["loan_number", "end_of_period"]):
        w = Window.partitionBy("loan_number").orderBy(F.col("end_of_period").desc())
        df = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
        logger.info(f"  After business-key dedup: {df.count():,}")
    return df

def log_data_quality(df: DataFrame) -> None:
    """Logs null percentages and distinct counts for key columns."""
    total = df.count()
    logger.info(f"DATA QUALITY — {total:,} rows, {len(df.columns)} cols")
    for c in ["region", "country", "loan_type", "loan_status"]:
        if c in df.columns:
            logger.info(f"  Distinct '{c}': {df.select(c).distinct().count()}")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def run_bronze_to_silver(spark: SparkSession, use_local: bool = False):
    """Orchestrates the full Bronze → Silver cleansing pipeline."""
    bp = LOCAL_BRONZE if use_local else BRONZE_PATH
    sp = LOCAL_SILVER if use_local else SILVER_PATH

    logger.info("=" * 60)
    logger.info("BRONZE → SILVER PIPELINE STARTED")
    logger.info(f"  Input : {bp}")
    logger.info(f"  Output: {sp}")
    logger.info("=" * 60)

    try:
        # Step 1: Read raw CSV
        df = (spark.read.option("header", "true").option("inferSchema", "true")
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .csv(bp))
        logger.info(f"  Loaded {df.count():,} raw records.")

        # Quarantine corrupt records
        if "_corrupt_record" in df.columns:
            bad = df.where(F.col("_corrupt_record").isNotNull()).count()
            if bad > 0:
                logger.warning(f"  {bad:,} corrupt records quarantined.")
            df = df.where(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

        # Steps 2-6: Transform
        df = standardize_column_names(df)
        df = cast_numeric_columns(df)
        df = convert_date_columns(df)
        df = handle_missing_values(df)
        df = deduplicate_records(df)
        log_data_quality(df)

        # Step 7: Write Parquet
        writer = df.write.mode("overwrite").option("compression", "snappy")
        if PARTITION_COL in df.columns:
            writer.partitionBy(PARTITION_COL).parquet(sp)
        else:
            writer.parquet(sp)

        logger.info(f"✅ BRONZE → SILVER COMPLETE — {df.count():,} clean records written.")

    except AnalysisException as ae:
        logger.error(f"Spark AnalysisException: {ae}"); raise
    except Exception as exc:
        logger.critical(f"Fatal error: {exc}", exc_info=True); raise

if __name__ == "__main__":
    use_local = "--local" in sys.argv
    spark = None
    try:
        spark = create_spark_session()
        run_bronze_to_silver(spark, use_local=use_local)
    except Exception as exc:
        logger.critical(f"Pipeline failed: {exc}"); sys.exit(1)
    finally:
        if spark: spark.stop()
