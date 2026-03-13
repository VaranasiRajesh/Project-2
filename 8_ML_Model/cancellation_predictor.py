"""
================================================================================
 IBRD Loan Pipeline — Loan Cancellation Prediction Model
================================================================================
 Module  : cancellation_predictor.py
 Purpose : Builds a binary classification model using PySpark MLlib to predict
           whether an IBRD loan is likely to experience a significant
           cancellation (cancelled_amount > 10% of original_principal_amount).
           Uses the Gold layer Star Schema data for feature engineering.
 Stack   : PySpark MLlib (RandomForest, GBT), Pipeline API
 Input   : Gold layer fact_loans + dimension tables (Parquet)
 Output  : Trained model, evaluation metrics, feature importances
 Author  : Data Engineering Team
 Created : 2026-03-13
================================================================================
"""

import os, sys, logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler,
    StandardScaler, Imputer
)
from pyspark.ml.classification import (
    RandomForestClassifier,
    GBTClassifier,
    LogisticRegression
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
ADLS_ACCOUNT = os.environ.get("ADLS_ACCOUNT_NAME", "ibrdstorageaccount")
ADLS_CONTAINER = os.environ.get("ADLS_CONTAINER", "ibrd-datalake")
ADLS_BASE = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"

GOLD_BASE = f"{ADLS_BASE}/3_ADLS_Data_Lake/gold_layer"
LOCAL_GOLD = "3_ADLS_Data_Lake/gold_layer"
MODEL_OUTPUT = "8_ML_Model/trained_model"

# Cancellation threshold: loans where cancelled > 10% of principal
CANCELLATION_THRESHOLD = 0.10

# Train/Test split ratio
TRAIN_RATIO = 0.8
TEST_RATIO = 0.2
RANDOM_SEED = 42

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════
def _get_logger():
    lgr = logging.getLogger("cancellation_predictor")
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
# SPARK SESSION
# ══════════════════════════════════════════════════════════════════════════════
def create_spark_session() -> SparkSession:
    """Creates SparkSession configured for MLlib workloads."""
    logger.info("Initializing SparkSession for ML pipeline...")
    spark = (
        SparkSession.builder
        .appName("IBRD_Cancellation_Predictor")
        .master("local[*]")
        .config(f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
                os.environ.get("ADLS_ACCOUNT_KEY", ""))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING & FEATURE ENGINEERING
# ══════════════════════════════════════════════════════════════════════════════
def load_and_prepare_data(spark: SparkSession, use_local: bool = False) -> DataFrame:
    """
    Loads Gold layer data, joins dimensions, and engineers features.

    Features engineered:
      1. cancellation_ratio       : cancelled / original_principal
      2. disbursement_ratio       : disbursed / original_principal
      3. repayment_ratio          : repaid / original_principal
      4. processing_days          : board_approval → agreement_signing
      5. loan_age_days            : board_approval → today
      6. has_third_party_sales    : binary flag
      7. region (categorical)     : encoded via StringIndexer + OneHotEncoder
      8. loan_type (categorical)  : encoded
      9. loan_status (categorical): encoded

    Target variable:
      is_high_cancellation : 1 if cancellation_ratio > CANCELLATION_THRESHOLD, else 0
    """
    gold = LOCAL_GOLD if use_local else GOLD_BASE
    logger.info("Loading Gold layer data...")

    # Load tables
    fact = spark.read.parquet(f"{gold}/fact_loans")
    dim_country = spark.read.parquet(f"{gold}/dim_country")
    dim_loan_type = spark.read.parquet(f"{gold}/dim_loan_type")

    logger.info(f"  fact_loans   : {fact.count():,} rows")
    logger.info(f"  dim_country  : {dim_country.count():,} rows")
    logger.info(f"  dim_loan_type: {dim_loan_type.count():,} rows")

    # Join dimension attributes onto fact table
    df = (
        fact
        .join(F.broadcast(dim_country.select(
            "country_key", "region"
        )), on="country_key", how="left")
        .join(F.broadcast(dim_loan_type.select(
            "loan_type_key", "loan_type", "loan_status"
        )), on="loan_type_key", how="left")
    )

    # ── Feature Engineering ──────────────────────────────────────────────
    logger.info("Engineering features...")

    df = (
        df
        # Cancellation ratio (target basis)
        .withColumn("cancellation_ratio",
            F.when(F.col("original_principal_amount") > 0,
                   F.col("cancelled_amount") / F.col("original_principal_amount"))
            .otherwise(0.0))

        # Disbursement ratio
        .withColumn("disbursement_ratio",
            F.when(F.col("original_principal_amount") > 0,
                   F.col("disbursed_amount") / F.col("original_principal_amount"))
            .otherwise(0.0))

        # Repayment ratio
        .withColumn("repayment_ratio",
            F.when(F.col("original_principal_amount") > 0,
                   F.col("repaid_to_ibrd") / F.col("original_principal_amount"))
            .otherwise(0.0))

        # Processing days (board approval → signing)
        .withColumn("processing_days",
            F.when(
                F.col("board_approval_date").isNotNull() &
                F.col("agreement_signing_date").isNotNull(),
                F.datediff(F.col("agreement_signing_date"),
                          F.col("board_approval_date"))
            ).otherwise(0).cast(DoubleType()))

        # Loan age in days
        .withColumn("loan_age_days",
            F.when(F.col("board_approval_date").isNotNull(),
                   F.datediff(F.current_date(), F.col("board_approval_date")))
            .otherwise(0).cast(DoubleType()))

        # Third-party involvement flag
        .withColumn("has_third_party",
            F.when(F.col("sold_3rd_party") > 0, 1.0).otherwise(0.0))

        # Log-transform principal amount (reduce skewness)
        .withColumn("log_principal",
            F.when(F.col("original_principal_amount") > 0,
                   F.log1p(F.col("original_principal_amount")))
            .otherwise(0.0))

        # ── TARGET VARIABLE ──────────────────────────────────────────────
        .withColumn("is_high_cancellation",
            F.when(F.col("cancellation_ratio") > CANCELLATION_THRESHOLD, 1.0)
            .otherwise(0.0))
    )

    # Fill remaining nulls in features
    df = df.fillna({
        "region": "Unknown", "loan_type": "Unknown", "loan_status": "Unknown",
        "interest_rate": 0.0, "service_charge_rate": 0.0,
        "processing_days": 0.0, "loan_age_days": 0.0,
        "disbursement_ratio": 0.0, "repayment_ratio": 0.0
    })

    # Log class distribution
    pos = df.where(F.col("is_high_cancellation") == 1.0).count()
    neg = df.where(F.col("is_high_cancellation") == 0.0).count()
    logger.info(f"  Class distribution: HIGH_CANCEL={pos:,} | NORMAL={neg:,}")
    logger.info(f"  Positive rate: {pos / (pos + neg) * 100:.2f}%")

    return df


# ══════════════════════════════════════════════════════════════════════════════
# ML PIPELINE CONSTRUCTION
# ══════════════════════════════════════════════════════════════════════════════
def build_ml_pipeline():
    """
    Constructs a PySpark ML Pipeline with:
      1. StringIndexer for categorical features
      2. OneHotEncoder for indexed categoricals
      3. Imputer for missing numeric values
      4. VectorAssembler to combine all features
      5. StandardScaler for feature normalization
      6. RandomForestClassifier (primary model)
    """
    logger.info("Building ML pipeline...")

    # ── Categorical encoding ─────────────────────────────────────────────
    # StringIndexer converts string categories to numeric indices
    region_indexer = StringIndexer(
        inputCol="region", outputCol="region_idx",
        handleInvalid="keep"
    )
    loan_type_indexer = StringIndexer(
        inputCol="loan_type", outputCol="loan_type_idx",
        handleInvalid="keep"
    )
    loan_status_indexer = StringIndexer(
        inputCol="loan_status", outputCol="loan_status_idx",
        handleInvalid="keep"
    )

    # OneHotEncoder converts indices to binary vectors
    encoder = OneHotEncoder(
        inputCols=["region_idx", "loan_type_idx", "loan_status_idx"],
        outputCols=["region_vec", "loan_type_vec", "loan_status_vec"],
        handleInvalid="keep"
    )

    # ── Numeric feature imputation ───────────────────────────────────────
    numeric_features = [
        "log_principal", "interest_rate", "service_charge_rate",
        "disbursement_ratio", "repayment_ratio",
        "processing_days", "loan_age_days", "has_third_party"
    ]

    imputer = Imputer(
        inputCols=numeric_features,
        outputCols=[f"{c}_imp" for c in numeric_features],
        strategy="median"
    )

    # ── Feature assembly ─────────────────────────────────────────────────
    assembler_inputs = (
        [f"{c}_imp" for c in numeric_features] +
        ["region_vec", "loan_type_vec", "loan_status_vec"]
    )

    assembler = VectorAssembler(
        inputCols=assembler_inputs,
        outputCol="features_raw",
        handleInvalid="keep"
    )

    # ── Feature scaling ──────────────────────────────────────────────────
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=False      # Sparse vector compatibility
    )

    # ── Model: Random Forest ─────────────────────────────────────────────
    rf_classifier = RandomForestClassifier(
        labelCol="is_high_cancellation",
        featuresCol="features",
        predictionCol="prediction",
        probabilityCol="probability",
        rawPredictionCol="rawPrediction",
        numTrees=100,
        maxDepth=10,
        minInstancesPerNode=5,
        featureSubsetStrategy="sqrt",
        seed=RANDOM_SEED
    )

    # ── Assemble full pipeline ───────────────────────────────────────────
    pipeline = Pipeline(stages=[
        region_indexer, loan_type_indexer, loan_status_indexer,
        encoder, imputer, assembler, scaler, rf_classifier
    ])

    logger.info("  ML pipeline built with 8 stages.")
    return pipeline, numeric_features


# ══════════════════════════════════════════════════════════════════════════════
# MODEL TRAINING & EVALUATION
# ══════════════════════════════════════════════════════════════════════════════
def train_and_evaluate(df: DataFrame, pipeline, numeric_features: list):
    """
    Trains the model with cross-validation and evaluates on a held-out test set.

    Evaluation Metrics:
      - AUC-ROC: Area under ROC curve (primary metric)
      - AUC-PR: Area under Precision-Recall curve (for imbalanced classes)
      - Accuracy, Precision, Recall, F1-Score
    """
    logger.info("=" * 60)
    logger.info("MODEL TRAINING & EVALUATION")
    logger.info("=" * 60)

    # ── Train/Test split ─────────────────────────────────────────────────
    train_df, test_df = df.randomSplit(
        [TRAIN_RATIO, TEST_RATIO], seed=RANDOM_SEED
    )
    logger.info(f"  Train set: {train_df.count():,} rows")
    logger.info(f"  Test set : {test_df.count():,} rows")

    # ── Cross-Validation with Hyperparameter Grid ────────────────────────
    logger.info("Setting up 3-fold cross-validation...")

    param_grid = (
        ParamGridBuilder()
        .addGrid(pipeline.getStages()[-1].numTrees, [50, 100, 200])
        .addGrid(pipeline.getStages()[-1].maxDepth, [5, 10, 15])
        .build()
    )

    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="is_high_cancellation",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator_auc,
        numFolds=3,
        parallelism=4,
        seed=RANDOM_SEED
    )

    # ── Train the model ──────────────────────────────────────────────────
    logger.info("Training model (this may take several minutes)...")
    cv_model = cv.fit(train_df)
    best_model = cv_model.bestModel

    logger.info("  Training complete. Best model selected via cross-validation.")

    # ── Evaluate on test set ─────────────────────────────────────────────
    logger.info("Evaluating on test set...")
    predictions = best_model.transform(test_df)

    # AUC-ROC
    auc_roc = evaluator_auc.evaluate(predictions)

    # AUC-PR (better for imbalanced datasets)
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="is_high_cancellation",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR"
    )
    auc_pr = evaluator_pr.evaluate(predictions)

    # Multi-class metrics (Accuracy, F1, Precision, Recall)
    mc_evaluator = MulticlassClassificationEvaluator(
        labelCol="is_high_cancellation",
        predictionCol="prediction"
    )

    accuracy = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "accuracy"})
    f1 = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "f1"})
    precision = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedPrecision"})
    recall = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedRecall"})

    # ── Log evaluation results ───────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("EVALUATION METRICS")
    logger.info("=" * 60)
    logger.info(f"  AUC-ROC           : {auc_roc:.4f}")
    logger.info(f"  AUC-PR            : {auc_pr:.4f}")
    logger.info(f"  Accuracy          : {accuracy:.4f}")
    logger.info(f"  F1-Score          : {f1:.4f}")
    logger.info(f"  Precision (wtd)   : {precision:.4f}")
    logger.info(f"  Recall (wtd)      : {recall:.4f}")
    logger.info("=" * 60)

    # ── Feature Importances ──────────────────────────────────────────────
    rf_model = best_model.stages[-1]    # RandomForestClassifier stage
    importances = rf_model.featureImportances.toArray()

    # Map feature indices back to names
    feature_names = (
        [f"{c}_imp" for c in numeric_features] +
        ["region_vec", "loan_type_vec", "loan_status_vec"]
    )

    logger.info("FEATURE IMPORTANCES (Top 10):")
    # Only map numeric features (vector features have aggregated importance)
    imp_pairs = sorted(
        zip(feature_names[:len(importances)], importances),
        key=lambda x: x[1], reverse=True
    )
    for fname, imp in imp_pairs[:10]:
        bar = "█" * int(imp * 50)
        logger.info(f"  {fname:<30} : {imp:.4f} {bar}")

    # ── Confusion Matrix ─────────────────────────────────────────────────
    logger.info("CONFUSION MATRIX:")
    predictions.groupBy("is_high_cancellation", "prediction").count().show()

    return cv_model, predictions


# ══════════════════════════════════════════════════════════════════════════════
# MODEL PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════
def save_model(cv_model, output_path: str):
    """Saves the trained cross-validator model to disk/ADLS."""
    logger.info(f"Saving model to {output_path}...")
    try:
        cv_model.bestModel.write().overwrite().save(output_path)
        logger.info(f"  ✓ Model saved successfully to {output_path}")
    except Exception as exc:
        logger.error(f"  ✗ Failed to save model: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def run_ml_pipeline(spark: SparkSession, use_local: bool = False):
    """Orchestrates the full ML pipeline: load → engineer → train → evaluate → save."""
    logger.info("=" * 60)
    logger.info("IBRD LOAN CANCELLATION PREDICTION PIPELINE")
    logger.info(f"  Cancellation Threshold: {CANCELLATION_THRESHOLD * 100:.0f}%")
    logger.info(f"  Train/Test Split      : {TRAIN_RATIO}/{TEST_RATIO}")
    logger.info("=" * 60)

    try:
        # Step 1: Load and prepare data
        df = load_and_prepare_data(spark, use_local)

        # Step 2: Build ML pipeline
        pipeline, numeric_features = build_ml_pipeline()

        # Step 3: Train and evaluate
        cv_model, predictions = train_and_evaluate(df, pipeline, numeric_features)

        # Step 4: Save model
        save_model(cv_model, MODEL_OUTPUT)

        logger.info("✅ ML PIPELINE COMPLETED SUCCESSFULLY")

    except Exception as exc:
        logger.critical(f"ML pipeline failed: {exc}", exc_info=True)
        raise


if __name__ == "__main__":
    use_local = "--local" in sys.argv
    spark = None
    try:
        spark = create_spark_session()
        run_ml_pipeline(spark, use_local=use_local)
    except Exception as exc:
        logger.critical(f"Fatal: {exc}"); sys.exit(1)
    finally:
        if spark: spark.stop()
