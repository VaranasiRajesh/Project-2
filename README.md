# 🏦 Enhancing Loan Management Efficiency for the International Bank for Reconstruction and Development (IBRD)

> **A production-grade, end-to-end data engineering pipeline built on the Medallion Architecture to analyze IBRD loan cancellation patterns and predict cancellation risk using PySpark, Azure Cloud Services, Snowflake, and PowerBI.**

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Business Problem](#-business-problem)
- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Directory Structure](#-directory-structure)
- [Pipeline Flow — Step by Step](#-pipeline-flow--step-by-step)
  - [Phase 1: Data Ingestion](#phase-1-data-ingestion-bronze-layer)
  - [Phase 2: Data Cleansing](#phase-2-data-cleansing-bronze--silver)
  - [Phase 3: Star Schema Transformation](#phase-3-star-schema-transformation-silver--gold)
  - [Phase 4: Real-Time Streaming](#phase-4-real-time-streaming)
  - [Phase 5: Cloud Data Warehousing](#phase-5-cloud-data-warehousing-snowflake)
  - [Phase 6: Business Intelligence](#phase-6-business-intelligence-powerbi)
  - [Phase 7: Machine Learning](#phase-7-machine-learning-prediction)
  - [Phase 8: Orchestration](#phase-8-orchestration-airflow)
- [Star Schema Design](#-star-schema-design)
- [Key Design Decisions](#-key-design-decisions)
- [How to Run](#-how-to-run)
- [Security](#-security)
- [Monitoring & Alerting](#-monitoring--alerting)
- [Future Enhancements](#-future-enhancements)

---

## 🎯 Project Overview

The **International Bank for Reconstruction and Development (IBRD)** is one of the five institutions that make up the World Bank Group. IBRD provides loans to middle-income and creditworthy low-income countries. Managing a portfolio of hundreds of thousands of loans across 100+ countries requires robust data infrastructure to detect inefficiencies, especially around **loan cancellations** — where committed funds are returned unused.

This project builds that infrastructure:

| What We Build | Why It Matters |
|----------------|---------------|
| **Automated data pipeline** | Eliminates manual data collection from World Bank APIs |
| **Medallion Architecture** | Ensures data quality through progressive refinement (Raw → Clean → Analytics) |
| **Star Schema warehouse** | Enables fast, intuitive querying for analysts and dashboards |
| **Real-time monitoring** | Flags high-risk cancellations ($1M+) the moment data arrives |
| **ML prediction model** | Predicts which loans are likely to experience significant cancellations |
| **PowerBI dashboards** | Provides executive-level visibility into portfolio health |

---

## 💼 Business Problem

**Problem Statement:** IBRD needs to understand *why* loans get cancelled and *which* loans are at risk of cancellation, so that loan officers can intervene early and reduce financial waste.

**Key Business Questions This Pipeline Answers:**

1. **Loan Processing Time** — How long does it take from board approval to agreement signing, and does processing speed correlate with cancellation risk?

2. **Top Borrowing Countries** — Which countries receive the most IBRD loans, and are any showing year-over-year decline in borrowing?

3. **Cancellation Patterns** — What is the cancellation rate by region, loan type, and borrower? Are there geographic or temporal trends?

4. **Repayment Analysis** — What is the average repayment duration by country, and do longer repayment periods correlate with higher default/cancellation risk?

5. **Predictive Risk** — Given a loan's characteristics (country, interest rate, principal amount, loan type), can we predict whether it will experience a significant cancellation?

---

## 🏗 Architecture

This pipeline follows the **Medallion Architecture** — a data design pattern popularized by Databricks that organizes data into three progressive layers of quality:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                       │
│                                                                                 │
│   ┌─────────────────┐     ┌─────────────────┐     ┌────────────────────────┐   │
│   │  World Bank API  │     │  Local CSV/JSON  │     │  Real-Time CSV Files   │   │
│   │  (SODA Endpoint) │     │  (Historical)    │     │  (Streaming Input)     │   │
│   └────────┬────────┘     └────────┬────────┘     └───────────┬────────────┘   │
│            │                       │                           │                 │
└────────────┼───────────────────────┼───────────────────────────┼─────────────────┘
             │                       │                           │
             ▼                       ▼                           ▼
┌─────────────────────────────────────────────────┐  ┌──────────────────────────┐
│              BATCH INGESTION                     │  │   STREAMING INGESTION    │
│                                                  │  │                          │
│  fetch_ibrd_loans.py                             │  │  loan_stream_            │
│  • Paginated API calls (10K records/page)        │  │  processing.py           │
│  • Exponential backoff retry (5 attempts)        │  │  • Strict StructType     │
│  • Saves ibrd_loans_1M.csv locally               │  │  • Filters > $1M cancel  │
│  • Post-ingestion validation                     │  │  • Console + checkpoint  │
│                                                  │  │                          │
└──────────────────┬───────────────────────────────┘  └──────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        AZURE DATA FACTORY                                        │
│                                                                                  │
│    Pipeline: pl_ingest_ibrd_api_to_adls_bronze                                   │
│    ┌────────────────┐      ┌─────────────────┐      ┌────────────────────────┐   │
│    │  HTTP Source    │─────▶│  Copy Data      │─────▶│  ADLS Gen2 Sink        │   │
│    │  (World Bank)   │      │  Activity        │      │  (Bronze Layer)        │   │
│    └────────────────┘      └─────────────────┘      └────────────────────────┘   │
│                                                                                  │
│    Features: 3 retries, 1-hour timeout, audit logging, failure handler           │
└──────────────────────────────────────────────────────┬───────────────────────────┘
                                                       │
                                                       ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    ADLS GEN2 — MEDALLION DATA LAKE                               │
│                                                                                  │
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────────────┐  │
│  │   🟤 BRONZE       │     │   ⚪ SILVER       │     │   🟡 GOLD                │  │
│  │   (Raw Zone)      │────▶│   (Clean Zone)    │────▶│   (Analytics Zone)       │  │
│  │                   │     │                   │     │                          │  │
│  │ • loans_raw.csv   │     │ • loans_clean     │     │ • fact_loans             │  │
│  │ • As-is from API  │     │   .parquet/       │     │ • dim_country            │  │
│  │ • Immutable       │     │ • Partitioned by  │     │ • dim_project            │  │
│  │ • 90-day retain   │     │   country_code    │     │ • dim_loan_type          │  │
│  │                   │     │ • Snappy compress  │     │ • dim_borrower           │  │
│  └──────────────────┘     └──────────────────┘     └────────────┬─────────────┘  │
│                                                                  │                │
│  PySpark: clean_loan_data.py          PySpark: transform_loans.py│                │
└──────────────────────────────────────────────────────────────────┼────────────────┘
                                                                   │
                                            ┌──────────────────────┘
                                            ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                          ❄️ SNOWFLAKE                                            │
│                                                                                  │
│    Snowpipe (AUTO_INGEST = TRUE)                                                 │
│    ┌──────────────┐     ┌──────────────────┐     ┌─────────────────────────┐    │
│    │ ADLS External │────▶│  Event Grid      │────▶│  COPY INTO tables       │    │
│    │ Stage          │     │  Notification    │     │  (fact + 4 dimensions)  │    │
│    └──────────────┘     └──────────────────┘     └─────────────────────────┘    │
│                                                                                  │
│    Database: IBRD_LOAN_ANALYTICS  |  Schema: GOLD  |  Warehouse: WH_IBRD_ETL   │
└──────────────────────────────────────────────────────┬───────────────────────────┘
                                                       │
                              ┌─────────────────────────┤
                              ▼                         ▼
┌──────────────────────────────────┐  ┌────────────────────────────────────────────┐
│       📊 POWERBI                  │  │       🤖 MACHINE LEARNING                  │
│                                   │  │                                            │
│  4-Page Dashboard:                │  │  cancellation_predictor.py                 │
│  1. Executive Summary (KPIs)     │  │  • 8 engineered features                   │
│  2. Cancellation Deep Dive       │  │  • RandomForest + 3-fold CV                │
│  3. Loan Lifecycle Analysis      │  │  • AUC-ROC, F1, Precision, Recall          │
│  4. Borrower & Country Insights  │  │  • Feature importance ranking              │
│                                   │  │  • Predicts high-risk cancellations        │
│  20+ DAX measures                │  │                                            │
│  Snowflake DirectQuery           │  │  Target: cancelled > 10% of principal      │
└──────────────────────────────────┘  └────────────────────────────────────────────┘

                              ┌─────────────────────────────┐
                              │       ⏰ ORCHESTRATION       │
                              │                             │
                              │  Apache Airflow DAG         │
                              │  Schedule: Daily @ 02:00 UTC│
                              │                             │
                              │  fetch_api ──▶ adf_ingest   │
                              │           ──▶ bronze_silver  │
                              │           ──▶ silver_gold    │
                              │                             │
                              │  Retries, email alerts,     │
                              │  failure handling           │
                              └─────────────────────────────┘
```

---

## 🛠 Technology Stack

| Layer | Technology | Version | Role |
|-------|-----------|---------|------|
| **Ingestion** | Python `requests` | 2.31+ | Paginated API data fetch with retry logic |
| **Ingestion** | Azure Data Factory | — | Cloud-native Copy Data pipeline (HTTP → ADLS) |
| **Storage** | Azure Data Lake Gen2 | — | Medallion Architecture data lake (Bronze/Silver/Gold) |
| **ETL** | Apache Spark (PySpark) | 3.3+ | Distributed data cleansing & transformation |
| **Streaming** | Spark Structured Streaming | 3.3+ | Real-time CSV file monitoring & risk flagging |
| **Warehouse** | Snowflake | — | Cloud data warehouse (Star Schema) |
| **Loading** | Snowpipe | — | Event-driven auto-ingest from ADLS → Snowflake |
| **Visualization** | Microsoft PowerBI | — | Executive dashboards & KPI reporting |
| **ML** | PySpark MLlib | 3.3+ | Loan cancellation binary classification |
| **Orchestration** | Apache Airflow | 2.5+ | DAG-based pipeline scheduling & monitoring |
| **Infrastructure** | Azure ARM Templates | — | Infrastructure-as-Code for ADF deployment |

---

## 📁 Directory Structure

```
project-2/
│
├── 📁 1_Data_Source/                          ← PHASE 1: Raw data acquisition
│   └── world_bank_api/
│       └── fetch_ibrd_loans.py               Python script to fetch IBRD loans via API
│
├── 📁 2_ADF_Pipelines/                       ← PHASE 1: Cloud ingestion
│   └── ingest_api_to_adls.json              ADF ARM template (HTTP → ADLS Bronze)
│
├── 📁 3_ADLS_Data_Lake/                      ← DATA LAKE: All three Medallion layers
│   ├── README.md                             Layer documentation
│   ├── schema_manifest.json                  Column-level schema for all layers
│   ├── bronze_layer/                         Raw CSV (as-received)
│   ├── silver_layer/                         Cleaned Parquet (partitioned)
│   └── gold_layer/                           Star Schema Parquet (5 tables)
│
├── 📁 4_PySpark_ETL/                         ← PHASE 2 & 3: Transformations
│   ├── bronze_to_silver/
│   │   └── clean_loan_data.py               Data cleansing pipeline
│   └── silver_to_gold/
│       └── transform_loans.py               Star Schema builder
│
├── 📁 5_Spark_Streaming/                     ← PHASE 4: Real-time processing
│   ├── spark_stream_processing/
│   │   └── loan_stream_processing.py        Structured Streaming with risk filtering
│   ├── streaming_input/                      Drop zone for incoming CSV files
│   └── checkpoints/                          Exactly-once processing checkpoints
│
├── 📁 6_Snowflake/                           ← PHASE 5: Cloud data warehouse
│   ├── tables/
│   │   └── create_tables.sql                DDL for Star Schema (5 tables)
│   └── snowpipe/
│       └── create_snowpipe.sql              Auto-ingest pipe configuration
│
├── 📁 7_PowerBI/                             ← PHASE 6: Business intelligence
│   ├── dax_measures/
│   │   └── dax_measures.dax                 20+ production DAX measures
│   └── dashboard_template/
│       └── dashboard_config.json            4-page dashboard blueprint
│
├── 📁 8_ML_Model/                            ← PHASE 7: Predictive analytics
│   ├── cancellation_predictor.py            PySpark MLlib training pipeline
│   └── model_metadata.json                  Model card & documentation
│
├── 📁 9_Orchestration/                       ← PHASE 8: Pipeline scheduling
│   └── airflow_dags/
│       └── loan_pipeline_dag.py             Apache Airflow daily DAG
│
├── 📁 10_Documentation/                      ← Project documentation
│   ├── architecture.md                       System architecture & diagrams
│   ├── data_dictionary.md                    Column-level docs (all layers)
│   └── setup_guide.md                        Environment setup instructions
│
└── 📄 README.md                              ← This file
```

---

## 🔄 Pipeline Flow — Step by Step

### Phase 1: Data Ingestion (Bronze Layer)

**Goal:** Get raw IBRD loan data from the World Bank into our data lake — reliably, repeatedly, and at scale.

**File:** `1_Data_Source/world_bank_api/fetch_ibrd_loans.py`

```
World Bank SODA API ──[HTTP GET]──▶ fetch_ibrd_loans.py ──▶ ibrd_loans_1M.csv (local)
```

**How it works:**

1. **API Connection** — The script connects to the World Bank's SODA-compatible REST API endpoint (`https://finances.worldbank.org/resource/zucq-nqt1.json`). This endpoint exposes the complete IBRD Statement of Loans dataset.

2. **Session Configuration** — A `requests.Session` is created with an `HTTPAdapter` that configures automatic retry behavior. If the API returns HTTP 429 (rate limited), 500, 502, 503, or 504, the session automatically retries up to **5 times** with **exponential backoff** (2s → 4s → 8s → 16s → 32s). This prevents the pipeline from failing due to transient API issues.

3. **Paginated Fetching** — The API is called in pages of **10,000 records** each using `$limit` and `$offset` parameters. Records are ordered by `$order=:id` to ensure deterministic pagination (no records skipped or duplicated between pages). The script continues fetching until either:
   - A page returns fewer than 10,000 records (end of dataset), or
   - The total exceeds 1,000,000 records (safety cap)

4. **Failure Handling** — If 3 consecutive pages fail, the script aborts gracefully. Each failure is logged with the HTTP status code and response body for debugging.

5. **CSV Persistence** — All records are collected into a list of dictionaries, then written to `ibrd_loans_1M.csv` using Python's `csv.DictWriter`. The writer automatically handles sparse records (where some API records have fields that others don't).

6. **Post-Validation** — After writing, the script reopens the CSV and validates:
   - File exists and is non-empty
   - Row count matches expectations
   - CSV is parseable (not corrupted)

7. **Logging** — Every step is logged to both the console (INFO level) and a log file (DEBUG level) with timestamps, function names, and line numbers.

---

**File:** `2_ADF_Pipelines/ingest_api_to_adls.json`

```
HTTP (World Bank API) ──[ADF Copy Data Activity]──▶ ADLS Gen2 Bronze Layer
```

**How it works:**

1. **ARM Template** — This is an Azure Resource Manager template that deploys:
   - An **HTTP Linked Service** pointing to the World Bank API
   - An **ADLS Gen2 Linked Service** with account key authentication
   - A **source dataset** (HTTP/CSV) configured to fetch up to 500K records
   - A **sink dataset** targeting the Bronze layer with a timestamped filename
   - A **pipeline** containing the Copy Data activity

2. **Copy Data Activity** — The core activity fetches CSV data from the API via HTTP GET and writes it directly to ADLS Gen2 at `3_ADLS_Data_Lake/bronze_layer/ibrd_loans_raw_YYYYMMDD_HHmmss.csv`. The filename includes a timestamp so each daily run creates a new file (preserving history).

3. **Resilience** — The activity is configured with 3 retries (60-second intervals), a 1-hour timeout, and `enableSkipIncompatibleRow=true` so that malformed records don't crash the entire copy.

4. **Audit Logging** — Copy activity logs are written to `pipeline_logs/copy_activity/` in ADLS, including row counts, data size, duration, and any errors.

5. **Failure Handling** — A `SetVariable` activity captures error details if the copy fails, making it available for downstream alerting.

---

### Phase 2: Data Cleansing (Bronze → Silver)

**Goal:** Transform messy, untyped raw CSV data into a clean, strongly-typed, deduplicated Parquet dataset ready for analytics.

**File:** `4_PySpark_ETL/bronze_to_silver/clean_loan_data.py`

```
🟤 Bronze (loans_raw.csv) ──[7-Step PySpark Pipeline]──▶ ⚪ Silver (loans_clean.parquet/)
```

**The 7-step transformation pipeline:**

| Step | Operation | What It Does | Why It Matters |
|------|-----------|--------------|----------------|
| 1 | **Read Raw CSV** | Reads `loans_raw.csv` with `inferSchema=true` and `mode=PERMISSIVE`. Corrupt records are captured in a special `_corrupt_record` column and quarantined. | Prevents bad records from poisoning the pipeline. |
| 2 | **Column Standardization** | Converts all column names to `snake_case`. Strips whitespace, replaces special characters with underscores, collapses multiple underscores, lowercases everything. Example: `"  Original Principal Amount (USD) "` → `"original_principal_amount_usd"` | Eliminates naming inconsistencies that break downstream queries. |
| 3 | **Numeric Casting** | Casts 13 monetary/rate columns from String to `DoubleType`. This includes `original_principal_amount`, `cancelled_amount`, `interest_rate`, etc. Values that can't be cast become NULL. | Enables accurate arithmetic (SUM, AVG, division) in aggregations. String "1000000" + "2000000" ≠ 3000000. |
| 4 | **Date Parsing** | Converts 8 date columns from strings to `DateType` using a multi-format `coalesce` strategy. Tries 4 formats in order: ISO 8601 with millis, ISO 8601, US format (MM/dd/yyyy), standard (yyyy-MM-dd). | Handles the inconsistent date formats that the World Bank API sometimes returns. |
| 5 | **Null Handling** | Fills nulls with domain-specific defaults: monetary columns → `0.0`, rate columns → `0.0`, categorical columns → `'Unknown'`. This is intentional — a NULL in `cancelled_amount` means "no cancellation reported", which is semantically `0.0`. | Prevents downstream aggregations from returning NULL and ensures JOIN operations work correctly. |
| 6 | **Deduplication** | Two-pass dedup: first, exact duplicate removal via `dropDuplicates()`. Second, business-key dedup using a window function — partition by `loan_number`, order by `end_of_period DESC`, keep only the latest record per loan. | The API often returns multiple snapshots of the same loan across reporting periods. We want the most current state. |
| 7 | **Write Parquet** | Writes the clean dataframe to the Silver layer as Parquet with Snappy compression, partitioned by `country_code`. | Parquet is columnar (10-50x faster reads than CSV), Snappy offers the best compression/speed tradeoff, and partitioning by country enables partition pruning in queries. |

**Data Quality Profile:** After transformation, the script logs null percentages per column and distinct counts for key categoricals (region, country, loan_type, loan_status). This serves as a data quality dashboard in the logs.

---

### Phase 3: Star Schema Transformation (Silver → Gold)

**Goal:** Reshape the flat cleaned dataset into a dimensional Star Schema optimized for analytical queries and PowerBI.

**File:** `4_PySpark_ETL/silver_to_gold/transform_loans.py`

```
⚪ Silver (loans_clean.parquet/) ──[Star Schema Builder]──▶ 🟡 Gold (5 Parquet tables)
```

**Why a Star Schema?**

A Star Schema separates data into **dimension tables** (descriptive attributes) and **fact tables** (measurable events). This design:
- Reduces data redundancy (country name stored once, not repeated in every row)
- Enables fast aggregations (fact table is smaller, dimensions are broadcast-joined)
- Maps naturally to PowerBI's relationship model
- Follows the industry standard for OLAP analytics

**The 4 Dimension Tables:**

| Table | Natural Key | Attributes | Rows (typical) |
|-------|-------------|------------|-----------------|
| **dim_country** | `country_code` | country, region, guarantor_country_code, guarantor | ~150 |
| **dim_project** | `project_id` | project_name | ~10,000 |
| **dim_loan_type** | `loan_type` + `loan_status` + `currency` | — (composite key) | ~50 |
| **dim_borrower** | `borrower` | country_code | ~5,000 |

Each dimension gets a **surrogate key** (e.g., `country_key`) generated via `monotonically_increasing_id()`. This integer key is more efficient for joins than string-based natural keys.

**The Fact Table — `fact_loans`:**

The fact table contains:
- **Foreign keys** linking to each dimension (`country_key`, `project_key`, `loan_type_key`, `borrower_key`)
- **Degenerate dimensions** kept directly in the fact for drill-through (`loan_number`, `end_of_period`)
- **Date keys** for time-based analysis (7 date columns)
- **Measures** — the numeric values analysts aggregate: `original_principal_amount`, `cancelled_amount`, `disbursed_amount`, `repaid_to_ibrd`, `due_to_ibrd`, `borrowers_obligation`, `interest_rate`, `service_charge_rate`

**Join Strategy:** Dimension tables are **broadcast-joined** onto the fact table using `F.broadcast()`. Since dimensions are small (< 1MB typically), Spark sends the entire dimension to every executor, eliminating expensive shuffle operations. This can make the join **10-100x faster** than a standard sort-merge join.

**Output:** All 5 tables are written to `3_ADLS_Data_Lake/gold_layer/` as individual Parquet directories with Snappy compression. Small dimension tables are `coalesce(1)` to avoid creating many tiny files.

---

### Phase 4: Real-Time Streaming

**Goal:** Monitor incoming loan data in real-time and immediately flag high-risk cancellations for human review.

**File:** `5_Spark_Streaming/spark_stream_processing/loan_stream_processing.py`

```
CSV files landing in streaming_input/ ──[Structured Streaming]──▶ Console alerts
```

**How it works:**

1. **Source** — `readStream` monitors the `5_Spark_Streaming/streaming_input/` directory. Whenever a new CSV file is dropped into this folder, Spark picks it up in the next micro-batch.

2. **Schema Enforcement** — Unlike batch reads, streaming requires an explicit `StructType` schema (29 fields). This prevents schema drift between batches and avoids the overhead of inference on every micro-batch.

3. **Processing** (per micro-batch):
   - Add `ingestion_timestamp` (when Spark processed the record)
   - Add `source_file` (which CSV file the record came from)
   - Filter: keep only records where `cancelled_amount > $1,000,000`
   - Categorize risk: `> $10M` = CRITICAL, `> $5M` = HIGH, else ELEVATED
   - Calculate `cancellation_ratio` (cancelled ÷ principal × 100)

4. **Sink** — Filtered records are written to the console with `truncate=false` so analysts monitoring the terminal can see full country names, project names, and amounts.

5. **Checkpointing** — Write-ahead logs are stored in `checkpoints/streaming_checkpoint/`. If the streaming job crashes and restarts, it resumes from exactly where it left off — no records are duplicated or lost (**exactly-once semantics**).

6. **Trigger** — Micro-batches run every 30 seconds. At most 5 files are processed per batch to prevent memory spikes.

---

### Phase 5: Cloud Data Warehousing (Snowflake)

**Goal:** Load the Gold layer Star Schema into Snowflake for SQL-based analytics, PowerBI connectivity, and multi-user concurrent querying.

**Files:** `6_Snowflake/tables/create_tables.sql` + `6_Snowflake/snowpipe/create_snowpipe.sql`

```
🟡 Gold (ADLS Parquet) ──[Event Grid notification]──▶ ❄️ Snowpipe ──▶ Snowflake tables
```

**Step-by-step setup:**

1. **Database & Schema** — Creates `IBRD_LOAN_ANALYTICS` database with a `GOLD` schema. A dedicated `WH_IBRD_ETL` warehouse (MEDIUM size, auto-suspend after 2 minutes) handles compute.

2. **Table DDL** — Creates all 5 Star Schema tables with:
   - `PRIMARY KEY` on surrogate keys
   - `REFERENCES` (foreign key) constraints from fact to dimensions
   - `NUMBER(18,2)` for monetary measures (exact decimal, no floating-point errors)
   - `_loaded_at` and `_source_file` audit columns (auto-populated)
   - **Clustering** on `fact_loans` by `(country_key, board_approval_date)` — this physically orders data on disk to accelerate the most common query patterns
   - **Search optimization** on `loan_number`, `country_key`, `project_key` for fast lookups

3. **RBAC** — Two roles: `IBRD_ANALYST` (SELECT only) and `IBRD_ENGINEER` (full access). Analysts can query but never modify data.

4. **Storage Integration** — A Snowflake Storage Integration connects to ADLS Gen2 via Azure AD Service Principal (OAuth2, no shared keys).

5. **External Stages** — Five external stages map to the five Gold layer Parquet directories in ADLS.

6. **Snowpipe** — Five pipes (one per table) with `AUTO_INGEST = TRUE`. When a new Parquet file lands in ADLS:
   - Azure Event Grid fires a notification
   - Snowflake receives the event
   - Snowpipe executes `COPY INTO` with column mapping from Parquet's `$1` notation
   - Audit columns are auto-populated: `_loaded_at = CURRENT_TIMESTAMP()`, `_source_file = METADATA$FILENAME`

**This is fully event-driven** — there is no polling, no scheduling, no manual intervention. Data flows from ADLS to Snowflake within seconds of the Gold layer write completing.

---

### Phase 6: Business Intelligence (PowerBI)

**Goal:** Provide interactive dashboards for IBRD executives, loan officers, and analysts to monitor portfolio health and cancellation patterns.

**Files:** `7_PowerBI/dax_measures/dax_measures.dax` + `7_PowerBI/dashboard_template/dashboard_config.json`

**4-Page Dashboard Layout:**

| Page | Audience | Key Visuals |
|------|----------|-------------|
| **1. Executive Summary** | C-suite, senior stakeholders | KPI cards (total loans, cancellations, rate), filled world map, yearly trend line |
| **2. Cancellation Deep Dive** | Risk officers, loan managers | Regional treemap, Top 10 bar chart, scatter plot (cancellation vs. interest rate), matrix heatmap |
| **3. Loan Lifecycle** | Operations team | Disbursement efficiency gauge, processing days card, loan pipeline funnel, processing days by region |
| **4. Borrower Insights** | Country teams, analysts | Top 20 borrowers table, loans-by-region donut, portfolio composition stacked bar |

**20+ DAX Measures including:**
- `Cancellation Rate` — Portfolio-wide cancelled ÷ principal
- `YoY Cancellation Change` — Compares current vs. previous year using `DATEADD`
- `Weighted Avg Interest Rate` — Weighted by principal amount (not simple average)
- `Disbursement Efficiency` — Disbursed ÷ principal as a percentage
- `Avg Processing Days` — `DATEDIFF` from board approval to agreement signing
- `High Risk Cancellation Count` — Loans where cancelled > $1M
- `Top 3 Countries` — Dynamic ranking using `TOPN` + `CONCATENATEX`

**Slicers:** Region, Country, Loan Status, Loan Type (all multi-select), and a Date Range slider on Board Approval Date.

---

### Phase 7: Machine Learning (Prediction)

**Goal:** Build a binary classification model that predicts whether a loan will experience a significant cancellation (> 10% of original principal).

**File:** `8_ML_Model/cancellation_predictor.py`

```
Gold layer data ──▶ Feature Engineering ──▶ ML Pipeline ──▶ Trained Model + Metrics
```

**Feature Engineering (8 features):**

| Feature | Type | Derivation | Hypothesis |
|---------|------|------------|------------|
| `log_principal` | Numeric | `log1p(original_principal_amount)` | Larger loans may have different cancellation patterns; log reduces skewness |
| `interest_rate` | Numeric | Direct from data | Higher rates may correlate with riskier borrowers |
| `service_charge_rate` | Numeric | Direct from data | Additional cost indicator |
| `disbursement_ratio` | Numeric | `disbursed ÷ principal` | Low disbursement may indicate project issues leading to cancellation |
| `repayment_ratio` | Numeric | `repaid ÷ principal` | Active repayment suggests low cancellation risk |
| `processing_days` | Numeric | `DATEDIFF(approval, signing)` | Long processing may indicate complications |
| `loan_age_days` | Numeric | `DATEDIFF(approval, today)` | Older loans have had more time for cancellations |
| `has_third_party` | Binary | `1 if sold_3rd_party > 0` | Third-party involvement may affect cancellation dynamics |
| `region` | Categorical | OneHotEncoded | Geographic patterns in cancellation behavior |
| `loan_type` | Categorical | OneHotEncoded | Different loan types have different risk profiles |
| `loan_status` | Categorical | OneHotEncoded | Disbursing vs. Repaying vs. Approved |

**ML Pipeline (PySpark Pipeline API):**

```
StringIndexer → OneHotEncoder → Imputer (median) → VectorAssembler → StandardScaler → RandomForestClassifier
```

**Training Strategy:**
- **Train/Test Split:** 80/20 (seed=42 for reproducibility)
- **Cross-Validation:** 3-fold CV on training set
- **Hyperparameter Grid:** `numTrees` × `maxDepth` = [50, 100, 200] × [5, 10, 15] = 9 combinations
- **Primary Metric:** AUC-ROC (handles class imbalance better than accuracy)

**Evaluation Outputs:**
- AUC-ROC, AUC-PR, Accuracy, F1-Score, Weighted Precision, Weighted Recall
- Confusion Matrix
- Feature Importance ranking (with visual bars in logs)
- Saved model artifact in `8_ML_Model/trained_model/`

---

### Phase 8: Orchestration (Airflow)

**Goal:** Automate the entire pipeline to run daily without human intervention, with built-in failure handling and alerting.

**File:** `9_Orchestration/airflow_dags/loan_pipeline_dag.py`

```
Daily @ 02:00 UTC:

  pipeline_start ──▶ fetch_ibrd_api_data ──▶ trigger_adf_ingestion
                                            ──▶ pyspark_bronze_to_silver
                                            ──▶ pyspark_silver_to_gold ──▶ pipeline_complete
                                                                        ──▶ handle_failure (on error)
```

**DAG Configuration:**
- **Schedule:** `0 2 * * *` (daily at 2:00 AM UTC — off-peak hours)
- **Catchup:** `False` (don't backfill historical runs)
- **Max Active Runs:** 1 (only one pipeline instance at a time)
- **Retries:** 2 per task with exponential backoff (5min → 10min)
- **Execution Timeout:** 2 hours per task
- **Email Alerts:** Sent on task failure to `data-engineering@ibrd-pipeline.org`

**Task Details:**

| Task | Operator | What It Does |
|------|----------|--------------|
| `pipeline_start` | DummyOperator | Entry sentinel (clear DAG entry point) |
| `fetch_ibrd_api_data` | BashOperator | Runs `fetch_ibrd_loans.py` (3 retries, 10-min delay) |
| `trigger_adf_ingestion` | BashOperator | Azure CLI `az datafactory pipeline create-run` |
| `pyspark_bronze_to_silver` | BashOperator | `spark-submit clean_loan_data.py` on YARN |
| `pyspark_silver_to_gold` | BashOperator | `spark-submit transform_loans.py` on YARN |
| `pipeline_complete` | DummyOperator | Success sentinel (`ALL_SUCCESS` trigger rule) |
| `handle_pipeline_failure` | BashOperator | Error logger (`ONE_FAILED` trigger rule) |

**Dependency Chain:** Each task depends on the previous task's success. If `fetch_ibrd_api_data` fails, `trigger_adf_ingestion`, `pyspark_bronze_to_silver`, and `pyspark_silver_to_gold` are all automatically **skipped** — they never execute with missing data.

---

## ⭐ Star Schema Design

```
                    ┌─────────────────┐
                    │   dim_country   │
                    │─────────────────│
                    │ country_key (PK)│
                    │ country_code    │
                    │ country         │
                    │ region          │
                    │ guarantor       │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────────┐
│   dim_project   │          │          │    dim_loan_type     │
│─────────────────│          │          │─────────────────────│
│ project_key (PK)│          │          │ loan_type_key (PK)  │
│ project_id      │    ┌─────┴──────┐   │ loan_type           │
│ project_name    │────│ fact_loans  │───│ loan_status         │
│                 │    │────────────│   │ currency_of_commit. │
└─────────────────┘    │ loan_fact_ │   └─────────────────────┘
                       │   key (PK) │
                       │            │
                       │ country_key│   ┌─────────────────────┐
                       │ project_key│   │    dim_borrower      │
                       │ loan_type_ │   │─────────────────────│
                       │   key      │───│ borrower_key (PK)   │
                       │ borrower_  │   │ borrower            │
                       │   key      │   │ country_code        │
                       │            │   └─────────────────────┘
                       │ MEASURES:  │
                       │ principal  │
                       │ cancelled  │
                       │ disbursed  │
                       │ repaid     │
                       │ due        │
                       │ int. rate  │
                       └────────────┘
```

---

## 🔑 Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Medallion Architecture** (Bronze/Silver/Gold) | Industry-standard pattern for progressive data quality. Bronze preserves raw data (reprocessable), Silver provides a clean base, Gold is optimized for consumption. |
| **Parquet + Snappy** over CSV | Parquet is columnar (10-50x faster for analytical queries), supports predicate pushdown, and Snappy offers the best compression-to-speed ratio. |
| **Partition by `country_code`** | Most analytical queries filter by country/region. Partitioning enables Spark to skip irrelevant partitions entirely (partition pruning). |
| **Broadcast joins** for dimensions | Dimension tables are typically < 1MB. Broadcasting avoids expensive shuffle operations, making joins 10-100x faster. |
| **`monotonically_increasing_id()`** for surrogate keys | Guaranteed unique across Spark partitions. Not consecutive, but uniqueness is all that matters for surrogate keys. |
| **Multi-format date parsing** with `coalesce` | The World Bank API returns dates in inconsistent formats. Trying multiple formats ensures maximum parse coverage. |
| **Snowpipe AUTO_INGEST** over scheduled COPY | Event-driven loading (via Azure Event Grid) means data appears in Snowflake within seconds of Gold layer write — no polling overhead, no scheduling lag. |
| **RandomForest** over deep learning | Tabular data with categorical features. Random Forest handles this well, is interpretable (feature importances), and doesn't require GPU infrastructure. |
| **3-fold CV** with hyperparameter grid | Prevents overfitting on the training set and systematically finds the best hyperparameter combination. |
| **AUC-ROC** as primary metric | Better than accuracy for imbalanced classes (most loans are NOT high-cancellation). AUC-ROC measures ranking quality. |

---

## 🚀 How to Run

### Quick Start (Local Development)

```bash
# 1. Fetch data from World Bank API
python 1_Data_Source/world_bank_api/fetch_ibrd_loans.py

# 2. Run Bronze → Silver cleansing (local mode)
spark-submit 4_PySpark_ETL/bronze_to_silver/clean_loan_data.py --local

# 3. Run Silver → Gold transformation (local mode)
spark-submit 4_PySpark_ETL/silver_to_gold/transform_loans.py --local

# 4. Train ML model (local mode)
spark-submit 8_ML_Model/cancellation_predictor.py --local

# 5. Start streaming monitor
spark-submit 5_Spark_Streaming/spark_stream_processing/loan_stream_processing.py
```

### Production (Azure + Airflow)

```bash
# Deploy ADF pipeline
az deployment group create \
    --resource-group rg-ibrd-pipeline \
    --template-file 2_ADF_Pipelines/ingest_api_to_adls.json

# Setup Snowflake tables and Snowpipe
snowsql -f 6_Snowflake/tables/create_tables.sql
snowsql -f 6_Snowflake/snowpipe/create_snowpipe.sql

# Deploy and trigger Airflow DAG
cp 9_Orchestration/airflow_dags/loan_pipeline_dag.py ~/airflow/dags/
airflow dags trigger ibrd_loan_pipeline_daily
```

See `10_Documentation/setup_guide.md` for detailed environment setup instructions.

---

## 🔐 Security

| Concern | Implementation |
|---------|---------------|
| **Credentials** | All secrets in environment variables (never hard-coded in scripts) |
| **ADLS Authentication** | Azure AD Service Principal (OAuth2) for production; account keys for dev only |
| **Snowflake RBAC** | `IBRD_ANALYST` role = read-only; `IBRD_ENGINEER` = full access |
| **Snowpipe** | Storage Integration via Azure AD (no shared access keys) |
| **Key Management** | Compatible with Azure Key Vault and AWS Secrets Manager |
| **Network** | Recommend ADLS firewall rules + Snowflake network policies |
| **ADF** | ARM template uses `securestring` for account keys |

---

## 📡 Monitoring & Alerting

| Component | Monitoring Method | Alert Mechanism |
|-----------|-------------------|-----------------|
| API Ingestion | Python logging (console + file) | Log file analysis |
| ADF Pipeline | Azure Monitor + Activity logs | ADF built-in alerts |
| PySpark ETL | Spark UI + structured logging | Log aggregation |
| Streaming | Console output + Spark Streaming UI | Real-time console |
| Snowpipe | `COPY_HISTORY` + `PIPE_STATUS` | Snowflake alerts |
| Airflow | Airflow Web UI + task logs | Email on failure |
| ML Model | Evaluation metrics per training run | Metric drift detection |

---

## 🔮 Future Enhancements

- [ ] **Delta Lake** — Replace raw Parquet with Delta Lake for ACID transactions, time travel, and MERGE/UPSERT support
- [ ] **Azure Databricks** — Migrate PySpark jobs to Databricks for managed cluster scaling and Unity Catalog governance
- [ ] **Kafka Integration** — Replace file-based streaming with Apache Kafka for true event-driven architecture
- [ ] **dbt** — Add dbt for SQL-based transformations with built-in testing and documentation
- [ ] **Great Expectations** — Implement data quality gates between layers with automated schema validation
- [ ] **MLflow** — Track ML experiments, model versions, and deploy models as REST endpoints
- [ ] **CI/CD** — GitHub Actions pipeline for automated testing and deployment of ETL scripts
- [ ] **Terraform** — Replace ARM templates with Terraform for multi-cloud infrastructure management

---

## 📝 License

This project is developed for educational and analytical purposes as part of the IBRD Loan Management Efficiency study.

---

## 👥 Team

**Data Engineering Team** — Responsible for pipeline design, implementation, testing, and documentation.

**Created:** March 2026
