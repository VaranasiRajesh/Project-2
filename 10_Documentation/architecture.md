# ============================================================================
# IBRD Loan Pipeline — Project Architecture & Documentation
# ============================================================================
# Enhancing Loan Management Efficiency for the International Bank
# for Reconstruction and Development (IBRD)
# ============================================================================

## 1. Executive Summary

This project implements a **production-grade data engineering pipeline** using
the **Medallion Architecture** (Bronze → Silver → Gold) to process IBRD
Statement of Loans data. The pipeline supports both **batch** and **real-time
streaming** processing, loads analytics-ready data into **Snowflake** via
**Snowpipe**, and powers **PowerBI dashboards** for loan management insights.

A **PySpark MLlib** model predicts loan cancellation risk, enabling proactive
intervention by IBRD loan officers.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │ World Bank   │    │ CSV/JSON     │    │ Real-Time CSV Stream     │  │
│  │ SODA API     │    │ (Local)      │    │ (Spark Streaming)        │  │
│  └──────┬───────┘    └──────┬───────┘    └────────────┬─────────────┘  │
└─────────┼──────────────────┼──────────────────────────┼────────────────┘
          │                  │                          │
          ▼                  ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                                     │
│  ┌──────────────────────┐    ┌──────────────────────────────────────┐  │
│  │ fetch_ibrd_loans.py  │    │ loan_stream_processing.py           │  │
│  │ (Batch - Python)     │    │ (Streaming - PySpark)               │  │
│  └──────────┬───────────┘    └─────────────────────────────────────┘  │
└─────────────┼─────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  AZURE DATA FACTORY                                     │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ pl_ingest_ibrd_api_to_adls_bronze                               │  │
│  │ Copy Data: HTTP (World Bank API) → ADLS Gen2 Bronze Layer       │  │
│  └──────────────────────────┬───────────────────────────────────────┘  │
└─────────────────────────────┼─────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              ADLS GEN2 DATA LAKE (Medallion Architecture)               │
│                                                                         │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────────┐    │
│  │  BRONZE LAYER  │───▶│  SILVER LAYER  │───▶│    GOLD LAYER      │    │
│  │  (Raw CSV)     │    │  (Clean Parq.) │    │  (Star Schema)     │    │
│  │                │    │                │    │  ┌──────────────┐  │    │
│  │ loans_raw.csv  │    │ loans_clean    │    │  │ fact_loans   │  │    │
│  │                │    │ .parquet/      │    │  │ dim_country  │  │    │
│  │                │    │ country_code=  │    │  │ dim_project  │  │    │
│  │                │    │                │    │  │ dim_loan_type│  │    │
│  │                │    │                │    │  │ dim_borrower │  │    │
│  │                │    │                │    │  └──────────────┘  │    │
│  └────────────────┘    └────────────────┘    └────────┬───────────┘    │
│       PySpark ETL           PySpark ETL              │                 │
│    clean_loan_data.py    transform_loans.py          │                 │
└──────────────────────────────────────────────────────┼─────────────────┘
                                                       │
                              ┌─────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      SNOWFLAKE                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ Snowpipe AUTO_INGEST (Event Grid triggered)                     │  │
│  │ Database: IBRD_LOAN_ANALYTICS | Schema: GOLD                    │  │
│  │ Tables: fact_loans, dim_country, dim_project, dim_loan_type,    │  │
│  │         dim_borrower                                             │  │
│  └──────────────────────────┬───────────────────────────────────────┘  │
└─────────────────────────────┼─────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  ANALYTICS & ML                                         │
│  ┌──────────────────────┐         ┌─────────────────────────────────┐  │
│  │ PowerBI Dashboard    │         │ PySpark MLlib                   │  │
│  │ (4-page report)      │         │ cancellation_predictor.py       │  │
│  │ DAX Measures         │         │ Random Forest + Cross-Val       │  │
│  └──────────────────────┘         └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Technology Stack

| Component         | Technology                  | Purpose                        |
|-------------------|-----------------------------|--------------------------------|
| Data Ingestion    | Python `requests`, PySpark  | API calls, file/stream reads   |
| Cloud Storage     | Azure Data Lake Gen2 (ADLS) | Medallion Architecture layers  |
| Orchestration     | Azure Data Factory + Airflow| Pipeline scheduling & triggers |
| Batch ETL         | PySpark (Databricks/YARN)   | Bronze→Silver→Gold transforms  |
| Streaming         | Spark Structured Streaming  | Real-time risk monitoring      |
| Data Warehouse    | Snowflake                   | Star Schema analytics          |
| Data Loading      | Snowpipe (AUTO_INGEST)      | Event-driven data ingestion    |
| Visualization     | PowerBI                     | Executive & analyst dashboards |
| Machine Learning  | PySpark MLlib               | Cancellation risk prediction   |

---

## 4. Directory Structure

```
project-2/
├── 1_Data_Source/
│   └── world_bank_api/
│       └── fetch_ibrd_loans.py          ← API ingestion script
│
├── 2_ADF_Pipelines/
│   └── ingest_api_to_adls.json          ← ADF ARM template
│
├── 3_ADLS_Data_Lake/
│   ├── README.md                         ← Layer documentation
│   ├── schema_manifest.json              ← Full schema definitions
│   ├── bronze_layer/                     ← Raw CSV data
│   ├── silver_layer/                     ← Cleaned Parquet
│   └── gold_layer/                       ← Star Schema Parquet
│
├── 4_PySpark_ETL/
│   ├── bronze_to_silver/
│   │   └── clean_loan_data.py           ← Data cleansing pipeline
│   └── silver_to_gold/
│       └── transform_loans.py           ← Star Schema builder
│
├── 5_Spark_Streaming/
│   ├── spark_stream_processing/
│   │   └── loan_stream_processing.py    ← Structured Streaming
│   ├── streaming_input/                  ← CSV drop zone
│   └── checkpoints/                      ← Stream checkpoints
│
├── 6_Snowflake/
│   ├── tables/
│   │   └── create_tables.sql            ← Star Schema DDL
│   └── snowpipe/
│       └── create_snowpipe.sql          ← Auto-ingest setup
│
├── 7_PowerBI/
│   ├── dax_measures/
│   │   └── dax_measures.dax             ← DAX calculation measures
│   └── dashboard_template/
│       └── dashboard_config.json        ← Dashboard layout blueprint
│
├── 8_ML_Model/
│   ├── cancellation_predictor.py        ← ML training pipeline
│   ├── model_metadata.json              ← Model card / documentation
│   └── trained_model/                    ← Saved PipelineModel
│
├── 9_Orchestration/
│   └── airflow_dags/
│       └── loan_pipeline_dag.py         ← Airflow DAG
│
└── 10_Documentation/
    ├── architecture.md                   ← This file
    ├── data_dictionary.md                ← Column-level documentation
    └── setup_guide.md                    ← Environment setup instructions
```

---

## 5. Data Flow Summary

| Step | Source               | Process                    | Output              | Tool            |
|------|----------------------|----------------------------|----------------------|-----------------|
| 1    | World Bank API       | Paginated fetch + CSV save | ibrd_loans_1M.csv   | Python/requests |
| 2    | Local CSV            | ADF Copy to ADLS           | Bronze: loans_raw   | Azure Data Factory |
| 3    | Bronze CSV           | Clean, standardize, dedup  | Silver: loans_clean | PySpark ETL     |
| 4    | Silver Parquet       | Star Schema transformation | Gold: 5 tables      | PySpark ETL     |
| 5    | Gold Parquet (ADLS)  | Auto-ingest via Snowpipe   | Snowflake tables    | Snowpipe        |
| 6    | Snowflake tables     | DAX measures + visuals     | Dashboard           | PowerBI         |
| 7    | Gold Parquet         | ML feature engineering     | Risk predictions    | PySpark MLlib   |

---

## 6. Security Considerations

- **Credentials**: All secrets stored in environment variables (never hard-coded)
- **ADLS Authentication**: Azure AD Service Principal (OAuth2) for production
- **Snowflake RBAC**: IBRD_ANALYST (read-only) and IBRD_ENGINEER (full) roles
- **Snowpipe**: Storage Integration uses Azure Event Grid (no shared keys)
- **Key Vault**: Compatible with Azure Key Vault / AWS Secrets Manager
- **Network**: ADLS firewall rules and Snowflake network policies recommended

---

## 7. Monitoring & Alerting

| Component        | Monitoring Method                         |
|------------------|-------------------------------------------|
| API Ingestion    | Python logging (console + file)           |
| ADF Pipeline     | Azure Monitor + ADF activity logs         |
| PySpark ETL      | Spark UI + structured logging             |
| Streaming        | Console output + Spark Streaming UI       |
| Snowpipe         | COPY_HISTORY + PIPE_STATUS functions      |
| Airflow          | Airflow UI + email alerts on failure      |
| ML Model         | Evaluation metrics logged per training run|
