# ============================================================================
# ADLS Gen2 Data Lake — Directory Structure & Schema Documentation
# ============================================================================
# This README documents the Medallion Architecture layout within ADLS Gen2.
# Container: ibrd-datalake
# Protocol:  abfss://<container>@<account>.dfs.core.windows.net/
# ============================================================================

## Directory Layout

```
3_ADLS_Data_Lake/
├── bronze_layer/                    ← RAW ingested data (immutable)
│   ├── loans_raw.csv               ← Batch: World Bank API full extract
│   ├── ibrd_loans_raw_YYYYMMDD.csv ← ADF timestamped copies
│   └── _quarantine/                ← Corrupt/malformed records
│       └── bad_records_YYYYMMDD.csv
│
├── silver_layer/                    ← CLEANED & STANDARDIZED data
│   └── loans_clean.parquet/        ← Partitioned Parquet
│       ├── country_code=IN/
│       ├── country_code=BR/
│       ├── country_code=NG/
│       └── ...
│
├── gold_layer/                      ← ANALYTICS-READY Star Schema
│   ├── fact_loans/                  ← Central fact table (Parquet)
│   ├── dim_country/                 ← Country dimension
│   ├── dim_project/                 ← Project dimension
│   ├── dim_loan_type/               ← Loan type dimension
│   └── dim_borrower/                ← Borrower dimension
│
└── README.md                        ← This file
```

## Layer Descriptions

### Bronze Layer (Raw Zone)
- **Format:** CSV
- **Quality:** As-is from source — no transformations applied
- **Retention:** 90 days (configurable via Azure Lifecycle Policy)
- **Access:** Write by ADF/Ingestion scripts; Read by PySpark ETL

### Silver Layer (Cleansed Zone)
- **Format:** Parquet (Snappy compression)
- **Quality:** Standardized columns, parsed dates, nulls handled, deduplicated
- **Partitioning:** By `country_code`
- **Access:** Write by Bronze→Silver ETL; Read by Silver→Gold ETL

### Gold Layer (Curated Zone)
- **Format:** Parquet (Snappy compression)
- **Quality:** Star Schema — fully enriched, aggregation-ready
- **Access:** Read by Snowpipe, PowerBI, Analysts
- **Auto-Ingest:** Snowpipe monitors this directory for new files
