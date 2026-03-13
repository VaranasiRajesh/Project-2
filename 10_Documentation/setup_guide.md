# ============================================================================
# IBRD Loan Pipeline — Environment Setup Guide
# ============================================================================

## Prerequisites

| Tool              | Version   | Purpose                                |
|-------------------|-----------|----------------------------------------|
| Python            | 3.9+      | API ingestion script                   |
| Apache Spark      | 3.3+      | PySpark ETL and Streaming              |
| Java JDK          | 11 or 17  | Spark runtime dependency               |
| Azure CLI         | 2.40+     | ADF pipeline triggering                |
| Snowflake CLI     | Latest    | SQL script execution (optional)        |
| Apache Airflow    | 2.5+      | Pipeline orchestration                 |
| PowerBI Desktop   | Latest    | Dashboard development                  |

---

## Step 1: Python Environment Setup

```bash
# Create a virtual environment
python -m venv ibrd_env
source ibrd_env/bin/activate    # Linux/Mac
ibrd_env\Scripts\activate       # Windows

# Install dependencies
pip install requests pyspark==3.5.0 apache-airflow==2.7.0

# Verify PySpark
pyspark --version
```

---

## Step 2: Environment Variables

Set the following environment variables before running any pipeline scripts:

```bash
# Azure Data Lake Storage Gen2
export ADLS_ACCOUNT_NAME="your-adls-account-name"
export ADLS_CONTAINER="ibrd-datalake"
export ADLS_ACCOUNT_KEY="your-adls-account-key"

# World Bank API (optional — increases rate limits)
export WORLD_BANK_APP_TOKEN="your-app-token"

# Azure Data Factory
export ADF_FACTORY_NAME="adf-ibrd-loan-pipeline"
export AZURE_RESOURCE_GROUP="rg-ibrd-pipeline"
export AZURE_SUBSCRIPTION_ID="your-subscription-id"

# Snowflake
export SNOWFLAKE_ACCOUNT="your-account.snowflakecomputing.com"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_ROLE="IBRD_ENGINEER"
```

**Production Note:** Use Azure Key Vault or AWS Secrets Manager instead of
environment variables in production environments.

---

## Step 3: Azure Resources Setup

### 3.1 Create ADLS Gen2 Storage Account
```bash
az storage account create \
    --name ibrdstorageaccount \
    --resource-group rg-ibrd-pipeline \
    --location eastus \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hns true     # Hierarchical namespace (required for ADLS Gen2)
```

### 3.2 Create Container (Filesystem)
```bash
az storage fs create \
    --name ibrd-datalake \
    --account-name ibrdstorageaccount
```

### 3.3 Create Directory Structure
```bash
az storage fs directory create --name "3_ADLS_Data_Lake/bronze_layer" \
    --file-system ibrd-datalake --account-name ibrdstorageaccount
az storage fs directory create --name "3_ADLS_Data_Lake/silver_layer" \
    --file-system ibrd-datalake --account-name ibrdstorageaccount
az storage fs directory create --name "3_ADLS_Data_Lake/gold_layer" \
    --file-system ibrd-datalake --account-name ibrdstorageaccount
```

### 3.4 Deploy ADF Pipeline
```bash
az deployment group create \
    --resource-group rg-ibrd-pipeline \
    --template-file 2_ADF_Pipelines/ingest_api_to_adls.json \
    --parameters \
        factoryName=adf-ibrd-loan-pipeline \
        adlsAccountName=ibrdstorageaccount \
        adlsAccountKey=$ADLS_ACCOUNT_KEY
```

---

## Step 4: Snowflake Setup

```bash
# Connect to Snowflake and run DDL scripts
snowsql -a <account> -u <user> -f 6_Snowflake/tables/create_tables.sql
snowsql -a <account> -u <user> -f 6_Snowflake/snowpipe/create_snowpipe.sql
```

**Important:** After creating the Storage Integration, you must:
1. Run `DESC STORAGE INTEGRATION adls_ibrd_integration;` in Snowflake
2. Copy the `AZURE_CONSENT_URL` and `AZURE_MULTI_TENANT_APP_NAME`
3. Register the Snowflake service principal in Azure AD
4. Grant the principal `Storage Blob Data Contributor` role on the ADLS account

---

## Step 5: Running the Pipeline

### Individual Components
```bash
# 1. Fetch API data
python 1_Data_Source/world_bank_api/fetch_ibrd_loans.py

# 2. Trigger ADF pipeline (via Azure CLI)
az datafactory pipeline create-run \
    --factory-name adf-ibrd-loan-pipeline \
    --resource-group rg-ibrd-pipeline \
    --name pl_ingest_ibrd_api_to_adls_bronze

# 3. Bronze → Silver transformation
spark-submit 4_PySpark_ETL/bronze_to_silver/clean_loan_data.py

# 4. Silver → Gold transformation
spark-submit 4_PySpark_ETL/silver_to_gold/transform_loans.py

# 5. Start streaming monitor
spark-submit 5_Spark_Streaming/spark_stream_processing/loan_stream_processing.py

# 6. Train ML model
spark-submit 8_ML_Model/cancellation_predictor.py
```

### Full Pipeline via Airflow
```bash
# Initialize Airflow database
airflow db init

# Copy DAG to Airflow dags folder
cp 9_Orchestration/airflow_dags/loan_pipeline_dag.py ~/airflow/dags/

# Start Airflow scheduler and webserver
airflow scheduler -D
airflow webserver -D

# Trigger manual run
airflow dags trigger ibrd_loan_pipeline_daily
```

### Local Development (No ADLS)
```bash
# Use --local flag for local filesystem paths
spark-submit 4_PySpark_ETL/bronze_to_silver/clean_loan_data.py --local
spark-submit 4_PySpark_ETL/silver_to_gold/transform_loans.py --local
spark-submit 8_ML_Model/cancellation_predictor.py --local
```

---

## Step 6: PowerBI Dashboard Setup

1. Open PowerBI Desktop
2. **Get Data** → **Snowflake** connector
3. Enter server: `<account>.snowflakecomputing.com`
4. Database: `IBRD_LOAN_ANALYTICS`, Schema: `GOLD`
5. Select tables: `fact_loans`, `dim_country`, `dim_project`, `dim_loan_type`, `dim_borrower`
6. Verify relationships in the Model view
7. Import DAX measures from `7_PowerBI/dax_measures/dax_measures.dax`
8. Build visuals following `7_PowerBI/dashboard_template/dashboard_config.json`

---

## Troubleshooting

| Issue                               | Solution                                          |
|-------------------------------------|---------------------------------------------------|
| API returns 429 (rate limited)      | Set WORLD_BANK_APP_TOKEN env var                  |
| ADLS access denied                  | Check Service Principal permissions               |
| PySpark OOM error                   | Increase --driver-memory and --executor-memory    |
| Snowpipe not loading                | Run ALTER PIPE ... REFRESH; check Event Grid      |
| Airflow DAG not visible             | Check dags_folder in airflow.cfg                  |
| PowerBI can't connect to Snowflake  | Install Snowflake ODBC driver                     |
