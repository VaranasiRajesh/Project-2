-- ============================================================================
-- IBRD Loan Pipeline — Snowpipe Auto-Ingest Configuration
-- ============================================================================
-- File    : create_snowpipe.sql
-- Purpose : Creates an External Stage pointing to ADLS Gen2 Gold layer,
--           a file format for Parquet files, and a Snowpipe with AUTO_INGEST
--           enabled that automatically loads new Parquet files into the
--           fact_loans table when they land in ADLS.
-- Author  : Data Engineering Team
-- Created : 2026-03-13
-- ============================================================================

-- Use the IBRD analytics database and Gold schema
USE DATABASE IBRD_LOAN_ANALYTICS;
USE SCHEMA GOLD;
USE WAREHOUSE WH_IBRD_ETL;


-- ══════════════════════════════════════════════════════════════════════════════
-- STEP 1: STORAGE INTEGRATION (Azure Service Principal)
-- ══════════════════════════════════════════════════════════════════════════════
-- A Storage Integration securely connects Snowflake to ADLS Gen2 using
-- Azure AD Service Principal OAuth2 authentication (no shared keys).
-- NOTE: This must be created by an ACCOUNTADMIN role.

CREATE OR REPLACE STORAGE INTEGRATION adls_ibrd_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'AZURE'
    ENABLED                   = TRUE

    -- Azure AD tenant ID for the service principal
    AZURE_TENANT_ID           = '<YOUR_AZURE_TENANT_ID>'

    -- ADLS Gen2 locations Snowflake is allowed to access
    STORAGE_ALLOWED_LOCATIONS = (
        'azure://<YOUR_ADLS_ACCOUNT>.blob.core.windows.net/ibrd-datalake/3_ADLS_Data_Lake/gold_layer/'
    )

    COMMENT = 'Storage integration for ADLS Gen2 Gold layer access (IBRD pipeline)';

-- After creating the integration, run this to get the Azure consent URL
-- and Snowflake service principal details for Azure AD app registration:
-- DESC STORAGE INTEGRATION adls_ibrd_integration;


-- ══════════════════════════════════════════════════════════════════════════════
-- STEP 2: PARQUET FILE FORMAT
-- ══════════════════════════════════════════════════════════════════════════════
-- Define a named file format for Parquet files produced by the PySpark
-- Gold layer transformation (Snappy compression).

CREATE OR REPLACE FILE FORMAT ff_parquet_snappy
    TYPE                = 'PARQUET'
    COMPRESSION         = 'SNAPPY'         -- Match PySpark output compression
    BINARY_AS_TEXT      = FALSE
    TRIM_SPACE          = TRUE
    COMMENT             = 'Parquet file format with Snappy compression for Gold layer data';


-- ══════════════════════════════════════════════════════════════════════════════
-- STEP 3: EXTERNAL STAGE (ADLS Gen2 → Snowflake)
-- ══════════════════════════════════════════════════════════════════════════════
-- The External Stage maps to the ADLS Gen2 Gold layer directory where
-- PySpark writes the fact_loans Parquet files.

CREATE OR REPLACE STAGE stg_adls_gold_layer
    STORAGE_INTEGRATION = adls_ibrd_integration

    -- ADLS Gen2 path to the Gold layer fact_loans directory
    URL = 'azure://<YOUR_ADLS_ACCOUNT>.blob.core.windows.net/ibrd-datalake/3_ADLS_Data_Lake/gold_layer/fact_loans/'

    FILE_FORMAT = ff_parquet_snappy

    COMMENT = 'External stage pointing to ADLS Gen2 Gold layer — fact_loans Parquet files';

-- Verify the stage can list files (run after granting Azure AD consent):
-- LIST @stg_adls_gold_layer;


-- ══════════════════════════════════════════════════════════════════════════════
-- STEP 4: NOTIFICATION INTEGRATION (Azure Event Grid)
-- ══════════════════════════════════════════════════════════════════════════════
-- For AUTO_INGEST to work with ADLS Gen2, Snowflake uses Azure Event Grid
-- notifications. This integration must be created by ACCOUNTADMIN.

CREATE OR REPLACE NOTIFICATION INTEGRATION ni_adls_gold_events
    ENABLED             = TRUE
    TYPE                = QUEUE
    NOTIFICATION_PROVIDER = AZURE_EVENT_GRID

    -- Azure Event Grid subscription endpoint
    AZURE_STORAGE_QUEUE_PRIMARY_URI =
        'https://<YOUR_ADLS_ACCOUNT>.queue.core.windows.net/snowpipe-notifications'

    AZURE_TENANT_ID     = '<YOUR_AZURE_TENANT_ID>'

    COMMENT = 'Event Grid notification integration for Snowpipe auto-ingest from ADLS';


-- ══════════════════════════════════════════════════════════════════════════════
-- STEP 5: SNOWPIPE — AUTO-INGEST INTO fact_loans
-- ══════════════════════════════════════════════════════════════════════════════
-- Snowpipe automatically detects new Parquet files landing in the ADLS Gold
-- layer and loads them into the fact_loans table using COPY INTO.
-- AUTO_INGEST = TRUE enables event-driven loading (no manual triggers).

CREATE OR REPLACE PIPE pipe_fact_loans_auto_ingest
    AUTO_INGEST = TRUE
    INTEGRATION = 'NI_ADLS_GOLD_EVENTS'
    COMMENT     = 'Auto-ingest pipe: loads new Parquet files from ADLS Gold layer into fact_loans'
AS
    COPY INTO fact_loans (
        -- ── Surrogate Key ───────────────────────────────────────────────
        loan_fact_key,
        -- ── Foreign Keys ────────────────────────────────────────────────
        country_key,
        project_key,
        loan_type_key,
        borrower_key,
        -- ── Degenerate Dimensions ───────────────────────────────────────
        loan_number,
        end_of_period,
        -- ── Date Keys ───────────────────────────────────────────────────
        board_approval_date,
        effective_date_most_recent,
        closed_date_most_recent,
        agreement_signing_date,
        first_repayment_date,
        last_repayment_date,
        last_disbursement_date,
        -- ── Measures ────────────────────────────────────────────────────
        original_principal_amount,
        cancelled_amount,
        undisbursed_amount,
        disbursed_amount,
        repaid_to_ibrd,
        due_to_ibrd,
        borrowers_obligation,
        sold_3rd_party,
        repaid_3rd_party,
        due_3rd_party,
        loans_held,
        -- ── Rates ───────────────────────────────────────────────────────
        interest_rate,
        service_charge_rate,
        -- ── Audit ───────────────────────────────────────────────────────
        _loaded_at,
        _source_file
    )
    FROM (
        SELECT
            -- Map Parquet columns to table columns using $1 notation
            $1:loan_fact_key::BIGINT,
            $1:country_key::BIGINT,
            $1:project_key::BIGINT,
            $1:loan_type_key::BIGINT,
            $1:borrower_key::BIGINT,
            $1:loan_number::VARCHAR(50),
            $1:end_of_period::DATE,
            $1:board_approval_date::DATE,
            $1:effective_date_most_recent::DATE,
            $1:closed_date_most_recent::DATE,
            $1:agreement_signing_date::DATE,
            $1:first_repayment_date::DATE,
            $1:last_repayment_date::DATE,
            $1:last_disbursement_date::DATE,
            $1:original_principal_amount::NUMBER(18,2),
            $1:cancelled_amount::NUMBER(18,2),
            $1:undisbursed_amount::NUMBER(18,2),
            $1:disbursed_amount::NUMBER(18,2),
            $1:repaid_to_ibrd::NUMBER(18,2),
            $1:due_to_ibrd::NUMBER(18,2),
            $1:borrowers_obligation::NUMBER(18,2),
            $1:sold_3rd_party::NUMBER(18,2),
            $1:repaid_3rd_party::NUMBER(18,2),
            $1:due_3rd_party::NUMBER(18,2),
            $1:loans_held::NUMBER(18,2),
            $1:interest_rate::NUMBER(8,4),
            $1:service_charge_rate::NUMBER(8,4),
            CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,     -- _loaded_at
            METADATA$FILENAME::VARCHAR(500)          -- _source_file
        FROM @stg_adls_gold_layer
    )
    FILE_FORMAT  = ff_parquet_snappy
    ON_ERROR     = 'SKIP_FILE'           -- Skip corrupt files, continue loading
    PURGE        = FALSE;                -- Retain source files after loading


-- ══════════════════════════════════════════════════════════════════════════════
-- STEP 6: ADDITIONAL SNOWPIPES FOR DIMENSION TABLES
-- ══════════════════════════════════════════════════════════════════════════════

-- Stage for dim_country
CREATE OR REPLACE STAGE stg_adls_dim_country
    STORAGE_INTEGRATION = adls_ibrd_integration
    URL = 'azure://<YOUR_ADLS_ACCOUNT>.blob.core.windows.net/ibrd-datalake/3_ADLS_Data_Lake/gold_layer/dim_country/'
    FILE_FORMAT = ff_parquet_snappy;

CREATE OR REPLACE PIPE pipe_dim_country_auto_ingest
    AUTO_INGEST = TRUE
    INTEGRATION = 'NI_ADLS_GOLD_EVENTS'
AS
    COPY INTO dim_country (country_key, country_code, country, region,
                           guarantor_country_code, guarantor, _loaded_at, _source_file)
    FROM (
        SELECT $1:country_key::BIGINT, $1:country_code::VARCHAR(10),
               $1:country::VARCHAR(200), $1:region::VARCHAR(200),
               $1:guarantor_country_code::VARCHAR(10), $1:guarantor::VARCHAR(200),
               CURRENT_TIMESTAMP(), METADATA$FILENAME
        FROM @stg_adls_dim_country
    )
    FILE_FORMAT = ff_parquet_snappy
    ON_ERROR = 'SKIP_FILE';

-- Stage for dim_project
CREATE OR REPLACE STAGE stg_adls_dim_project
    STORAGE_INTEGRATION = adls_ibrd_integration
    URL = 'azure://<YOUR_ADLS_ACCOUNT>.blob.core.windows.net/ibrd-datalake/3_ADLS_Data_Lake/gold_layer/dim_project/'
    FILE_FORMAT = ff_parquet_snappy;

CREATE OR REPLACE PIPE pipe_dim_project_auto_ingest
    AUTO_INGEST = TRUE
    INTEGRATION = 'NI_ADLS_GOLD_EVENTS'
AS
    COPY INTO dim_project (project_key, project_id, project_name,
                           _loaded_at, _source_file)
    FROM (
        SELECT $1:project_key::BIGINT, $1:project_id::VARCHAR(50),
               $1:project_name::VARCHAR(500),
               CURRENT_TIMESTAMP(), METADATA$FILENAME
        FROM @stg_adls_dim_project
    )
    FILE_FORMAT = ff_parquet_snappy
    ON_ERROR = 'SKIP_FILE';

-- Stage for dim_loan_type
CREATE OR REPLACE STAGE stg_adls_dim_loan_type
    STORAGE_INTEGRATION = adls_ibrd_integration
    URL = 'azure://<YOUR_ADLS_ACCOUNT>.blob.core.windows.net/ibrd-datalake/3_ADLS_Data_Lake/gold_layer/dim_loan_type/'
    FILE_FORMAT = ff_parquet_snappy;

CREATE OR REPLACE PIPE pipe_dim_loan_type_auto_ingest
    AUTO_INGEST = TRUE
    INTEGRATION = 'NI_ADLS_GOLD_EVENTS'
AS
    COPY INTO dim_loan_type (loan_type_key, loan_type, loan_status,
                             currency_of_commitment, _loaded_at, _source_file)
    FROM (
        SELECT $1:loan_type_key::BIGINT, $1:loan_type::VARCHAR(50),
               $1:loan_status::VARCHAR(100), $1:currency_of_commitment::VARCHAR(10),
               CURRENT_TIMESTAMP(), METADATA$FILENAME
        FROM @stg_adls_dim_loan_type
    )
    FILE_FORMAT = ff_parquet_snappy
    ON_ERROR = 'SKIP_FILE';

-- Stage for dim_borrower
CREATE OR REPLACE STAGE stg_adls_dim_borrower
    STORAGE_INTEGRATION = adls_ibrd_integration
    URL = 'azure://<YOUR_ADLS_ACCOUNT>.blob.core.windows.net/ibrd-datalake/3_ADLS_Data_Lake/gold_layer/dim_borrower/'
    FILE_FORMAT = ff_parquet_snappy;

CREATE OR REPLACE PIPE pipe_dim_borrower_auto_ingest
    AUTO_INGEST = TRUE
    INTEGRATION = 'NI_ADLS_GOLD_EVENTS'
AS
    COPY INTO dim_borrower (borrower_key, borrower, country_code,
                            _loaded_at, _source_file)
    FROM (
        SELECT $1:borrower_key::BIGINT, $1:borrower::VARCHAR(500),
               $1:country_code::VARCHAR(10),
               CURRENT_TIMESTAMP(), METADATA$FILENAME
        FROM @stg_adls_dim_borrower
    )
    FILE_FORMAT = ff_parquet_snappy
    ON_ERROR = 'SKIP_FILE';


-- ══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION & MONITORING
-- ══════════════════════════════════════════════════════════════════════════════

-- Check pipe status (run after setup)
SELECT SYSTEM$PIPE_STATUS('pipe_fact_loans_auto_ingest');
SELECT SYSTEM$PIPE_STATUS('pipe_dim_country_auto_ingest');
SELECT SYSTEM$PIPE_STATUS('pipe_dim_project_auto_ingest');
SELECT SYSTEM$PIPE_STATUS('pipe_dim_loan_type_auto_ingest');
SELECT SYSTEM$PIPE_STATUS('pipe_dim_borrower_auto_ingest');

-- View recent Snowpipe load history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME       => 'FACT_LOANS',
    START_TIME       => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC
LIMIT 20;

-- Show all pipes in the schema
SHOW PIPES IN SCHEMA GOLD;

-- Manually refresh a pipe (to catch any missed files)
-- ALTER PIPE pipe_fact_loans_auto_ingest REFRESH;
