-- ============================================================================
-- IBRD Loan Pipeline — Snowflake Star Schema DDL
-- ============================================================================
-- File    : create_tables.sql
-- Purpose : Creates the Star Schema tables in Snowflake to receive Gold layer
--           data from ADLS Gen2 via Snowpipe. Includes dimension tables
--           (dim_country, dim_project, dim_loan_type, dim_borrower) and the
--           central fact table (fact_loans).
-- Author  : Data Engineering Team
-- Created : 2026-03-13
-- ============================================================================

-- ──────────────────────────────────────────────────────────────────────────────
-- DATABASE & SCHEMA SETUP
-- ──────────────────────────────────────────────────────────────────────────────

-- Create a dedicated database for the IBRD loan analytics project
CREATE DATABASE IF NOT EXISTS IBRD_LOAN_ANALYTICS
    COMMENT = 'IBRD Loan Management Analytics — Gold Layer Star Schema';

-- Use the newly created database
USE DATABASE IBRD_LOAN_ANALYTICS;

-- Create a schema for the Gold layer star schema tables
CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Gold layer — Star Schema for analytical queries and PowerBI';

-- Set the active schema
USE SCHEMA GOLD;

-- ──────────────────────────────────────────────────────────────────────────────
-- WAREHOUSE CONFIGURATION
-- ──────────────────────────────────────────────────────────────────────────────

-- Create a compute warehouse for ETL loads (auto-suspend to save credits)
CREATE WAREHOUSE IF NOT EXISTS WH_IBRD_ETL
    WAREHOUSE_SIZE   = 'MEDIUM'
    AUTO_SUSPEND     = 120           -- Suspend after 2 minutes of inactivity
    AUTO_RESUME      = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3            -- Enable multi-cluster for concurrency
    SCALING_POLICY   = 'ECONOMY'     -- Prioritize cost over speed
    COMMENT          = 'ETL warehouse for IBRD loan data loading';

USE WAREHOUSE WH_IBRD_ETL;


-- ══════════════════════════════════════════════════════════════════════════════
-- DIMENSION TABLE: dim_country
-- ══════════════════════════════════════════════════════════════════════════════
-- Contains one row per unique country with geographic attributes.
-- country_code is the natural key (ISO); country_key is the surrogate key.

CREATE OR REPLACE TABLE dim_country (
    -- Surrogate key (populated from PySpark monotonically_increasing_id)
    country_key             BIGINT          NOT NULL    PRIMARY KEY,

    -- Natural key — ISO country code (e.g., 'IN', 'BR', 'NG')
    country_code            VARCHAR(10)     NOT NULL,

    -- Full country name
    country                 VARCHAR(200)    NOT NULL    DEFAULT 'Unknown',

    -- Geographic region (e.g., 'South Asia', 'Latin America and Caribbean')
    region                  VARCHAR(200)    NOT NULL    DEFAULT 'Unknown',

    -- Guarantor information
    guarantor_country_code  VARCHAR(10)     DEFAULT 'N/A',
    guarantor               VARCHAR(200)    DEFAULT 'N/A',

    -- Audit columns
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500)
)
COMMENT = 'Dimension: Country — geographic and guarantor attributes'
CLUSTER BY (region);    -- Cluster by region for efficient regional queries


-- ══════════════════════════════════════════════════════════════════════════════
-- DIMENSION TABLE: dim_project
-- ══════════════════════════════════════════════════════════════════════════════
-- Contains one row per unique World Bank project.

CREATE OR REPLACE TABLE dim_project (
    -- Surrogate key
    project_key             BIGINT          NOT NULL    PRIMARY KEY,

    -- Natural key — World Bank project identifier (e.g., 'P000004')
    project_id              VARCHAR(50)     NOT NULL,

    -- Human-readable project name
    project_name            VARCHAR(500)    NOT NULL    DEFAULT 'Unknown Project',

    -- Audit columns
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500)
)
COMMENT = 'Dimension: Project — World Bank project identifiers and names';


-- ══════════════════════════════════════════════════════════════════════════════
-- DIMENSION TABLE: dim_loan_type
-- ══════════════════════════════════════════════════════════════════════════════
-- Contains one row per unique combination of loan type, status, and currency.

CREATE OR REPLACE TABLE dim_loan_type (
    -- Surrogate key
    loan_type_key           BIGINT          NOT NULL    PRIMARY KEY,

    -- Loan classification (e.g., 'IBRD', 'IDA', 'Blend')
    loan_type               VARCHAR(50)     NOT NULL    DEFAULT 'Unknown',

    -- Current loan status (e.g., 'Approved', 'Disbursing', 'Repaying', 'Fully Repaid')
    loan_status             VARCHAR(100)    NOT NULL    DEFAULT 'Unknown',

    -- Currency of the loan commitment (usually 'USD')
    currency_of_commitment  VARCHAR(10)     NOT NULL    DEFAULT 'USD',

    -- Audit columns
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500)
)
COMMENT = 'Dimension: Loan Type — classification, status, and currency';


-- ══════════════════════════════════════════════════════════════════════════════
-- DIMENSION TABLE: dim_borrower
-- ══════════════════════════════════════════════════════════════════════════════
-- Contains one row per unique borrowing entity.

CREATE OR REPLACE TABLE dim_borrower (
    -- Surrogate key
    borrower_key            BIGINT          NOT NULL    PRIMARY KEY,

    -- Borrower entity name (e.g., government ministry, state bank)
    borrower                VARCHAR(500)    NOT NULL    DEFAULT 'Unknown Borrower',

    -- Country code of the borrower (denormalized for convenience)
    country_code            VARCHAR(10)     DEFAULT 'N/A',

    -- Audit columns
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500)
)
COMMENT = 'Dimension: Borrower — borrowing entities and their countries';


-- ══════════════════════════════════════════════════════════════════════════════
-- FACT TABLE: fact_loans
-- ══════════════════════════════════════════════════════════════════════════════
-- Central fact table containing all measurable loan metrics with foreign keys
-- referencing each dimension. This is the primary table for PowerBI dashboards
-- and analytical queries.

CREATE OR REPLACE TABLE fact_loans (
    -- ── Surrogate Key ───────────────────────────────────────────────────
    loan_fact_key               BIGINT          NOT NULL    PRIMARY KEY,

    -- ── Foreign Keys (referencing dimensions) ───────────────────────────
    country_key                 BIGINT          REFERENCES dim_country(country_key),
    project_key                 BIGINT          REFERENCES dim_project(project_key),
    loan_type_key               BIGINT          REFERENCES dim_loan_type(loan_type_key),
    borrower_key                BIGINT          REFERENCES dim_borrower(borrower_key),

    -- ── Degenerate Dimensions (kept in fact for drill-through) ──────────
    loan_number                 VARCHAR(50),
    end_of_period               DATE,

    -- ── Date Keys (for time-based analysis & joins to date dimension) ───
    board_approval_date         DATE,
    effective_date_most_recent  DATE,
    closed_date_most_recent     DATE,
    agreement_signing_date      DATE,
    first_repayment_date        DATE,
    last_repayment_date         DATE,
    last_disbursement_date      DATE,

    -- ── Measures (USD amounts) ──────────────────────────────────────────
    original_principal_amount   NUMBER(18, 2)   DEFAULT 0.00,
    cancelled_amount            NUMBER(18, 2)   DEFAULT 0.00,
    undisbursed_amount          NUMBER(18, 2)   DEFAULT 0.00,
    disbursed_amount            NUMBER(18, 2)   DEFAULT 0.00,
    repaid_to_ibrd              NUMBER(18, 2)   DEFAULT 0.00,
    due_to_ibrd                 NUMBER(18, 2)   DEFAULT 0.00,
    borrowers_obligation        NUMBER(18, 2)   DEFAULT 0.00,
    sold_3rd_party              NUMBER(18, 2)   DEFAULT 0.00,
    repaid_3rd_party            NUMBER(18, 2)   DEFAULT 0.00,
    due_3rd_party               NUMBER(18, 2)   DEFAULT 0.00,
    loans_held                  NUMBER(18, 2)   DEFAULT 0.00,

    -- ── Rates ───────────────────────────────────────────────────────────
    interest_rate               NUMBER(8, 4)    DEFAULT 0.0000,
    service_charge_rate         NUMBER(8, 4)    DEFAULT 0.0000,

    -- ── Audit Columns ───────────────────────────────────────────────────
    _loaded_at                  TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file                VARCHAR(500)
)
COMMENT = 'Fact: Loans — central fact table with all loan metrics and dimension FKs'
CLUSTER BY (country_key, board_approval_date);  -- Optimize for common query patterns


-- ══════════════════════════════════════════════════════════════════════════════
-- INDEXES & SEARCH OPTIMIZATION
-- ══════════════════════════════════════════════════════════════════════════════

-- Enable search optimization on the fact table for faster point lookups
ALTER TABLE fact_loans ADD SEARCH OPTIMIZATION
    ON EQUALITY(loan_number, country_key, project_key);

-- ══════════════════════════════════════════════════════════════════════════════
-- GRANTS (Role-Based Access Control)
-- ══════════════════════════════════════════════════════════════════════════════

-- Create roles for different access levels
CREATE ROLE IF NOT EXISTS IBRD_ANALYST;
CREATE ROLE IF NOT EXISTS IBRD_ENGINEER;

-- Analysts: READ-ONLY access to all Gold tables
GRANT USAGE ON DATABASE IBRD_LOAN_ANALYTICS TO ROLE IBRD_ANALYST;
GRANT USAGE ON SCHEMA GOLD TO ROLE IBRD_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE IBRD_ANALYST;

-- Engineers: FULL access (for ETL operations)
GRANT USAGE ON DATABASE IBRD_LOAN_ANALYTICS TO ROLE IBRD_ENGINEER;
GRANT USAGE ON SCHEMA GOLD TO ROLE IBRD_ENGINEER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA GOLD TO ROLE IBRD_ENGINEER;
GRANT USAGE ON WAREHOUSE WH_IBRD_ETL TO ROLE IBRD_ENGINEER;

-- ══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES
-- ══════════════════════════════════════════════════════════════════════════════

-- Verify all tables were created successfully
SHOW TABLES IN SCHEMA GOLD;

-- Display table structures
DESCRIBE TABLE dim_country;
DESCRIBE TABLE dim_project;
DESCRIBE TABLE dim_loan_type;
DESCRIBE TABLE dim_borrower;
DESCRIBE TABLE fact_loans;
