# ============================================================================
# IBRD Loan Pipeline — Data Dictionary
# ============================================================================
# Complete column-level documentation for all layers of the pipeline.
# ============================================================================

## Bronze Layer — Raw Columns (As-Received from API)

| # | Column Name                  | Source Type | Description                                    |
|---|------------------------------|-------------|------------------------------------------------|
| 1 | End of Period                | String      | Reporting period end date                      |
| 2 | Loan Number                  | String      | Unique IBRD loan identifier                    |
| 3 | Region                       | String      | Geographic region (e.g., South Asia)           |
| 4 | Country Code                 | String      | ISO 2-letter country code                      |
| 5 | Country                      | String      | Full country name                              |
| 6 | Borrower                     | String      | Borrowing entity name                          |
| 7 | Guarantor Country Code       | String      | Guarantor country ISO code                     |
| 8 | Guarantor                    | String      | Guarantor entity name                          |
| 9 | Loan Type                    | String      | Loan classification (IBRD, IDA, Blend)         |
|10 | Loan Status                  | String      | Current status (Approved, Disbursing, etc.)    |
|11 | Interest Rate                | String/Num  | Annual interest rate (%)                       |
|12 | Service Charge Rate          | String/Num  | Service charge rate (%)                        |
|13 | Currency of Commitment       | String      | Commitment currency (usually USD)              |
|14 | Project ID                   | String      | World Bank project identifier                  |
|15 | Project Name                 | String      | Human-readable project name                    |
|16 | Original Principal Amount    | String/Num  | Original loan amount (USD)                     |
|17 | Cancelled Amount             | String/Num  | Amount cancelled (USD)                         |
|18 | Undisbursed Amount           | String/Num  | Amount not yet disbursed (USD)                 |
|19 | Disbursed Amount             | String/Num  | Amount disbursed (USD)                         |
|20 | Repaid to IBRD               | String/Num  | Amount repaid to IBRD (USD)                    |
|21 | Due to IBRD                  | String/Num  | Outstanding amount due (USD)                   |
|22 | Borrower's Obligation        | String/Num  | Borrower's outstanding obligation (USD)        |
|23 | Sold 3rd Party               | String/Num  | Amount sold to third party (USD)               |
|24 | Repaid 3rd Party             | String/Num  | Amount repaid to third party (USD)             |
|25 | Due 3rd Party                | String/Num  | Amount due to third party (USD)                |
|26 | Loans Held                   | String/Num  | Loans held amount (USD)                        |
|27 | First Repayment Date         | String      | Scheduled first repayment date                 |
|28 | Last Repayment Date          | String      | Scheduled last repayment date                  |
|29 | Agreement Signing Date       | String      | Date of loan agreement signing                 |
|30 | Board Approval Date          | String      | Date of board approval                         |
|31 | Effective Date (Most Recent) | String      | Most recent effective date                     |
|32 | Closed Date (Most Recent)    | String      | Most recent closure date                       |
|33 | Last Disbursement Date       | String      | Date of last disbursement                      |

---

## Silver Layer — Cleaned Columns

| Column (snake_case)           | Type     | Null Handling        | Notes                    |
|-------------------------------|----------|----------------------|--------------------------|
| end_of_period                 | Date     | —                    | Multi-format parsed      |
| loan_number                   | String   | —                    | Natural key              |
| region                        | String   | 'Unknown'            | —                        |
| country_code                  | String   | 'Unknown'            | Partition column         |
| country                       | String   | 'Unknown'            | —                        |
| borrower                      | String   | 'Unknown'            | —                        |
| guarantor_country_code        | String   | 'Unknown'            | —                        |
| guarantor                     | String   | 'Unknown'            | —                        |
| loan_type                     | String   | 'Unknown'            | —                        |
| loan_status                   | String   | 'Unknown'            | —                        |
| interest_rate                 | Double   | 0.0                  | —                        |
| service_charge_rate           | Double   | 0.0                  | —                        |
| currency_of_commitment        | String   | 'Unknown'            | —                        |
| project_id                    | String   | 'Unknown'            | —                        |
| project_name                  | String   | 'Unknown'            | —                        |
| original_principal_amount     | Double   | 0.0                  | USD                      |
| cancelled_amount              | Double   | 0.0                  | USD                      |
| undisbursed_amount            | Double   | 0.0                  | USD                      |
| disbursed_amount              | Double   | 0.0                  | USD                      |
| repaid_to_ibrd                | Double   | 0.0                  | USD                      |
| due_to_ibrd                   | Double   | 0.0                  | USD                      |
| borrowers_obligation          | Double   | 0.0                  | USD                      |
| sold_3rd_party                | Double   | 0.0                  | USD                      |
| repaid_3rd_party              | Double   | 0.0                  | USD                      |
| due_3rd_party                 | Double   | 0.0                  | USD                      |
| loans_held                    | Double   | 0.0                  | USD                      |
| first_repayment_date          | Date     | —                    | Multi-format parsed      |
| last_repayment_date           | Date     | —                    | Multi-format parsed      |
| agreement_signing_date        | Date     | —                    | Multi-format parsed      |
| board_approval_date           | Date     | —                    | Multi-format parsed      |
| effective_date_most_recent    | Date     | —                    | Multi-format parsed      |
| closed_date_most_recent       | Date     | —                    | Multi-format parsed      |
| last_disbursement_date        | Date     | —                    | Multi-format parsed      |

---

## Gold Layer — Star Schema

### fact_loans

| Column                        | Type         | FK To          | Description              |
|-------------------------------|--------------|----------------|--------------------------|
| loan_fact_key                 | BIGINT (PK)  | —              | Surrogate key            |
| country_key                   | BIGINT (FK)  | dim_country    | Country dimension ref    |
| project_key                   | BIGINT (FK)  | dim_project    | Project dimension ref    |
| loan_type_key                 | BIGINT (FK)  | dim_loan_type  | Loan type dimension ref  |
| borrower_key                  | BIGINT (FK)  | dim_borrower   | Borrower dimension ref   |
| loan_number                   | VARCHAR(50)  | — (degenerate) | Drill-through identifier |
| end_of_period                 | DATE         | —              | Reporting period         |
| board_approval_date           | DATE         | —              | Board approval           |
| effective_date_most_recent    | DATE         | —              | Effective date           |
| closed_date_most_recent       | DATE         | —              | Closure date             |
| agreement_signing_date        | DATE         | —              | Signing date             |
| first_repayment_date          | DATE         | —              | First repayment          |
| last_repayment_date           | DATE         | —              | Last repayment           |
| last_disbursement_date        | DATE         | —              | Last disbursement        |
| original_principal_amount     | NUMBER(18,2) | —              | Measure: principal USD   |
| cancelled_amount              | NUMBER(18,2) | —              | Measure: cancelled USD   |
| undisbursed_amount            | NUMBER(18,2) | —              | Measure: undisbursed USD |
| disbursed_amount              | NUMBER(18,2) | —              | Measure: disbursed USD   |
| repaid_to_ibrd                | NUMBER(18,2) | —              | Measure: repaid USD      |
| due_to_ibrd                   | NUMBER(18,2) | —              | Measure: due USD         |
| borrowers_obligation          | NUMBER(18,2) | —              | Measure: obligation USD  |
| interest_rate                 | NUMBER(8,4)  | —              | Rate (%)                 |
| service_charge_rate           | NUMBER(8,4)  | —              | Rate (%)                 |

### dim_country

| Column                  | Type          | Description                        |
|-------------------------|---------------|------------------------------------|
| country_key             | BIGINT (PK)   | Surrogate key                      |
| country_code            | VARCHAR(10)   | ISO country code (natural key)     |
| country                 | VARCHAR(200)  | Full country name                  |
| region                  | VARCHAR(200)  | Geographic region                  |
| guarantor_country_code  | VARCHAR(10)   | Guarantor ISO code                 |
| guarantor               | VARCHAR(200)  | Guarantor entity name              |

### dim_project

| Column        | Type          | Description                        |
|---------------|---------------|------------------------------------|
| project_key   | BIGINT (PK)   | Surrogate key                      |
| project_id    | VARCHAR(50)   | World Bank project ID (natural key)|
| project_name  | VARCHAR(500)  | Human-readable project name        |

### dim_loan_type

| Column                  | Type          | Description                    |
|-------------------------|---------------|--------------------------------|
| loan_type_key           | BIGINT (PK)   | Surrogate key                  |
| loan_type               | VARCHAR(50)   | Loan classification            |
| loan_status             | VARCHAR(100)  | Current status                 |
| currency_of_commitment  | VARCHAR(10)   | Commitment currency            |

### dim_borrower

| Column        | Type          | Description                        |
|---------------|---------------|------------------------------------|
| borrower_key  | BIGINT (PK)   | Surrogate key                      |
| borrower      | VARCHAR(500)  | Borrowing entity name              |
| country_code  | VARCHAR(10)   | Borrower's country code            |
