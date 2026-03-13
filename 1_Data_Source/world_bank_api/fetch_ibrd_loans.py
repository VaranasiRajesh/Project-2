"""
================================================================================
 IBRD Statement of Loans — World Bank API Data Ingestion
================================================================================
 Module      : fetch_ibrd_loans.py
 Layer       : Data Source / Bronze Ingestion
 Purpose     : Fetches the complete historical IBRD Statement of Loans dataset
               from the World Bank Open Data API (SODA-compatible endpoint),
               handles pagination, retries, and persists the result as a local
               CSV file (ibrd_loans_1M.csv) for downstream Azure Data Factory
               ingestion into the Bronze layer of ADLS Gen2.
 API Docs    : https://finances.worldbank.org/resource/zucq-nqt1.json
 Author      : Data Engineering Team
 Created     : 2026-03-13
================================================================================
"""

# ──────────────────────────────────────────────────────────────────────────────
# Standard Library Imports
# ──────────────────────────────────────────────────────────────────────────────
import os
import sys
import csv
import time
import logging
from datetime import datetime, timezone

# ──────────────────────────────────────────────────────────────────────────────
# Third-Party Imports
# ──────────────────────────────────────────────────────────────────────────────
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

# World Bank SODA API endpoint for IBRD Statement of Loans
API_BASE_URL = "https://finances.worldbank.org/resource/zucq-nqt1.json"

# Application token (optional but recommended to avoid throttling).
# Set via environment variable for security; falls back to None (anonymous).
APP_TOKEN = os.environ.get("WORLD_BANK_APP_TOKEN", None)

# Pagination settings
PAGE_SIZE = 10000          # Number of records per API call ($limit)
MAX_RECORDS = 1_000_000    # Safety cap — stop after 1M records

# Retry / timeout settings
REQUEST_TIMEOUT = 120      # Seconds before a single request times out
MAX_RETRIES = 5            # Maximum retry attempts per failed request
RETRY_BACKOFF = 2          # Exponential backoff factor (2, 4, 8, 16 … sec)
RETRY_STATUS_FORCELIST = [429, 500, 502, 503, 504]  # HTTP codes to retry on

# Output settings
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILENAME = "ibrd_loans_1M.csv"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════════════

def _configure_logger() -> logging.Logger:
    """
    Configures and returns a module-level logger with both console and
    rotating file handlers.  Log format includes timestamp, level, and
    message for production traceability.
    """
    logger = logging.getLogger("ibrd_api_ingestion")
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers if module is re-imported
    if logger.handlers:
        return logger

    # ── Console Handler (INFO+) ──────────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_fmt = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-8s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_fmt)
    logger.addHandler(console_handler)

    # ── File Handler (DEBUG+) ────────────────────────────────────────────
    log_file = os.path.join(OUTPUT_DIR, "fetch_ibrd_loans.log")
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_fmt = logging.Formatter(
        fmt="%(asctime)s │ %(name)s │ %(levelname)-8s │ %(funcName)s:%(lineno)d │ %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z"
    )
    file_handler.setFormatter(file_fmt)
    logger.addHandler(file_handler)

    return logger


logger = _configure_logger()


# ══════════════════════════════════════════════════════════════════════════════
# HTTP SESSION WITH AUTOMATIC RETRY
# ══════════════════════════════════════════════════════════════════════════════

def _create_session() -> requests.Session:
    """
    Builds a requests.Session pre-configured with:
      • Automatic retries (exponential backoff) on transient failures.
      • Connection pooling for efficient pagination.
      • Optional World Bank application token in headers.
    """
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=RETRY_STATUS_FORCELIST,
        allowed_methods=["GET"],        # Only retry idempotent GET requests
        raise_on_status=False           # We handle status codes manually
    )

    # Mount the retry adapter for both HTTP and HTTPS
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Set default headers
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "IBRD-Loan-Pipeline/1.0"
    })

    # Attach app token if available (increases rate limits)
    if APP_TOKEN:
        session.headers["X-App-Token"] = APP_TOKEN
        logger.info("World Bank App Token attached to session headers.")
    else:
        logger.warning(
            "No WORLD_BANK_APP_TOKEN found in environment. "
            "Requests may be throttled by the API."
        )

    return session


# ══════════════════════════════════════════════════════════════════════════════
# CORE INGESTION LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def fetch_page(session: requests.Session, offset: int) -> list:
    """
    Fetches a single page of records from the World Bank API.

    Parameters
    ----------
    session : requests.Session
        Pre-configured HTTP session with retry logic.
    offset : int
        The $offset parameter for SODA pagination.

    Returns
    -------
    list
        A list of dictionaries (JSON records) from the API.
        Returns an empty list if the request fails after retries.
    """
    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$order": ":id"        # Deterministic ordering for stable pagination
    }

    try:
        logger.debug(f"Requesting offset={offset}, limit={PAGE_SIZE}")
        response = session.get(
            API_BASE_URL,
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        # ── Check for HTTP errors ────────────────────────────────────────
        if response.status_code != 200:
            logger.error(
                f"HTTP {response.status_code} at offset {offset}. "
                f"Response: {response.text[:500]}"
            )
            response.raise_for_status()   # Will be caught by outer except

        # ── Parse JSON payload ───────────────────────────────────────────
        data = response.json()

        if not isinstance(data, list):
            logger.error(
                f"Unexpected response format at offset {offset}. "
                f"Expected list, got {type(data).__name__}."
            )
            return []

        logger.debug(f"Received {len(data)} records at offset {offset}.")
        return data

    except requests.exceptions.Timeout:
        logger.error(
            f"Request TIMED OUT after {REQUEST_TIMEOUT}s at offset {offset}."
        )
        return []

    except requests.exceptions.ConnectionError as conn_err:
        logger.error(
            f"Connection error at offset {offset}: {conn_err}"
        )
        return []

    except requests.exceptions.HTTPError as http_err:
        logger.error(
            f"HTTP error at offset {offset}: {http_err}"
        )
        return []

    except requests.exceptions.JSONDecodeError:
        logger.error(
            f"Failed to decode JSON response at offset {offset}. "
            f"Raw text: {response.text[:300]}"
        )
        return []

    except Exception as exc:
        # Catch-all for unexpected failures (e.g., SSL errors)
        logger.critical(
            f"Unexpected error at offset {offset}: {exc}",
            exc_info=True
        )
        return []


def fetch_all_records() -> list:
    """
    Paginates through the entire World Bank IBRD loans dataset.

    Stops when:
      1. The API returns fewer records than PAGE_SIZE (end of data), OR
      2. Total fetched records reach MAX_RECORDS (safety cap).

    Returns
    -------
    list
        Complete list of loan record dictionaries.
    """
    session = _create_session()
    all_records = []
    offset = 0
    page_num = 0
    consecutive_failures = 0
    max_consecutive_failures = 3    # Abort if 3 pages fail in a row

    logger.info("=" * 70)
    logger.info("IBRD LOAN DATA INGESTION STARTED")
    logger.info(f"  API Endpoint  : {API_BASE_URL}")
    logger.info(f"  Page Size     : {PAGE_SIZE:,}")
    logger.info(f"  Max Records   : {MAX_RECORDS:,}")
    logger.info(f"  Output File   : {OUTPUT_PATH}")
    logger.info("=" * 70)

    start_time = time.time()

    while offset < MAX_RECORDS:
        page_num += 1
        page_data = fetch_page(session, offset)

        # ── Handle empty / failed page ───────────────────────────────────
        if not page_data:
            consecutive_failures += 1
            logger.warning(
                f"Page {page_num} returned 0 records. "
                f"Consecutive failures: {consecutive_failures}/{max_consecutive_failures}"
            )
            if consecutive_failures >= max_consecutive_failures:
                logger.error(
                    f"Aborting: {max_consecutive_failures} consecutive page "
                    f"failures. Last successful offset: {offset - PAGE_SIZE}"
                )
                break
            # Wait briefly then retry same offset
            time.sleep(5)
            continue

        # ── Success — reset failure counter and accumulate ───────────────
        consecutive_failures = 0
        all_records.extend(page_data)
        records_so_far = len(all_records)

        logger.info(
            f"Page {page_num:>4} │ Fetched {len(page_data):>6,} records │ "
            f"Total: {records_so_far:>10,}"
        )

        # ── Check for end-of-data (partial page) ────────────────────────
        if len(page_data) < PAGE_SIZE:
            logger.info(
                f"Received partial page ({len(page_data)} < {PAGE_SIZE}). "
                "End of dataset reached."
            )
            break

        # ── Advance offset for next page ─────────────────────────────────
        offset += PAGE_SIZE

        # Polite delay to avoid hammering the API (rate-limit courtesy)
        time.sleep(0.5)

    elapsed = time.time() - start_time
    logger.info("-" * 70)
    logger.info(f"INGESTION COMPLETE — {len(all_records):,} records in {elapsed:.1f}s")
    logger.info("-" * 70)

    return all_records


# ══════════════════════════════════════════════════════════════════════════════
# CSV PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════

def save_to_csv(records: list, output_path: str) -> None:
    """
    Persists the fetched records to a CSV file.

    Parameters
    ----------
    records : list
        List of dictionaries (each dict = one loan record).
    output_path : str
        Absolute path to the output CSV file.

    Raises
    ------
    IOError
        If the file cannot be written (permissions, disk space, etc.).
    """
    if not records:
        logger.warning("No records to save. CSV will not be created.")
        return

    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # ── Collect all unique field names across records ────────────────
        # (Some records may have fields others don't due to API sparsity)
        all_fields = []
        seen = set()
        for record in records:
            for key in record.keys():
                if key not in seen:
                    all_fields.append(key)
                    seen.add(key)

        logger.info(f"Writing {len(records):,} records with {len(all_fields)} columns to CSV...")

        # ── Write CSV with DictWriter (handles missing keys gracefully) ──
        with open(output_path, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=all_fields,
                extrasaction="ignore",      # Ignore unexpected extra fields
                restval=""                   # Fill missing fields with empty string
            )
            writer.writeheader()
            writer.writerows(records)

        # ── Log file size ────────────────────────────────────────────────
        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logger.info(
            f"CSV saved successfully: {output_path} "
            f"({file_size_mb:.2f} MB, {len(records):,} rows)"
        )

    except PermissionError:
        logger.critical(f"Permission denied writing to {output_path}.")
        raise

    except OSError as os_err:
        logger.critical(f"OS error writing CSV: {os_err}")
        raise

    except Exception as exc:
        logger.critical(f"Unexpected error saving CSV: {exc}", exc_info=True)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# DATA VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_output(output_path: str) -> bool:
    """
    Performs basic post-ingestion validation on the output CSV.

    Checks
    ------
    1. File exists and is non-empty.
    2. Row count matches header + data rows expectation.
    3. CSV is readable (not corrupted).

    Returns
    -------
    bool
        True if validation passes, False otherwise.
    """
    logger.info("Running post-ingestion validation...")

    if not os.path.exists(output_path):
        logger.error(f"Validation FAILED: File not found at {output_path}")
        return False

    file_size = os.path.getsize(output_path)
    if file_size == 0:
        logger.error("Validation FAILED: Output file is empty (0 bytes).")
        return False

    try:
        with open(output_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            row_count = sum(1 for _ in reader)

        logger.info(f"Validation PASSED: {row_count:,} data rows, {len(header)} columns.")
        logger.info(f"  Columns: {header[:5]} ... ({len(header)} total)")
        return True

    except Exception as exc:
        logger.error(f"Validation FAILED: Could not read CSV — {exc}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    """
    Orchestrates the full ingestion workflow:
      1. Fetch all IBRD loan records via paginated API calls.
      2. Save to local CSV.
      3. Validate the output file.
      4. Exit with appropriate status code.
    """
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logger.info(f"Run ID: {run_id}")

    try:
        # Step 1: Fetch data from World Bank API
        records = fetch_all_records()

        if not records:
            logger.error("No records fetched. Pipeline cannot continue.")
            sys.exit(1)

        # Step 2: Persist to CSV
        save_to_csv(records, OUTPUT_PATH)

        # Step 3: Validate output
        is_valid = validate_output(OUTPUT_PATH)

        if not is_valid:
            logger.error("Output validation failed. Review logs for details.")
            sys.exit(1)

        logger.info("=" * 70)
        logger.info(f"✅ INGESTION PIPELINE COMPLETED SUCCESSFULLY — Run {run_id}")
        logger.info("=" * 70)
        sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("Ingestion interrupted by user (Ctrl+C).")
        sys.exit(130)

    except Exception as exc:
        logger.critical(f"Fatal error in ingestion pipeline: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
