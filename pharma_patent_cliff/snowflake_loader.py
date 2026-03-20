"""
Snowflake Loader
COPY INTO helpers for loading S3 Parquet files into Snowflake RAW schema.

Pattern: S3 → Snowflake COPY INTO (production-standard ingestion).
S3 stays as raw landing zone; Snowflake handles transform + query.
Credentials sourced from environment variables — never hardcoded.
"""

import logging
import os

import snowflake.connector

logger = logging.getLogger(__name__)

# --- Connection config (from environment / Airflow Variables) ---
SNOWFLAKE_CONFIG = {
    "account":   os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user":      os.environ.get("SNOWFLAKE_USER"),
    "password":  os.environ.get("SNOWFLAKE_PASSWORD"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database":  os.environ.get("SNOWFLAKE_DATABASE", "PHARMA_CLIFF"),
    "schema":    "RAW",
    "role":      os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
}

S3_BUCKET = os.environ.get("S3_BUCKET", "data-engineering-portfolio-ericg")
S3_STAGE  = os.environ.get("SNOWFLAKE_S3_STAGE", "pharma_s3_stage")  # pre-created stage

# DDL for RAW tables — run once on first deploy
DDL_STATEMENTS = {
    "orange_book": """
        CREATE TABLE IF NOT EXISTS RAW.ORANGE_BOOK (
            appl_type            VARCHAR,
            appl_no              VARCHAR,
            product_no           VARCHAR,
            exclusivity_code     VARCHAR,
            exclusivity_date     DATE,
            trade_name           VARCHAR,
            ingredient           VARCHAR,
            applicant_full_name  VARCHAR,
            _loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "yfinance": """
        CREATE TABLE IF NOT EXISTS RAW.YFINANCE (
            ticker      VARCHAR,
            date        DATE,
            open        FLOAT,
            high        FLOAT,
            low         FLOAT,
            close       FLOAT,
            volume      BIGINT,
            market_cap  FLOAT,
            _loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "clinical_trials": """
        CREATE TABLE IF NOT EXISTS RAW.CLINICAL_TRIALS (
            ticker           VARCHAR,
            nct_id           VARCHAR,
            sponsor          VARCHAR,
            phase            VARCHAR,
            drug_name        VARCHAR,
            status           VARCHAR,
            start_date       DATE,
            completion_date  DATE,
            _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "edgar": """
        CREATE TABLE IF NOT EXISTS RAW.EDGAR (
            ticker      VARCHAR,
            concept     VARCHAR,
            metric      VARCHAR,
            form        VARCHAR,
            end_date    DATE,
            value_usd   BIGINT,
            filed       DATE,
            accn        VARCHAR,
            _loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
}

# COPY INTO templates — %s placeholders: stage, s3_path, table
COPY_TEMPLATE = """
    COPY INTO RAW.{table}
    FROM @{stage}/{s3_path}/
    FILE_FORMAT = (TYPE = 'PARQUET' SNAPPY_COMPRESSION = TRUE)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = CONTINUE
    PURGE = FALSE
"""


def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def ensure_tables_exist(conn: snowflake.connector.SnowflakeConnection) -> None:
    """Create RAW tables if they don't exist (idempotent)."""
    with conn.cursor() as cur:
        for table, ddl in DDL_STATEMENTS.items():
            logger.info("Ensuring table RAW.%s exists", table.upper())
            cur.execute(ddl)


def copy_into(
    conn: snowflake.connector.SnowflakeConnection,
    table: str,
    s3_path: str,
) -> int:
    """
    Run COPY INTO for one table from S3 path.
    Returns row count loaded.
    """
    sql = COPY_TEMPLATE.format(table=table.upper(), stage=S3_STAGE, s3_path=s3_path)
    logger.info("COPY INTO RAW.%s from %s", table.upper(), s3_path)
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchall()
        rows_loaded = sum(r[3] for r in result if r[3])  # rows_loaded column
        logger.info("Loaded %d rows into RAW.%s", rows_loaded, table.upper())
        return rows_loaded


# Default S3 paths per table — used when caller doesn't pass an explicit path
_DEFAULT_PATHS: dict[str, str] = {
    "orange_book":     "raw/orange_book/year={year}/month={month:02d}",
    "yfinance":        "raw/yfinance",
    "clinical_trials": "raw/clinical_trials/year={year}/month={month:02d}",
    "edgar":           "raw/edgar/year={year}/month={month:02d}",
}


def load_table(table: str, s3_path: str | None = None) -> int:
    """
    Load any RAW table from its S3 Parquet path.
    If s3_path is omitted, uses the default Hive-partitioned path for today.
    """
    from datetime import UTC, datetime
    now = datetime.now(UTC)
    path = s3_path or _DEFAULT_PATHS[table].format(
        year=now.year, month=now.month
    )
    with get_connection() as conn:
        ensure_tables_exist(conn)
        return copy_into(conn, table, path)


# Convenience wrappers kept for Airflow task callables and CLI use
def load_orange_book(s3_path: str | None = None) -> int:
    return load_table("orange_book", s3_path)


def load_yfinance(s3_path: str | None = None) -> int:
    return load_table("yfinance", s3_path)


def load_clinical_trials(s3_path: str | None = None) -> int:
    return load_table("clinical_trials", s3_path)


def load_edgar(s3_path: str | None = None) -> int:
    return load_table("edgar", s3_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for tbl in ("orange_book", "yfinance", "clinical_trials", "edgar"):
        n = load_table(tbl)
        print(f"{tbl}: {n} rows loaded")
