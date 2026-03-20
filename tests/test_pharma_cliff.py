"""
Pharma Patent Cliff — Unit Tests
Tests extractor logic with mocked S3 and API calls.
Run: pytest tests/test_pharma_cliff.py -v
"""

import io
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ── Orange Book Extractor ──────────────────────────────────────────────────────

def _make_orange_book_zip(excl_content: str, prod_content: str) -> bytes:
    """Build an in-memory ZIP with exclusivity.txt and products.txt."""
    import zipfile as zf
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w") as z:
        z.writestr("exclusivity.txt", excl_content)
        z.writestr("products.txt", prod_content)
    buf.seek(0)
    return buf.read()


class TestOrangeBookExtractor:
    """Tests for orange_book_extractor.py

    Orange Book ships as a ZIP containing two pipe-delimited files:
      exclusivity.txt  — code + date (no drug name)
      products.txt     — trade_name, ingredient, applicant_full_name
    filter_nce(excl, prod) joins them on Appl_No+Product_No.
    """

    # exclusivity.txt: 5 cols — Appl_Type, Appl_No, Product_No, Exclusivity_Code, Exclusivity_Date
    EXCL_CONTENT = (
        "Appl_Type~Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date\n"
        "N~210736~001~NCE~Jan 01, 2028\n"
        "N~210736~002~ODE~Jan 01, 2028\n"   # non-NCE — filtered out
        "N~210737~001~NCE-1~Mar 15, 2029\n"
    )
    # products.txt: has trade name, ingredient, applicant
    PROD_CONTENT = (
        "Appl_Type~Appl_No~Product_No~Trade_Name~Ingredient~Applicant_Full_Name\n"
        "N~210736~001~KEYTRUDA~PEMBROLIZUMAB~MERCK SHARP\n"
        "N~210736~002~KEYTRUDA~PEMBROLIZUMAB~MERCK SHARP\n"
        "N~210737~001~OPDIVO~NIVOLUMAB~BRISTOL-MYERS\n"
    )

    def _make_dfs(self, excl=None, prod=None):
        excl_csv = excl or self.EXCL_CONTENT
        prod_csv = prod or self.PROD_CONTENT
        excl_df = pd.read_csv(io.StringIO(excl_csv), sep="~", dtype=str)
        prod_df = pd.read_csv(io.StringIO(prod_csv), sep="~", dtype=str)
        return excl_df, prod_df

    def test_filter_nce_keeps_nce_codes(self):
        from pharma_patent_cliff.orange_book_extractor import filter_nce
        excl, prod = self._make_dfs()
        result = filter_nce(excl, prod)
        assert set(result["Exclusivity_Code"].unique()) <= {"NCE", "NCE-1"}

    def test_filter_nce_drops_null_trade_name(self):
        """Rows that fail the products join (no matching Appl_No/Product_No) yield null Trade_Name."""
        from pharma_patent_cliff.orange_book_extractor import filter_nce
        excl = pd.read_csv(io.StringIO(
            "Appl_Type~Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date\n"
            "N~999~001~NCE~Jan 01, 2029\n"  # no matching product row
        ), sep="~", dtype=str)
        prod = pd.read_csv(io.StringIO(
            "Appl_Type~Appl_No~Product_No~Trade_Name~Ingredient~Applicant_Full_Name\n"
        ), sep="~", dtype=str)
        result = filter_nce(excl, prod)
        assert len(result) == 0

    def test_filter_nce_parses_date(self):
        from pharma_patent_cliff.orange_book_extractor import filter_nce
        excl, prod = self._make_dfs()
        result = filter_nce(excl, prod)
        assert pd.api.types.is_datetime64_any_dtype(result["Exclusivity_Date"])

    def test_filter_nce_uppercases_trade_name(self):
        from pharma_patent_cliff.orange_book_extractor import filter_nce
        excl = pd.read_csv(io.StringIO(
            "Appl_Type~Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date\n"
            "N~210736~001~NCE~Jan 01, 2028\n"
        ), sep="~", dtype=str)
        prod = pd.read_csv(io.StringIO(
            "Appl_Type~Appl_No~Product_No~Trade_Name~Ingredient~Applicant_Full_Name\n"
            "N~210736~001~keytruda~pembrolizumab~merck sharp\n"
        ), sep="~", dtype=str)
        result = filter_nce(excl, prod)
        assert result.iloc[0]["Trade_Name"] == "KEYTRUDA"

    @patch("pharma_patent_cliff.s3_utils.boto3.client")
    def test_write_parquet_calls_put_object(self, mock_boto):
        from pharma_patent_cliff.s3_utils import write_parquet
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        df = pd.DataFrame({
            "Trade_Name": ["KEYTRUDA"],
            "Exclusivity_Date": [datetime(2028, 1, 1)],
        })
        uri = write_parquet(df, "raw/orange_book/year=2025/month=03/data.parquet", bucket="test-bucket")

        assert mock_s3.put_object.called
        assert uri.startswith("s3://test-bucket/")

    @patch("pharma_patent_cliff.orange_book_extractor.requests.get")
    def test_download_raises_on_http_error(self, mock_get):
        from pharma_patent_cliff.orange_book_extractor import download_orange_book_zip
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("404 Not Found")
        mock_get.return_value = mock_resp

        with pytest.raises(Exception, match="404"):
            download_orange_book_zip()


# ── yFinance Extractor ─────────────────────────────────────────────────────────

class TestYFinanceExtractor:
    """Tests for yfinance_extractor.py"""

    def _make_sample_history(self, ticker: str = "MRK") -> pd.DataFrame:
        dates = pd.date_range("2024-01-01", periods=5, freq="B", tz="UTC")
        return pd.DataFrame({
            "Date": dates,
            "Open":  [100.0, 101.0, 102.0, 103.0, 104.0],
            "High":  [105.0, 106.0, 107.0, 108.0, 109.0],
            "Low":   [99.0,  100.0, 101.0, 102.0, 103.0],
            "Close": [102.0, 103.0, 104.0, 105.0, 106.0],
            "Volume":[1000000, 1100000, 1200000, 1300000, 1400000],
        }).set_index("Date")

    @patch("pharma_patent_cliff.yfinance_extractor.yf.Ticker")
    def test_pull_ticker_returns_expected_columns(self, mock_ticker_cls):
        from pharma_patent_cliff.yfinance_extractor import pull_ticker
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = self._make_sample_history()
        mock_ticker.fast_info.shares = 2_000_000_000  # fast_info.shares, not shares_outstanding
        mock_ticker_cls.return_value = mock_ticker

        df = pull_ticker("MRK", "2024-01-01", "2024-12-31")

        assert "ticker" in df.columns
        assert "close" in df.columns
        assert "market_cap" in df.columns
        assert (df["close"] > 0).all()

    @patch("pharma_patent_cliff.yfinance_extractor.yf.Ticker")
    def test_pull_ticker_empty_history_returns_empty(self, mock_ticker_cls):
        from pharma_patent_cliff.yfinance_extractor import pull_ticker
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()
        mock_ticker_cls.return_value = mock_ticker

        df = pull_ticker("FAKE", "2024-01-01", "2024-12-31")
        assert df.empty

    @patch("pharma_patent_cliff.s3_utils.boto3.client")  # write_parquet lives in s3_utils
    @patch("pharma_patent_cliff.yfinance_extractor.yf.Ticker")
    def test_run_writes_all_tickers(self, mock_ticker_cls, mock_boto):
        from pharma_patent_cliff.yfinance_extractor import run
        from pharma_patent_cliff.config import TICKERS
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = self._make_sample_history()
        mock_ticker.fast_info.shares = 1_000_000_000  # fast_info.shares, not shares_outstanding
        mock_ticker_cls.return_value = mock_ticker

        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        results = run(tickers=TICKERS, bucket="test-bucket")
        assert len(results) == len(TICKERS)  # TICKERS drives count — add NVS there, test stays correct
        assert mock_s3.put_object.call_count == len(TICKERS)


# ── Clinical Trials Extractor ──────────────────────────────────────────────────

class TestClinicalTrialsExtractor:
    """Tests for clinical_trials_extractor.py"""

    SAMPLE_API_RESPONSE = {
        "studies": [
            {
                "protocolSection": {
                    "identificationModule": {"nctId": "NCT00000001", "briefTitle": "Drug A Trial"},
                    "statusModule": {
                        "overallStatus": "RECRUITING",
                        "startDateStruct": {"date": "2023-01"},
                        "primaryCompletionDateStruct": {"date": "2026-12"},
                    },
                    "designModule": {"phases": ["PHASE3"]},
                    "sponsorCollaboratorsModule": {"leadSponsor": {"name": "Merck"}},
                    "armsInterventionsModule": {
                        "interventions": [{"type": "DRUG", "name": "Pembrolizumab"}]
                    },
                }
            }
        ],
        "nextPageToken": None,
    }

    @patch("pharma_patent_cliff.clinical_trials_extractor.requests.get")
    def test_fetch_trials_parses_record(self, mock_get):
        from pharma_patent_cliff.clinical_trials_extractor import fetch_trials_for_sponsor
        mock_resp = MagicMock()
        mock_resp.json.return_value = self.SAMPLE_API_RESPONSE
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        records = fetch_trials_for_sponsor("Merck", "MRK")
        assert len(records) == 1
        assert records[0]["nct_id"] == "NCT00000001"
        assert records[0]["ticker"] == "MRK"
        assert records[0]["drug_name"] == "Pembrolizumab"

    @patch("pharma_patent_cliff.clinical_trials_extractor.requests.get")
    def test_fetch_trials_deduplicates_by_nct_id(self, mock_get):
        from pharma_patent_cliff.clinical_trials_extractor import fetch_all_companies
        # Return same study for every call (simulates overlap across sponsor_names)
        mock_resp = MagicMock()
        mock_resp.json.return_value = self.SAMPLE_API_RESPONSE
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_all_companies()
        # NCT00000001 should appear only once despite multiple sponsor lookups
        assert df["nct_id"].value_counts().max() == 1


# ── Snowflake Loader ───────────────────────────────────────────────────────────

class TestSnowflakeLoader:
    """Tests for snowflake_loader.py"""

    @patch("pharma_patent_cliff.snowflake_loader.snowflake.connector.connect")
    def test_ensure_tables_exist_runs_ddl(self, mock_connect):
        from pharma_patent_cliff.snowflake_loader import ensure_tables_exist, DDL_STATEMENTS
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        ensure_tables_exist(mock_conn)
        # Should execute one DDL per table
        assert mock_cursor.execute.call_count == len(DDL_STATEMENTS)

    @patch("pharma_patent_cliff.snowflake_loader.snowflake.connector.connect")
    def test_copy_into_returns_row_count(self, mock_connect):
        from pharma_patent_cliff.snowflake_loader import copy_into
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Simulate COPY INTO result: (file, status, rows_parsed, rows_loaded, ...)
        mock_cursor.fetchall.return_value = [
            ("file1.parquet", "LOADED", 100, 100, 0, 0, None, None, None, None)
        ]
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        rows = copy_into(mock_conn, "orange_book", "raw/orange_book/year=2025/month=03")
        assert rows == 100
