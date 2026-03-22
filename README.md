# Own the Model, Own the Returns

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat-square&logo=amazonaws&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=flat-square&logo=terraform&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat-square&logo=powerbi&logoColor=black)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white)
&nbsp;
![Pipelines](https://img.shields.io/badge/Pipelines-4-brightgreen?style=flat-square)
![Tests](https://img.shields.io/badge/Tests-213-brightgreen?style=flat-square)
![Stocks](https://img.shields.io/badge/Stocks%20Analyzed-10-blue?style=flat-square)

A data engineering portfolio proving a **+111.5% Sharpe ratio premium** for proprietary AI builders over third-party integrators (Spearman ρ = +0.800). Four production Airflow pipelines (Docker, CeleryExecutor) ingest data from Alpha Vantage, SEC EDGAR, and FRED — storing in a Hive-partitioned S3 data lake as Parquet/Snappy, querying with Athena, and visualizing findings in Power BI. Validated by 213 pytest unit tests with moto AWS mocking. Infrastructure codified end-to-end in Terraform.

## Key Finding: The Market Rewards AI Builders, Not AI Renters

Analysis of risk-adjusted returns (Jan 2023 – present) across 10 major tech stocks reveals a clear **AI value chain hierarchy** in Sharpe ratios:

| Tier | Companies | Avg Sharpe | AI Strategy |
|------|-----------|-----------|-------------|
| Infrastructure | NVDA | 3.180 | Sells the GPUs |
| AI Builders | META, GOOGL | 1.985 | Proprietary AI (Llama, Gemini, custom chips) |
| AI Integrators | MSFT, AMZN | 0.939 | Third-party partnerships (OpenAI, Anthropic) |
| Control | AAPL, TSLA | 1.131 | Mixed AI exposure |
| Legacy Tech | CRM, ORCL, ADBE | 0.247 | Traditional software |

**Builder Premium: +111.5%** — Companies building proprietary AI outperform those renting it through partnerships by 111% on risk-adjusted returns. The premium has widened from +58.4% (Dec 2025) as AI integrators weakened in early 2026 while builders held up.

Spearman rank correlation between AI% of capex and Sharpe ratio: **ρ = +0.800** — the premium holds stock by stock, not just in aggregate.

The FRED macro pipeline enables macro regime analysis: each month is classified into a combined regime using GS10, CPI, UNRATE, and FEDFUNDS data, and the trailing 12-month Sharpe premium for AI Builders vs. Integrators is computed per regime. Rate regime is determined by comparing the current GS10 yield to its 12-month rolling mean (rising vs. falling); inflation regime by CPI year-over-year change exceeding 4%; unemployment regime by UNRATE exceeding 5.5%. Run `make regime` to produce `stock_pipeline/regime_analysis.csv` (per-month detail) and `stock_pipeline/regime_summary.csv` (aggregated premium by regime combination) — results are not yet final since the analysis requires live S3 data from the FRED pipeline.

![Dashboard](dashboard.png)

In 2026, Big Tech will spend ~$650B on AI infrastructure. But spending more doesn't mean earning more — Meta spends the least of the four hybrids ($125B) yet delivers the highest Sharpe ratio (2.138) because nearly 100% of its capex goes to proprietary AI. Amazon spends the most ($200B) but dilutes returns across logistics and third-party partnerships.

## Architecture

```
┌──────────────────┐     ┌──────────────┐     ┌───────────┐     ┌─────────┐
│   Data Sources    │────>│Apache Airflow│────>│  AWS S3   │────>│ Athena  │
│                   │     │  (Docker)    │     │(Data Lake)│     │ (Query) │
│ Alpha Vantage     │     │              │     │           │     └────┬────┘
│ SEC EDGAR         │     │ 4 production │     │Partitioned│          │
│ FRED (St. Louis   │     │ DAGs +       │     │by symbol/ │     ┌────▼────┐
│   Fed)            │     │ analysis     │     │date/series│     │Power BI │
│                   │     │ pipeline     │     │           │     │(Dashboard)│
│                   │     └──────────────┘     └───────────┘     └─────────┘
└──────────────────┘
```

## Pipelines

### Stock Pipeline (`stock_pipeline/stock_pipeline.py`)
- **Stocks:** NVDA, MSFT, GOOGL, AMZN, META, CRM, ORCL, ADBE, AAPL, TSLA
- **Source:** Alpha Vantage API (Global Quote)
- **Schedule:** 5 PM ET Mon-Fri (after market close)
- **S3 path:** `stocks/date={date}/{timestamp}.parquet`

### SEC EDGAR Pipeline (`edgar_pipeline/edgar_pipeline.py`)
- **Source:** SEC EDGAR Company Facts API (free, no auth beyond User-Agent header)
- **Data:** Annual capex + revenue from 10-K filings for META, GOOGL, MSFT, AMZN
- **Schedule:** Quarterly (Jan/Apr/Jul/Oct 1) — picks up each company's 10-K within 3 months
- **S3 path:** `fundamentals/cik={cik}/year={year}/data.parquet`
- Rate-limit aware: 1-second sleep between company fetches (SEC 10 req/sec limit)
- Replaces hardcoded capex figures with authoritative SEC filings

### FRED Macro Pipeline (`fred_pipeline/fred_pipeline.py`)
- **Source:** St. Louis Fed FRED API (free, API key required)
- **Series:** GS10 (10-yr Treasury), CPIAUCSL (CPI), UNRATE (unemployment), FEDFUNDS (fed funds rate)
- **Schedule:** 1st of every month (FRED releases with ~2-week lag)
- **S3 path:** `macro_indicators/series={series_id}/year={year}/data.parquet`
- Enables macro regime analysis: does the AI premium hold across rate cycles?

### Analysis Pipeline (`analysis_pipeline/analysis_pipeline.py`)
- Runs Sharpe backtest + portfolio analysis automatically after daily stock load
- **Schedule:** 5:30 PM Mon-Fri (30 min after stock pipeline)
- Replaces the manual `make analyze` command

### Monitor (`monitoring/pipeline_monitor.py`)
Schedule-aware health checks across all pipelines — staleness thresholds vary by source cadence (daily/monthly/quarterly), with succeeded/failed symbol lists for targeted backfill.

## Historical Backtest (`stock_pipeline/historical_backtest.py`)

Pulls 3 years of monthly adjusted close prices and calculates:
- Annualized return, volatility, and Sharpe ratio per stock
- Category-level averages across the AI value chain
- Build vs. Rent premium (proprietary AI vs. partnership AI)
- Capex efficiency (Sharpe per $B of AI spend, from SEC EDGAR + earnings guidance)
- Spearman rank correlation between AI% of capex and Sharpe ratio

## Infrastructure as Code

AWS resources defined in Terraform under `terraform/`:
- **S3 bucket** — data lake for all pipelines
- **Glue catalog database + tables** — schema definitions for Athena queries
- **Athena workgroup** — query engine with S3 results location

```bash
cd terraform
terraform init
terraform validate   # Verify configuration
terraform plan       # Preview resources (no changes applied)
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Apache Airflow 2.10.4 (CeleryExecutor) |
| Infrastructure | Docker Compose (6 containers, PostgreSQL 16) |
| Storage | AWS S3 (Parquet/Snappy, Hive-style partitions) |
| Query Engine | AWS Athena (Presto SQL) |
| Visualization | Power BI |
| IaC | Terraform |
| CI/CD | GitHub Actions (lint, pytest, CodeQL, Scorecard, SBOM, dependency review) |
| Language | Python 3.12 |
| Key Libraries | boto3, pandas, numpy, pyarrow, requests |
| Testing | pytest + moto (213 tests, AWS mocked at HTTP layer) |

## Testing

213 tests across all pipelines, using moto to mock AWS at the HTTP layer — no real AWS calls in CI.

```bash
pytest tests/ -v        # Run all 213 tests
pytest tests/test_edgar_pipeline.py -v   # Single pipeline
make lint               # flake8 across all source dirs
```

| Test File | Tests | Coverage |
|-----------|-------|---------|
| test_utils.py | 30 | s3_read/write_json/ndjson/parquet, Athena client, register partition |
| test_data_quality.py | 18 | validation rules |
| test_finance_utils.py | 16 | annualized return, drawdown, beta, rolling Sharpe |
| test_edgar_pipeline.py | 16 | extract helper, transform, load (Parquet) |
| test_historical_backfill.py | 11 | format (list of dicts), write (Parquet), register |
| test_fred_pipeline.py | 11 | transform + load (Parquet) |
| test_portfolio_analysis.py | 10 | portfolio metrics |
| test_stock_pipeline.py | 9 | transform + load (Parquet) |
| test_sharpe_calculation.py | 8 | Sharpe math |
| test_forecast_pipeline.py | 7 | transform + load (Parquet) |
| test_analysis_pipeline.py | 6 | DAG structure |

## Security

All credentials managed via environment variables — zero hardcoded secrets:
- AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- API keys (`ALPHA_VANTAGE_API_KEY`, `OPENWEATHER_API_KEY`, `FRED_API_KEY`)
- Injected into Airflow containers via Docker Compose `.env` file
- Hardened `.gitignore` blocks keys, certs, and credential files from being committed

CI/CD security pipeline on every push:
- **bandit** — Python static analysis for security issues
- **pip-audit** — dependency vulnerability scanning
- **checkov** — Terraform IaC security scanning
- **CodeQL** — GitHub's semantic code analysis
- **Semgrep** — SAST rules for common vulnerability patterns
- **Dependency Review** — flags newly introduced vulnerable packages on PRs
- **OpenSSF Scorecard** — supply chain security scoring
- **SBOM** — software bill of materials generated on every push

Repo hardening:
- Branch protection: CI required, no force push, no direct commits to main, linear history
- Secret scanning + push protection enabled (blocks commits containing credentials)
- Dependabot security updates enabled
- Pre-commit hooks: trailing whitespace, private key detection, secret scanning
- `SECURITY.md` with vulnerability disclosure policy

## Project Structure

```
data-engineering-portfolio/
├── config.py                          # Central constants (symbols, S3 bucket, FRED series, EDGAR CIKs)
├── stock_pipeline/
│   ├── stock_pipeline.py              # Airflow DAG: 10-stock daily ingestion
│   ├── historical_backtest.py         # 3-year Sharpe ratio analysis
│   ├── historical_backfill.py         # One-time S3 backfill script (Parquet output)
│   ├── portfolio_analysis.py          # Build vs Rent + capex efficiency CSVs
│   ├── finance_utils.py               # Pure finance functions: Sharpe, drawdown, beta
│   ├── macro_regime_analysis.py       # Regime classification + builder premium by macro regime
│   └── *.csv / *.json                 # Power BI data files
├── edgar_pipeline/
│   └── edgar_pipeline.py             # Airflow DAG: SEC 10-K capex + revenue
├── fred_pipeline/
│   └── fred_pipeline.py              # Airflow DAG: FRED macro indicators
├── analysis_pipeline/
│   └── analysis_pipeline.py          # Airflow DAG: automated backtest trigger
├── forecast_pipeline/
│   └── forecast_pipeline.py          # Airflow DAG: 5-day forecast
├── monitoring/
│   ├── pipeline_monitor.py           # Airflow DAG: health checks
│   └── data_quality.py               # Validation functions
├── tests/
│   ├── conftest.py                   # Shared moto S3/Athena fixtures + Airflow stubs
│   ├── test_utils.py                 # 30 tests for shared utils helpers
│   ├── test_finance_utils.py         # 16 tests for finance math functions
│   ├── test_stock_pipeline.py
│   ├── test_edgar_pipeline.py
│   ├── test_fred_pipeline.py
│   ├── test_historical_backfill.py
│   ├── test_analysis_pipeline.py
│   ├── test_forecast_pipeline.py
│   ├── test_sharpe_calculation.py
│   ├── test_data_quality.py
│   └── test_portfolio_analysis.py
├── queries/
│   └── sample_queries.sql            # Athena SQL showcase queries
├── terraform/
│   ├── main.tf                       # S3, Glue, Athena resource definitions
│   ├── variables.tf
│   └── outputs.tf
├── docker-compose.yaml               # Airflow cluster (6 containers)
├── Makefile                          # make up/down/test/lint/analyze/demo
├── LICENSE                           # MIT
├── .github/workflows/ci.yml          # CI: lint, pytest, bandit, pip-audit, checkov, terraform fmt
├── .github/workflows/codeql.yml      # CodeQL semantic analysis
├── .github/workflows/scorecard.yml   # OpenSSF Scorecard
├── .github/workflows/semgrep.yml     # Semgrep SAST
├── .github/workflows/sbom.yml        # Software bill of materials
├── .github/workflows/dependency-review.yml  # PR dependency review
├── .env.example                      # Credential template
└── README.md
```

## Setup

### Prerequisites
- Docker Desktop
- Python 3.12+
- AWS account (S3, Athena, Glue)
- API keys: Alpha Vantage, FRED (free at fred.stlouisfed.org/docs/api/api_key.html)

### Quick Start
```bash
# 1. Clone the repo
git clone https://github.com/ericg1212/data-engineering-portfolio.git
cd data-engineering-portfolio

# 2. Create .env file with your credentials
cp .env.example .env
# Edit .env with your API keys and AWS credentials

# 3. Start Airflow
docker compose up -d

# 4. Access Airflow UI
# http://localhost:8090 (airflow/airflow)

# 6. Run the historical backtest + portfolio analysis
make analyze
```

### Makefile Commands
```bash
make setup    # Create .env from template
make up       # Start Airflow stack
make down     # Stop Airflow stack
make test     # Run pytest
make lint     # flake8 across all source dirs
make analyze  # Run backtest + portfolio analysis, refresh all CSVs
make demo     # Run full analysis in local mode (no AWS required)
make logs     # Tail scheduler + worker logs
make clean    # Remove __pycache__, logs, stopped containers
```

## Data Sources

| Source | API | Data | Rate Limit |
|--------|-----|------|-----------|
| [Alpha Vantage](https://www.alphavantage.co/) | Stock quotes + monthly history | Daily prices | 25 calls/day (free) |
| [SEC EDGAR](https://www.sec.gov/developer) | Company Facts API | Annual 10-K filings | No limit (free) |
| [FRED](https://fred.stlouisfed.org/docs/api/fred/) | Observations API | Macro indicators | No limit (free, key required) |

---

## Author

**Eric Grynspan** — Data Engineer · Financial Services & Healthcare

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Eric%20Grynspan-0A66C2?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/ericgrynspan/)
