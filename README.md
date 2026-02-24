# Own the Model, Own the Returns

A data engineering pipeline that analyzes whether building proprietary AI delivers superior risk-adjusted returns versus integrating third-party AI. Ingests daily stock data for 10 tech companies via Apache Airflow on Docker, stores in AWS S3, queries with Athena, and visualizes findings in Power BI.

## Key Finding: The Market Rewards AI Builders, Not AI Renters

Analysis of risk-adjusted returns (Jan 2023 – present) across 10 major tech stocks reveals a clear **AI value chain hierarchy** in Sharpe ratios:

| Tier | Companies | Avg Sharpe | AI Strategy |
|------|-----------|-----------|-------------|
| Infrastructure | NVDA | 3.180 | Sells the GPUs |
| AI Builders | META, GOOGL | 1.985 | Proprietary AI (Llama, Gemini, custom chips) |
| AI Integrators | MSFT, AMZN | 0.939 | Third-party partnerships (OpenAI, Anthropic) |
| Control | AAPL, TSLA | 1.131 | Mixed AI exposure |
| Legacy Tech | CRM, ORCL, ADBE | 0.247 | Traditional software |

**Builder Premium: +111.5%** - Companies building proprietary AI outperform those renting it through partnerships by 111% on risk-adjusted returns. The premium has widened from +58.4% (Dec 2025) as AI integrators weakened in early 2026 while builders held up.

![Dashboard](dashboard.png)

In 2026, Big Tech will spend ~$650B on AI infrastructure. But spending more doesn't mean earning more - Meta spends the least of the four hybrids ($125B) yet delivers the highest Sharpe ratio (2.138) because nearly 100% of its capex goes to proprietary AI. Amazon spends the most ($200B) but dilutes returns across logistics and third-party partnerships.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────┐     ┌─────────┐
│  Data Source │────>│ Apache Airflow│────>│  AWS S3   │────>│ Athena  │
│              │     │  (Docker)    │     │ (Data Lake)│    │ (Query) │
│ Alpha Vantage│     │              │     │           │     └────┬────┘
│  (10 stocks) │     │ Scheduled    │     │ Partitioned│         │
│              │     │ DAGs w/      │     │ by date   │    ┌────▼────┐
└─────────────┘     │ rate limiting│     └───────────┘    │ Power BI│
                    └──────────────┘                       │(Dashboard)│
                                                           └─────────┘
```

## Stock Pipeline (`stock_pipeline.py`)
- **Stocks:** NVDA, MSFT, GOOGL, AMZN, META, CRM, ORCL, ADBE, AAPL, TSLA
- **Source:** Alpha Vantage API (Global Quote)
- **Schedule:** 5 PM ET Mon-Fri (after market close)
- **Rate limiting:** 12-second intervals for free-tier compliance

### Additional Pipelines

The repo also includes pipelines demonstrating multi-source ingestion:
- **Crypto** (`crypto_pipeline.py`): BTC, ETH, SOL via Coinbase API (6-hour schedule)
- **Weather** (`weather_pipeline.py`): Brooklyn, NY via OpenWeatherMap API (daily)
- **Forecast** (`forecast_pipeline.py`): 5-day weather forecast via OpenWeatherMap API (daily at 6 AM)
- **Monitor** (`pipeline_monitor.py`): Health checks across all pipelines

## Historical Backtest (`historical_backtest.py`)

Pulls 3 years of monthly adjusted close prices and calculates:
- Annualized return, volatility, and Sharpe ratio per stock
- Category-level averages across the AI value chain
- Build vs. Rent premium (proprietary AI vs. partnership AI)
- Capex efficiency (Sharpe per $B of estimated AI spend, sourced from 2025/2026 earnings guidance)
- Spearman rank correlation between AI% of capex and Sharpe ratio

## Infrastructure as Code

AWS resources are defined in Terraform under `terraform/`, making the infrastructure fully reproducible:
- **S3 bucket** - Data lake for stocks, crypto, weather, and forecast data
- **Glue catalog database + 4 tables** - Schema definitions for Athena queries (stocks, crypto, weather, forecast)
- **Athena workgroup** - Query engine with S3 results location

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
| Storage | AWS S3 (NDJSON, date-partitioned) |
| Query Engine | AWS Athena (Presto SQL) |
| Visualization | Power BI |
| Language | Python 3.12 |
| Key Libraries | boto3, pandas, numpy, requests |

## Security

All credentials managed via environment variables - zero hardcoded secrets:
- AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- API keys (`ALPHA_VANTAGE_API_KEY`, `OPENWEATHER_API_KEY`)
- Injected into Airflow containers via Docker Compose `.env` file
- `.gitignore` prevents credential files from being committed

## Project Structure

```
data-engineering-portfolio/
├── config.py                          # Central constants (symbols, S3 bucket, capex data)
├── stock_pipeline/
│   ├── stock_pipeline.py              # Airflow DAG: 10-stock ingestion
│   ├── historical_backtest.py         # 3-year Sharpe ratio analysis
│   ├── portfolio_analysis.py          # Build vs Rent + capex efficiency
│   ├── backtest_results.json          # Stock-level results
│   ├── backtest_results.csv           # Stock-level CSV
│   ├── powerbi_master.csv             # Power BI master dataset
│   ├── build_vs_rent.csv              # Builders vs Integrators comparison
│   ├── capex_efficiency.csv           # Sharpe per $B of AI spend
│   ├── category_summary.csv           # Category-level averages
│   └── value_chain_summary.csv        # Full value chain rankings
├── crypto_pipeline/
│   └── crypto_pipeline.py             # Airflow DAG: BTC, ETH, SOL
├── weather_pipeline/
│   └── weather_pipeline.py            # Airflow DAG: Brooklyn weather
├── forecast_pipeline/
│   └── forecast_pipeline.py           # Airflow DAG: 5-day weather forecast
├── monitoring/
│   ├── pipeline_monitor.py            # Airflow DAG: health checks
│   └── data_quality.py                # Validation functions
├── tests/
│   ├── conftest.py                    # Shared pytest fixtures
│   ├── test_sharpe_calculation.py     # Sharpe ratio unit tests
│   ├── test_data_quality.py           # Data validation tests
│   ├── test_portfolio_analysis.py     # Portfolio analysis tests
│   └── test_forecast_pipeline.py      # Forecast pipeline tests
├── queries/
│   └── sample_queries.sql             # Athena SQL showcase queries
├── terraform/
│   ├── main.tf                        # S3, Glue, Athena resource definitions
│   ├── variables.tf                   # Configurable parameters
│   └── outputs.tf                     # Resource ARNs and names
├── conftest.py                        # Root pytest conftest (sys.path setup)
├── docker-compose.yaml                # Airflow cluster (6 containers)
├── Makefile                           # dev shortcuts (make up/down/dags/test/lint)
├── pytest.ini                         # Test configuration
├── requirements.txt                   # Production dependencies
├── requirements-dev.txt               # Dev/test dependencies
├── .env.example                       # Template for credentials
├── .gitignore
└── README.md
```

## Setup

### Prerequisites
- Docker Desktop
- Python 3.12+
- AWS account (S3, Athena)
- API keys: Alpha Vantage, OpenWeatherMap

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

# 4. Copy DAGs to Airflow (also copies config.py)
make dags

# 5. Access Airflow UI
# http://localhost:8090 (airflow/airflow)

# 6. Run the historical backtest
python stock_pipeline/historical_backtest.py
```

### Running Tests
```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run all tests
pytest tests/ -v

# Run a specific test file
pytest tests/test_sharpe_calculation.py -v

# Lint
make lint
```

### Makefile Commands
```bash
make setup    # Copy DAGs + create .env from template
make up       # Start Airflow stack
make down     # Stop Airflow stack
make dags     # Copy pipeline files (incl. config.py) into ./dags
make test     # Run pytest
make lint     # flake8 across all source dirs
make logs     # Tail scheduler + worker logs
make clean    # Remove __pycache__, logs, stopped containers
```

## Data Sources

| Source | API | Rate Limit |
|--------|-----|-----------|
| [Alpha Vantage](https://www.alphavantage.co/) | Stock quotes + monthly history | 25 calls/day (free) |
| [Coinbase](https://docs.cdp.coinbase.com/coinbase-app/docs/api-prices) | Crypto spot prices | No limit (public) |
| [OpenWeatherMap](https://openweathermap.org/api) | Current weather | 1,000 calls/day (free) |
