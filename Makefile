.PHONY: help setup up down restart logs test lint clean status analyze regime demo

help:
	@echo "Available commands:"
	@echo "  make setup    - Create .env from template"
	@echo "  make up       - Start Airflow stack (Docker Compose)"
	@echo "  make down     - Stop Airflow stack"
	@echo "  make restart  - Restart Airflow stack"
	@echo "  make logs     - Tail Airflow logs"
	@echo "  make test     - Run pytest unit tests"
	@echo "  make lint     - Lint Python files with flake8"
	@echo "  make status   - Show running containers"
	@echo "  make clean    - Remove logs, __pycache__, and stopped containers"
	@echo "  make analyze  - Run backtest + portfolio analysis, refresh all CSVs"
	@echo "  make regime   - Run macro regime analysis (builder premium by regime)"
	@echo "  make demo     - Run full analysis in local mode (no AWS required)"

setup:
	@if [ ! -f .env ]; then cp .env.example .env && echo "Created .env - fill in your API keys"; fi

up:
	docker compose up -d

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f airflow-scheduler airflow-worker

status:
	docker compose ps

test:
	pytest tests/ -v

lint:
	flake8 stock_pipeline/ crypto_pipeline/ weather_pipeline/ edgar_pipeline/ fred_pipeline/ analysis_pipeline/ monitoring/ tests/

analyze:
	@set -a && source .env && set +a && \
	python stock_pipeline/historical_backtest.py && \
	python stock_pipeline/portfolio_analysis.py
	@echo "All CSVs refreshed — refresh Power BI to update dashboard"

regime:
	@set -a && source .env && set +a && \
	python stock_pipeline/macro_regime_analysis.py
	@echo "Regime analysis complete — check stock_pipeline/regime_*.csv"

demo:
	@echo "Running portfolio demo (local mode — no AWS required)..."
	python stock_pipeline/historical_backtest.py --local
	python stock_pipeline/portfolio_analysis.py --local
	@echo "Demo complete. Results in stock_pipeline/*.csv"

clean:
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@rm -rf logs/scheduler logs/worker logs/triggerer 2>/dev/null || true
	docker compose rm -f
	@echo "Cleaned up"
