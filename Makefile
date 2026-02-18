.PHONY: help setup up down restart logs test lint clean dags status

help:
	@echo "Available commands:"
	@echo "  make setup    - Copy DAGs and create .env from template"
	@echo "  make up       - Start Airflow stack (Docker Compose)"
	@echo "  make down     - Stop Airflow stack"
	@echo "  make restart  - Restart Airflow stack"
	@echo "  make logs     - Tail Airflow logs"
	@echo "  make test     - Run pytest unit tests"
	@echo "  make lint     - Lint Python files with flake8"
	@echo "  make dags     - Copy pipeline files into ./dags"
	@echo "  make status   - Show running containers"
	@echo "  make clean    - Remove logs, __pycache__, and stopped containers"

setup: dags
	@if [ ! -f .env ]; then cp .env.example .env && echo "Created .env - fill in your API keys"; fi

dags:
	@mkdir -p dags
	cp config.py dags/
	cp stock_pipeline/stock_pipeline.py dags/
	cp crypto_pipeline/crypto_pipeline.py dags/
	cp weather_pipeline/weather_pipeline.py dags/
	cp monitoring/pipeline_monitor.py dags/
	cp monitoring/data_quality.py dags/
	@echo "DAGs copied to ./dags"

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
	flake8 stock_pipeline/ crypto_pipeline/ weather_pipeline/ monitoring/ tests/ --max-line-length=120

clean:
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@rm -rf logs/scheduler logs/worker logs/triggerer 2>/dev/null || true
	docker compose rm -f
	@echo "Cleaned up"
