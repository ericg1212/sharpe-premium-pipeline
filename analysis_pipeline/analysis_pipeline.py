"""
Analysis Pipeline: Runs Sharpe backtest + portfolio analysis automatically
after the daily stock pipeline completes.

Replaces the manual `make analyze` command. Runs at 5:30 PM Mon-Fri —
30 minutes after the stock pipeline (5:00 PM) to ensure stock data is loaded.

Output CSVs are written to data/exports/ on the host (mounted at
/opt/airflow/data/exports/ in the container) — Power BI sources read from there.

Note: Reads from Alpha Vantage directly until Session 11, when
historical_backtest.py is updated to read from Athena instead.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402
from datetime import datetime, timedelta  # noqa: E402
from utils import log_failure  # noqa: E402


default_args = {
    'owner': 'eric',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 26),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': log_failure,
}

dag = DAG(
    'analysis_pipeline',
    default_args=default_args,
    description='Run Sharpe backtest + portfolio analysis after daily stock load',
    schedule_interval='30 17 * * 1-5',  # 5:30 PM Mon-Fri
    catchup=False,
)

# PYTHONPATH points to dags/ so config.py is importable inside the container.
# data/ is mounted writable at /opt/airflow/data/ via docker-compose for output CSVs.
run_backtest = BashOperator(
    task_id='run_backtest',
    bash_command=(
        'PYTHONPATH=/opt/airflow/repo '
        'python /opt/airflow/repo/stock_pipeline/historical_backtest.py'
    ),
    dag=dag,
)

run_portfolio_analysis = BashOperator(
    task_id='run_portfolio_analysis',
    bash_command=(
        'PYTHONPATH=/opt/airflow/repo '
        'python /opt/airflow/repo/stock_pipeline/portfolio_analysis.py'
    ),
    dag=dag,
)

run_backtest >> run_portfolio_analysis
