"""
Analysis Pipeline: Runs Sharpe backtest + portfolio analysis automatically
after the daily stock pipeline completes.

Replaces the manual `make analyze` command. Runs at 5:30 PM Mon-Fri —
30 minutes after the stock pipeline (5:00 PM) to ensure stock data is loaded.

Output CSVs are written to stock_pipeline/ on the host (mounted at
/opt/airflow/stock_pipeline/ in the container) — Power BI paths unchanged.

Note: Reads from Alpha Vantage directly until Session 11, when
historical_backtest.py is updated to read from Athena instead.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging


def log_failure(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    logging.error(f"DAG {dag_id} task {task_id} failed at {execution_date}")


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
# stock_pipeline/ is mounted at /opt/airflow/stock_pipeline/ via docker-compose.
run_backtest = BashOperator(
    task_id='run_backtest',
    bash_command=(
        'PYTHONPATH=/opt/airflow/dags '
        'python /opt/airflow/stock_pipeline/historical_backtest.py'
    ),
    dag=dag,
)

run_portfolio_analysis = BashOperator(
    task_id='run_portfolio_analysis',
    bash_command=(
        'PYTHONPATH=/opt/airflow/dags '
        'python /opt/airflow/stock_pipeline/portfolio_analysis.py'
    ),
    dag=dag,
)

run_backtest >> run_portfolio_analysis
