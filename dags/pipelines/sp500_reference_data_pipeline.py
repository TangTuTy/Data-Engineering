

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from bronze.reference_data import load_sp500_tickers_to_bronze


default_args = {
    "owner": "mara",
}

with DAG(
    dag_id="sp500_reference_data_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="0 9 * * 1",
    catchup=False,
    tags=["sp500", "reference", "bronze"],
) as dag:

    load_sp500_tickers_task = PythonOperator(
        task_id="load_sp500_tickers_to_bronze",
        python_callable=load_sp500_tickers_to_bronze,
    )