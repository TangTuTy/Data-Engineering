from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from bronze.company_profiles_layer import fetch_profiles_batch

default_args = {
    'owner': 'Ake',
    'start_date': datetime(2026, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='sp500_company_profiles_pipeline',
    default_args=default_args,
    description='ดึง Company Profile ของ S&P 500 จาก yfinance ลง MongoDB',
    schedule_interval='@monthly',
    catchup=False,
    tags=['bronze', 'sp500', 'dimension'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    prev = start
    for batch_num in range(26):
        task = PythonOperator(
            task_id=f'profiles_batch_{batch_num:02d}',
            python_callable=fetch_profiles_batch,
            op_kwargs={'batch_num': batch_num},
        )
        prev >> task
        prev = task
    prev >> end