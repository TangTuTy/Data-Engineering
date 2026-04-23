from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from bronze.backfill_layer import backfill_batch

default_args = {
    'owner': 'Ake',
    'start_date': datetime(2026, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='sp500_backfill_pipeline',
    default_args=default_args,
    description='Backfill S&P 500 ราคาหุ้นย้อนหลัง 1 ปี (batch download, เร็ว)',
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'sp500', 'backfill'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    prev = start
    for batch_num in range(11):
        task = PythonOperator(
            task_id=f'backfill_batch_{batch_num:02d}',
            python_callable=backfill_batch,
            op_kwargs={'batch_num': batch_num},
        )
        prev >> task
        prev = task
    prev >> end