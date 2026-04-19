from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from pymongo import MongoClient
import logging
import time

MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'stock_database'
BATCH_SIZE = 50


def get_sp500_symbols():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    symbols = [doc['symbol'] for doc in db['sp500_tickers'].find({}, {'symbol': 1})]
    client.close()
    if not symbols:
        raise ValueError("ไม่มีข้อมูลใน sp500_tickers — ต้องรัน fetch_sp500_list DAG ก่อน")
    return symbols


def extract_daily_batch(batch_num):
    """ดึงราคาหุ้นย้อนหลัง 5 วัน ด้วย yf.download() (batch)"""
    all_symbols = get_sp500_symbols()

    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(all_symbols))
    batch_symbols = all_symbols[start_idx:end_idx]

    if not batch_symbols:
        logging.info(f"Batch {batch_num}: ไม่มีหุ้น")
        return

    logging.info(f"Batch {batch_num}: ดึง daily {len(batch_symbols)} ตัวพร้อมกัน")

    if batch_num > 0:
        time.sleep(10)

    df = yf.download(
        tickers=batch_symbols,
        period="5d",
        interval="1d",
        group_by="ticker",
        threads=True,
    )

    if df.empty:
        logging.warning(f"Batch {batch_num}: ไม่มีข้อมูล")
        return

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db['historical_prices']

    try:
        total = 0
        for symbol in batch_symbols:
            try:
                if len(batch_symbols) == 1:
                    sdf = df.copy()
                else:
                    if symbol not in df.columns.get_level_values(0):
                        continue
                    sdf = df[symbol].copy()

                sdf = sdf.dropna(how='all')
                if sdf.empty:
                    continue

                sdf = sdf.reset_index()
                sdf['symbol'] = symbol

                if 'Date' in sdf.columns:
                    sdf['DateTime'] = pd.to_datetime(sdf['Date'], utc=True)
                    sdf['Date'] = sdf['DateTime'].dt.strftime('%Y-%m-%d')

                records = sdf.to_dict('records')
                for record in records:
                    record = {k: v for k, v in record.items() if pd.notna(v)}
                    record['symbol'] = symbol
                    collection.update_one(
                        {"Date": record.get('Date'), "symbol": symbol},
                        {"$set": record},
                        upsert=True
                    )

                total += len(records)
                logging.info(f"  {symbol}: {len(records)} rows")

            except Exception as e:
                logging.error(f"  {symbol} error: {e}")
                continue

        logging.info(f"Batch {batch_num}: total {total} records saved")
    finally:
        client.close()


default_args = {
    'owner': 'Ake',
    'start_date': datetime(2026, 4, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='sp500_daily',
    default_args=default_args,
    description='ดึงราคาหุ้น S&P 500 รายวัน (batch download, เร็ว)',
    schedule_interval='0 18 * * 1-5',
    catchup=False,
    tags=['bronze', 'sp500', 'daily'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    prev = start
    for batch_num in range(11):
        task = PythonOperator(
            task_id=f'daily_batch_{batch_num:02d}',
            python_callable=extract_daily_batch,
            op_kwargs={'batch_num': batch_num},
        )
        prev >> task
        prev = task
    prev >> end
