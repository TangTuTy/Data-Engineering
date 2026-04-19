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
BATCH_SIZE = 50  # yf.download รองรับหลายตัวพร้อมกัน เร็วกว่าทีละตัวมาก


def get_sp500_symbols():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    symbols = [doc['symbol'] for doc in db['sp500_tickers'].find({}, {'symbol': 1})]
    client.close()
    if not symbols:
        raise ValueError("ไม่มีข้อมูลใน sp500_tickers — ต้องรัน fetch_sp500_list DAG ก่อน")
    return symbols


def backfill_batch(batch_num):
    """ดึงราคาหุ้นย้อนหลัง 1 ปี ด้วย yf.download() (เร็วกว่า Ticker.history 10x)"""
    all_symbols = get_sp500_symbols()

    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(all_symbols))
    batch_symbols = all_symbols[start_idx:end_idx]

    if not batch_symbols:
        logging.info(f"Batch {batch_num}: ไม่มีหุ้นให้ดึง")
        return

    logging.info(f"Batch {batch_num}: ดึง {len(batch_symbols)} ตัวพร้อมกัน")

    # หน่วงเวลาระหว่าง batch กัน spam
    if batch_num > 0:
        time.sleep(10)

    df = yf.download(
        tickers=batch_symbols,
        period="1y",
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
                    sdf['Date'] = pd.to_datetime(sdf['Date']).dt.strftime('%Y-%m-%d')

                records = sdf.to_dict('records')
                for record in records:
                    # ลบ NaN values
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
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='sp500_backfill_1y',
    default_args=default_args,
    description='Backfill S&P 500 ราคาหุ้นย้อนหลัง 1 ปี (batch download, เร็ว)',
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'sp500', 'backfill'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # 503 / 50 = ~11 batches
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
