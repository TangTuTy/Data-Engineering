from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import yfinance as yf
from pymongo import MongoClient
import logging
import time

MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'stock_database'
BATCH_SIZE = 20  # profile ต้องทีละตัว แต่จัด batch เพื่อจัดการ retry


def get_sp500_symbols():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    symbols = [doc['symbol'] for doc in db['sp500_tickers'].find({}, {'symbol': 1})]
    client.close()
    if not symbols:
        raise ValueError("ไม่มีข้อมูลใน sp500_tickers — ต้องรัน fetch_sp500_list DAG ก่อน")
    return symbols


def fetch_profiles_batch(batch_num):
    """ดึง company profile จาก yfinance เป็น batch"""
    all_symbols = get_sp500_symbols()

    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(all_symbols))
    batch_symbols = all_symbols[start_idx:end_idx]

    if not batch_symbols:
        logging.info(f"Batch {batch_num}: ไม่มีหุ้น")
        return

    logging.info(f"Batch {batch_num}: ดึง profile {len(batch_symbols)} ตัว")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db['company_profiles']

    try:
        for symbol in batch_symbols:
            try:
                time.sleep(2)  # หน่วง 2 วินาที กัน spam

                ticker = yf.Ticker(symbol)
                info = ticker.info

                profile = {
                    "symbol": symbol,
                    "full_name": info.get("longName"),
                    "sector": info.get("sector"),
                    "industry": info.get("industry"),
                    "market_cap": info.get("marketCap"),
                    "business_summary": info.get("longBusinessSummary"),
                    "updated_at": time.strftime('%Y-%m-%d %H:%M:%S'),
                }

                collection.update_one(
                    {"symbol": symbol},
                    {"$set": profile},
                    upsert=True
                )
                logging.info(f"  {symbol}: OK ({info.get('sector', 'N/A')})")

            except Exception as e:
                logging.error(f"  {symbol} error: {str(e)}")
                continue
    finally:
        client.close()


default_args = {
    'owner': 'Ake',
    'start_date': datetime(2026, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='sp500_company_profiles',
    default_args=default_args,
    description='ดึง Company Profile ของ S&P 500 จาก yfinance ลง MongoDB',
    schedule_interval='@monthly',
    catchup=False,
    tags=['bronze', 'sp500', 'dimension'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    prev = start
    for batch_num in range(26):  # 503 / 20 = ~26 batches
        task = PythonOperator(
            task_id=f'profiles_batch_{batch_num:02d}',
            python_callable=fetch_profiles_batch,
            op_kwargs={'batch_num': batch_num},
        )
        prev >> task
        prev = task
    prev >> end
