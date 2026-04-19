from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import logging
import requests
from bs4 import BeautifulSoup

MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'stock_database'


def fetch_sp500_list():
    """ดึงรายชื่อหุ้น S&P 500 จาก Wikipedia แล้วเก็บลง MongoDB"""
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

    logging.info("กำลังดึงรายชื่อ S&P 500 จาก Wikipedia...")
    headers = {'User-Agent': 'Mozilla/5.0 (stock-pipeline/1.0)'}
    resp = requests.get(url, timeout=30, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table', {'id': 'constituents'})

    if not table:
        raise ValueError("ไม่เจอตาราง S&P 500 บน Wikipedia")

    rows = table.find_all('tr')[1:]

    tickers = []
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 8:
            symbol = cols[0].text.strip().replace('.', '-')
            tickers.append({
                "symbol": symbol,
                "company_name": cols[1].text.strip(),
                "gics_sector": cols[2].text.strip(),
                "gics_sub_industry": cols[3].text.strip(),
                "headquarters": cols[4].text.strip(),
                "date_added": cols[5].text.strip(),
                "cik": cols[6].text.strip(),
                "founded": cols[7].text.strip(),
                "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })

    logging.info(f"พบหุ้น {len(tickers)} ตัว")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db['sp500_tickers']

    try:
        collection.delete_many({})
        if tickers:
            collection.insert_many(tickers)
            collection.create_index("symbol", unique=True)
            collection.create_index("gics_sector")
        logging.info(f"บันทึก {len(tickers)} หุ้นลง sp500_tickers สำเร็จ")
    finally:
        client.close()


default_args = {
    'owner': 'Tang',
    'start_date': datetime(2026, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='fetch_sp500_list',
    default_args=default_args,
    description='ดึงรายชื่อหุ้น S&P 500 จาก Wikipedia ลง MongoDB',
    schedule_interval='@weekly',
    catchup=False,
    tags=['bronze', 'sp500'],
) as dag:

    PythonOperator(
        task_id='fetch_sp500_list',
        python_callable=fetch_sp500_list,
    )
