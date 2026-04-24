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

# INCREMENTAL: ถ้า profile เก่ากว่า N วัน ค่อยดึงใหม่
PROFILE_REFRESH_DAYS = 30


def get_sp500_symbols():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    symbols = [doc['symbol'] for doc in db['sp500_tickers'].find({}, {'symbol': 1})]
    client.close()
    if not symbols:
        raise ValueError("ไม่มีข้อมูลใน sp500_tickers — ต้องรัน fetch_sp500_list DAG ก่อน")
    return symbols


def _needs_refresh(profile_doc, threshold_days=PROFILE_REFRESH_DAYS):
    """
    ตรวจว่า profile นี้ควรดึงใหม่หรือไม่
    - ไม่มี profile → True
    - profile เก่ากว่า threshold_days → True
    - profile sector เป็น null (ดึงไม่ครบครั้งก่อน) → True
    """
    if not profile_doc:
        return True

    if not profile_doc.get('sector'):
        return True  # ดึงไม่สำเร็จครั้งก่อน — ลองใหม่

    updated_at = profile_doc.get('updated_at')
    if not updated_at:
        return True

    try:
        last = datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S')
        age_days = (datetime.now() - last).days
        return age_days >= threshold_days
    except (ValueError, TypeError):
        return True  # parse ไม่ได้ → ดึงใหม่เพื่อความปลอดภัย


def fetch_profiles_batch(batch_num):
    """
    ดึง company profile จาก yfinance เป็น batch

    INCREMENTAL PROCESSING:
    - ตรวจ updated_at ของแต่ละ profile ก่อนดึง
    - ดึงเฉพาะที่เก่ากว่า PROFILE_REFRESH_DAYS หรือไม่มี
    - Idempotent ผ่าน update_one + upsert=True
    """
    all_symbols = get_sp500_symbols()

    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(all_symbols))
    batch_symbols = all_symbols[start_idx:end_idx]

    if not batch_symbols:
        logging.info(f"Batch {batch_num}: ไม่มีหุ้น")
        return

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db['company_profiles']

    try:
        # ───── INCREMENTAL: เช็ค profile ที่มีอยู่แล้ว ─────
        existing_profiles = {
            doc['symbol']: doc
            for doc in collection.find(
                {"symbol": {"$in": batch_symbols}},
                {"symbol": 1, "sector": 1, "updated_at": 1}
            )
        }

        symbols_to_fetch = [
            s for s in batch_symbols
            if _needs_refresh(existing_profiles.get(s))
        ]

        skipped = len(batch_symbols) - len(symbols_to_fetch)
        logging.info(
            f"Batch {batch_num}: total {len(batch_symbols)} | "
            f"📥 to fetch: {len(symbols_to_fetch)} | "
            f"⏭️  skipped (recent): {skipped}"
        )

        if not symbols_to_fetch:
            logging.info(f"✅ Batch {batch_num}: all profiles up-to-date — skip (idempotent)")
            return

        fetched, errors = 0, 0
        for symbol in symbols_to_fetch:
            try:
                time.sleep(2)

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

                # IDEMPOTENT: update_one + upsert
                collection.update_one(
                    {"symbol": symbol},
                    {"$set": profile},
                    upsert=True
                )
                fetched += 1
                logging.info(f"  ✅ {symbol}: OK ({info.get('sector', 'N/A')})")

            except Exception as e:
                errors += 1
                logging.error(f"  ❌ {symbol} error: {str(e)}")
                continue

        logging.info(
            f"📊 Batch {batch_num} summary: "
            f"fetched={fetched}, skipped={skipped}, errors={errors}"
        )
    finally:
        client.close()