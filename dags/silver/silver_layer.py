from pymongo import MongoClient, UpdateOne
from collections import defaultdict
import logging
import statistics
from datetime import datetime, date

from utils.data_quality import DataQualityChecker

MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'stock_database'
WAR_START_DATE = datetime.strptime('2026-01-01', '%Y-%m-%d').date()
PIPELINE_STATE_COLLECTION = 'pipeline_state'  # เก็บ watermark ของแต่ละ pipeline


def _get_watermark(db, pipeline_name):
    """อ่าน watermark (วันที่ล่าสุดที่ process แล้ว) จาก pipeline_state"""
    doc = db[PIPELINE_STATE_COLLECTION].find_one({"pipeline": pipeline_name})
    if doc and doc.get("last_processed_date"):
        return _to_date(doc["last_processed_date"])
    return None


def _set_watermark(db, pipeline_name, latest_date, records_processed):
    """อัปเดต watermark — idempotent (upsert)"""
    db[PIPELINE_STATE_COLLECTION].update_one(
        {"pipeline": pipeline_name},
        {"$set": {
            "pipeline": pipeline_name,
            "last_processed_date": latest_date.isoformat() if latest_date else None,
            "last_run_at": datetime.utcnow().isoformat(),
            "records_processed_last_run": records_processed,
        }},
        upsert=True,
    )

def _to_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def get_sp500_symbols():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    symbols = [doc['symbol'] for doc in db['sp500_tickers'].find({}, {'symbol': 1})]
    client.close()
    return symbols


def get_sector_map():
    """ดึง sector mapping จาก company_profiles (yfinance) + sp500_tickers (Wikipedia GICS)"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    sector_map = {}
    for doc in db['company_profiles'].find({}, {'symbol': 1, 'sector': 1, 'industry': 1}):
        if doc.get('sector'):
            sector_map[doc['symbol']] = {
                'sector': doc['sector'],
                'industry': doc.get('industry', 'Unknown'),
            }
    for doc in db['sp500_tickers'].find({}, {'symbol': 1, 'gics_sector': 1, 'gics_sub_industry': 1}):
        if doc['symbol'] not in sector_map and doc.get('gics_sector'):
            sector_map[doc['symbol']] = {
                'sector': doc['gics_sector'],
                'industry': doc.get('gics_sub_industry', 'Unknown'),
            }
    SECTOR_NORMALIZE = {
        "Financials": "Financial Services",
    }
    for sym in sector_map:
        raw = sector_map[sym]['sector']
        sector_map[sym]['sector'] = SECTOR_NORMALIZE.get(raw, raw)

    client.close()
    return sector_map


def transform_historical_to_silver():
    """
    Bronze -> Silver: historical_prices -> silver_historical_daily

    INCREMENTAL PROCESSING (Phase 2 Rubric):
    - ใช้ watermark pattern (last_processed_date) เพื่อ process เฉพาะข้อมูลใหม่
    - ใช้ bulk upsert เพื่อ idempotency (รันซ้ำได้ ไม่เกิดข้อมูลซ้ำ)
    - First run = full load (เพื่อคำนวณ moving average ย้อนหลัง)
    - Subsequent runs = incremental + lookback 30 วัน (สำหรับ moving avg)
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    bronze = db['historical_prices']
    silver = db['silver_historical_daily']
    sp500_symbols = get_sp500_symbols()
    sector_map = get_sector_map()

    PIPELINE = "silver_historical_daily"

    try:
        # ───── INCREMENTAL: อ่าน watermark ─────
        watermark = _get_watermark(db, PIPELINE)
        if watermark:
            # มี watermark = ไม่ใช่ first run
            # Process records ใหม่ + lookback 30 วัน (สำหรับคำนวณ moving avg ของวันใหม่)
            from datetime import timedelta
            lookback_date = watermark - timedelta(days=30)
            logging.info(f"📥 INCREMENTAL run — watermark={watermark}, lookback from {lookback_date}")
        else:
            lookback_date = None
            logging.info(f"📥 FIRST run — processing all historical data")

        # ───── Build query ─────
        query = {}
        if sp500_symbols:
            query["symbol"] = {"$in": sp500_symbols}
        if lookback_date:
            # MongoDB Date field เก็บเป็นได้หลายแบบ (string/datetime) — รองรับทั้งคู่
            query["Date"] = {"$gte": lookback_date.isoformat()}

        cursor = bronze.find(query, {"_id": 0}).sort([("symbol", 1), ("Date", 1)])
        records = list(cursor)
        logging.info(f"Bronze records to process: {len(records)}")

        if not records:
            logging.info("✅ No new data — pipeline is up to date (idempotent)")
            return

        grouped = defaultdict(list)
        for r in records:
            grouped[r['symbol']].append(r)

        silver_records = []
        for symbol, rows in grouped.items():
            rows = sorted(rows, key=lambda x: _to_date(x.get('Date')) or date.min)
            meta = sector_map.get(symbol, {})
            prev_close = None
            for i, row in enumerate(rows):
                date_value = row.get('Date')
                normalized_date = _to_date(date_value)
                close = row.get('Close')
                high = row.get('High')
                low = row.get('Low')
                open_price = row.get('Open')
                volume = row.get('Volume')

                daily_return_pct = None
                if prev_close is not None and prev_close != 0 and close is not None:
                    daily_return_pct = round(((close - prev_close) / prev_close) * 100, 4)

                period = "war" if normalized_date and normalized_date >= WAR_START_DATE else "pre_war"

                rec = {
                    "symbol": symbol,
                    "date": date_value,
                    "open": round(open_price, 2) if open_price is not None else None,
                    "high": round(high, 2) if high is not None else None,
                    "low": round(low, 2) if low is not None else None,
                    "close": round(close, 2) if close is not None else None,
                    "volume": volume,
                    "daily_return_pct": daily_return_pct,
                    "sector": meta.get("sector", "Unknown"),
                    "industry": meta.get("industry", "Unknown"),
                    "period": period,
                }

                if i >= 6:
                    c7 = [rows[j].get('Close') for j in range(i-6, i+1) if rows[j].get('Close') is not None]
                    rec['moving_avg_7d'] = round(sum(c7) / len(c7), 2) if c7 else None
                else:
                    rec['moving_avg_7d'] = None

                if i >= 29:
                    c30 = [rows[j].get('Close') for j in range(i-29, i+1) if rows[j].get('Close') is not None]
                    rec['moving_avg_30d'] = round(sum(c30) / len(c30), 2) if c30 else None
                else:
                    rec['moving_avg_30d'] = None

                silver_records.append(rec)
                prev_close = close

        # Dedupe in-memory (เผื่อ Bronze มี duplicate)
        seen = {}
        for r in silver_records:
            key = (r['symbol'], r['date'])
            seen[key] = r
        silver_records = list(seen.values())

        # ───── Data Quality Gate ─────
        dq = DataQualityChecker(stage="silver_historical_daily")
        dq.check_completeness(silver_records, ["symbol", "date", "close"])
        dq.check_uniqueness(silver_records, ["symbol", "date"])
        dq.check_validity(silver_records, "close", min_value=0)
        dq.check_validity(silver_records, "volume", min_value=0)
        dq.check_referential_integrity(silver_records, "symbol", sp500_symbols)
        dq.run()

        # ───── IDEMPOTENT UPSERT (แทน delete-then-insert) ─────
        # Bulk upsert: ถ้ามีอยู่แล้ว → update, ถ้าไม่มี → insert
        # รันซ้ำได้ ผลเหมือนเดิม (idempotent)
        if silver_records:
            # สร้าง index ก่อน (ถ้ายังไม่มี)
            silver.create_index([("symbol", 1), ("date", 1)], unique=True)
            silver.create_index("sector")
            silver.create_index("period")

            # Bulk upsert operations
            operations = [
                UpdateOne(
                    {"symbol": r["symbol"], "date": r["date"]},  # filter (composite key)
                    {"$set": r},                                  # update
                    upsert=True,                                  # insert ถ้าไม่มี
                )
                for r in silver_records
            ]

            # ทำเป็น batch ละ 1000 (เร็วกว่า + ใช้ memory น้อยกว่า)
            BATCH_SIZE = 1000
            total_upserted = 0
            total_modified = 0
            for i in range(0, len(operations), BATCH_SIZE):
                batch = operations[i:i + BATCH_SIZE]
                result = silver.bulk_write(batch, ordered=False)
                total_upserted += result.upserted_count
                total_modified += result.modified_count

            logging.info(f"✅ Bulk upsert: {total_upserted} new + {total_modified} updated")

        # ───── อัปเดต watermark ─────
        latest_date = max(_to_date(r['date']) for r in silver_records if _to_date(r.get('date')))
        _set_watermark(db, PIPELINE, latest_date, len(silver_records))
        logging.info(f"💾 Watermark updated to {latest_date}")

        logging.info(f"Silver historical: {len(silver_records)} records processed")
    finally:
        client.close()


def classify_war_impact():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    silver_daily = db['silver_historical_daily']

    try:
        all_records = list(silver_daily.find({}, {"_id": 0}))
        logging.info(f"Total silver records: {len(all_records)}")

        by_symbol = defaultdict(list)
        for r in all_records:
            by_symbol[r['symbol']].append(r)

        stock_metrics = []
        for symbol, rows in by_symbol.items():
            pre_war_returns = [r['daily_return_pct'] for r in rows
                             if r.get('period') == 'pre_war' and r.get('daily_return_pct') is not None]
            war_returns = [r['daily_return_pct'] for r in rows
                          if r.get('period') == 'war' and r.get('daily_return_pct') is not None]

            pre_rows = sorted(
                [r for r in rows if r.get('period') == 'pre_war'],
                key=lambda x: _to_date(x.get('date')) or date.min,
            )
            war_rows = sorted(
                [r for r in rows if r.get('period') == 'war'],
                key=lambda x: _to_date(x.get('date')) or date.min,
            )

            pre_cum = None
            if (
                pre_rows
                and pre_rows[0].get('close') is not None
                and pre_rows[-1].get('close') is not None
                and pre_rows[0]['close'] != 0
            ):
                pre_cum = round(((pre_rows[-1]['close'] - pre_rows[0]['close']) / pre_rows[0]['close']) * 100, 2)

            war_cum = None
            if (
                war_rows
                and war_rows[0].get('close') is not None
                and war_rows[-1].get('close') is not None
                and war_rows[0]['close'] != 0
            ):
                war_cum = round(((war_rows[-1]['close'] - war_rows[0]['close']) / war_rows[0]['close']) * 100, 2)

            sector = rows[0].get('sector', 'Unknown')
            industry = rows[0].get('industry', 'Unknown')

            perf_shift = None
            pre_avg = round(statistics.mean(pre_war_returns), 4) if pre_war_returns else None
            war_avg = round(statistics.mean(war_returns), 4) if war_returns else None
            if war_avg is not None and pre_avg is not None:
                perf_shift = round(war_avg - pre_avg, 4)

            stock_metrics.append({
                "symbol": symbol,
                "sector": sector,
                "industry": industry,
                "pre_war_avg_daily_return": round(statistics.mean(pre_war_returns), 4) if pre_war_returns else None,
                "war_avg_daily_return": round(statistics.mean(war_returns), 4) if war_returns else None,
                "pre_war_volatility": round(statistics.stdev(pre_war_returns), 4) if len(pre_war_returns) > 1 else None,
                "war_volatility": round(statistics.stdev(war_returns), 4) if len(war_returns) > 1 else None,
                "pre_war_cumulative_return_pct": pre_cum,
                "war_cumulative_return_pct": war_cum,
                "performance_shift": perf_shift,
                "pre_war_days": len(pre_war_returns),
                "war_days": len(war_returns),
            })

        sector_stocks = defaultdict(list)
        for m in stock_metrics:
            sector_stocks[m['sector']].append(m)

        sector_summary = []
        for sector, stocks in sector_stocks.items():
            shifts = [s['performance_shift'] for s in stocks if s['performance_shift'] is not None]
            war_cums = [s['war_cumulative_return_pct'] for s in stocks if s['war_cumulative_return_pct'] is not None]
            war_vols = [s['war_volatility'] for s in stocks if s['war_volatility'] is not None]

            median_shift = round(statistics.median(shifts), 2) if shifts else 0
            avg_war_cum = round(statistics.mean(war_cums), 2) if war_cums else 0
            avg_war_vol = round(statistics.mean(war_vols), 2) if war_vols else 0

            if median_shift >= 0.15:
                label = "strong_positive"
            elif median_shift >= 0.05:
                label = "positive"
            elif median_shift >= -0.05:
                label = "neutral"
            elif median_shift >= -0.15:
                label = "negative"
            else:
                label = "strong_negative"

            sector_summary.append({
                "sector": sector,
                "stock_count": len(stocks),
                "median_performance_shift": median_shift,
                "avg_war_cumulative_return": avg_war_cum,
                "avg_war_volatility": avg_war_vol,
                "war_impact_label": label,
            })

        sector_impact = {s['sector']: s['war_impact_label'] for s in sector_summary}
        for m in stock_metrics:
            m['war_impact'] = sector_impact.get(m['sector'], 'unknown')

        # ───── Data Quality Gate ─────
        dq = DataQualityChecker(stage="silver_stock_war_metrics")
        dq.check_completeness(stock_metrics, ["symbol", "sector"])
        dq.check_uniqueness(stock_metrics, ["symbol"])
        dq.check_validity(stock_metrics, "pre_war_days", min_value=0)
        dq.check_validity(stock_metrics, "war_days", min_value=0)
        dq.run()

        db['silver_stock_war_metrics'].delete_many({})
        if stock_metrics:
            db['silver_stock_war_metrics'].insert_many(stock_metrics)
            db['silver_stock_war_metrics'].create_index("symbol", unique=True)
            db['silver_stock_war_metrics'].create_index("sector")
            db['silver_stock_war_metrics'].create_index("war_impact")

        db['silver_war_impact_analysis'].delete_many({})
        if sector_summary:
            db['silver_war_impact_analysis'].insert_many(sector_summary)
            db['silver_war_impact_analysis'].create_index("sector", unique=True)

        logging.info(f"Stock metrics: {len(stock_metrics)} | Sectors: {len(sector_summary)}")
        for s in sorted(sector_summary, key=lambda x: x['median_performance_shift'], reverse=True):
            logging.info(f"  {s['sector']}: shift={s['median_performance_shift']}% -> {s['war_impact_label']} ({s['stock_count']} stocks)")

    finally:
        client.close()


def enrich_company_profiles():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    try:
        impact_map = {}
        for doc in db['silver_war_impact_analysis'].find({}, {"_id": 0}):
            impact_map[doc['sector']] = doc['war_impact_label']

        stock_metrics = {}
        for doc in db['silver_stock_war_metrics'].find({}, {"_id": 0}):
            stock_metrics[doc['symbol']] = doc

        profiles = list(db['company_profiles'].find({}, {"_id": 0}))
        silver_profiles = []
        SECTOR_NORMALIZE = {
            "Financials": "Financial Services",
        }
        for p in profiles:
            symbol = p.get('symbol')
            raw_sector = p.get('sector', 'Unknown')
            sector = SECTOR_NORMALIZE.get(raw_sector, raw_sector)
            metrics = stock_metrics.get(symbol, {})
            silver_profiles.append({
                "symbol": symbol,
                "full_name": p.get("full_name"),
                "sector": sector,
                "industry": p.get("industry"),
                "market_cap": p.get("market_cap"),
                "business_summary": p.get("business_summary"),
                "war_impact": impact_map.get(sector, "unknown"),
                "war_cumulative_return_pct": metrics.get("war_cumulative_return_pct"),
                "performance_shift": metrics.get("performance_shift"),
                "updated_at": p.get("updated_at"),
            })

        # ───── Data Quality Gate ─────
        dq = DataQualityChecker(stage="silver_company_enriched")
        dq.check_completeness(silver_profiles, ["symbol", "sector"])
        dq.check_uniqueness(silver_profiles, ["symbol"])
        dq.run()

        silver = db['silver_company_enriched']
        silver.delete_many({})
        if silver_profiles:
            silver.insert_many(silver_profiles)
            silver.create_index("symbol", unique=True)
            silver.create_index("sector")
            silver.create_index("war_impact")
        logging.info(f"Silver company enriched: {len(silver_profiles)} records")
    finally:
        client.close()


def transform_live_trades_to_silver():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    try:
        sector_map = get_sector_map()
        impact_map = {}
        for doc in db['silver_war_impact_analysis'].find({}, {"_id": 0}):
            impact_map[doc['sector']] = doc['war_impact_label']

        records = list(db['live_trades'].find({}, {"_id": 0}))
        logging.info(f"Bronze live trades: {len(records)}")
        if not records:
            logging.info("No live trades yet")
            return

        silver_records = []
        seen = set()
        for r in records:
            symbol = r.get('symbol')
            ts = r.get('timestamp')
            key = f"{symbol}_{ts}"
            if key in seen:
                continue
            seen.add(key)

            meta = sector_map.get(symbol, {})
            sector = meta.get('sector', 'Unknown')
            silver_records.append({
                "symbol": symbol,
                "price": r.get("price"),
                "volume": r.get("volume"),
                "timestamp": ts,
                "sector": sector,
                "industry": meta.get("industry", "Unknown"),
                "war_impact": impact_map.get(sector, "unknown"),
            })

        silver = db['silver_live_trades']

        # ───── Data Quality Gate ─────
        dq = DataQualityChecker(stage="silver_live_trades")
        dq.check_completeness(silver_records, ["symbol", "timestamp", "price"])
        dq.check_uniqueness(silver_records, ["symbol", "timestamp"])
        dq.check_validity(silver_records, "price", min_value=0)
        dq.run()

        silver.delete_many({})
        if silver_records:
            silver.insert_many(silver_records)
            silver.create_index([("symbol", 1), ("timestamp", 1)])
            silver.create_index("sector")
        logging.info(f"Silver live trades: {len(silver_records)} (deduped from {len(records)})")
    finally:
        client.close()