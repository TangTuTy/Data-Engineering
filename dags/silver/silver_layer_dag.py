from pymongo import MongoClient
from collections import defaultdict
import logging
import statistics
from datetime import datetime, date

MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'stock_database'
WAR_START_DATE = datetime.strptime('2026-01-01', '%Y-%m-%d').date()

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
    """Bronze -> Silver: historical_prices -> silver_historical_daily"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    bronze = db['historical_prices']
    silver = db['silver_historical_daily']
    sp500_symbols = get_sp500_symbols()
    sector_map = get_sector_map()

    try:
        query = {"symbol": {"$in": sp500_symbols}} if sp500_symbols else {}
        cursor = bronze.find(query, {"_id": 0}).sort([("symbol", 1), ("Date", 1)])
        records = list(cursor)
        logging.info(f"Bronze records: {len(records)}")
        if not records:
            logging.warning("No data in historical_prices")
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

        seen = {}
        for r in silver_records:
            key = (r['symbol'], r['date'])
            seen[key] = r
        silver_records = list(seen.values())

        silver.delete_many({})
        if silver_records:
            silver.insert_many(silver_records)
            silver.create_index([("symbol", 1), ("date", 1)], unique=True)
            silver.create_index("sector")
            silver.create_index("period")
        logging.info(f"Silver historical: {len(silver_records)} records")
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

        # FIX: ใช้ sector_map ที่ merge จาก company_profiles + sp500_tickers (มี fallback อยู่แล้ว)
        # แทนการอ่าน sector จาก company_profiles ตรงๆ ซึ่งอาจเป็น null ถ้า yfinance ดึงไม่ครบ
        sector_map = get_sector_map()

        profiles = list(db['company_profiles'].find({}, {"_id": 0}))
        silver_profiles = []
        SECTOR_NORMALIZE = {
            "Financials": "Financial Services",
        }
        for p in profiles:
            symbol = p.get('symbol')

            # ลำดับการหา sector: sector_map (มี fallback) > company_profiles > "Unknown"
            meta = sector_map.get(symbol, {})
            raw_sector = meta.get('sector') or p.get('sector') or 'Unknown'
            sector = SECTOR_NORMALIZE.get(raw_sector, raw_sector)

            # industry ก็ fallback เช่นเดียวกัน
            raw_industry = meta.get('industry') or p.get('industry') or 'Unknown'

            metrics = stock_metrics.get(symbol, {})
            silver_profiles.append({
                "symbol": symbol,
                "full_name": p.get("full_name"),
                "sector": sector,
                "industry": raw_industry,
                "market_cap": p.get("market_cap"),
                "business_summary": p.get("business_summary"),
                "war_impact": impact_map.get(sector, "unknown"),
                "war_cumulative_return_pct": metrics.get("war_cumulative_return_pct"),
                "performance_shift": metrics.get("performance_shift"),
                "updated_at": p.get("updated_at"),
            })

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
        silver.delete_many({})
        if silver_records:
            silver.insert_many(silver_records)
            silver.create_index([("symbol", 1), ("timestamp", 1)])
            silver.create_index("sector")
        logging.info(f"Silver live trades: {len(silver_records)} (deduped from {len(records)})")
    finally:
        client.close()