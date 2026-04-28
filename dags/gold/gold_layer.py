from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime, date
import logging
import pandas as pd

from utils.data_quality import DataQualityChecker


MONGO_URI = "mongodb://mongodb:27017/"
DB_NAME = "stock_database"

SILVER_COMPANY_ENRICHED_COLLECTION = "silver_company_enriched"
SILVER_STOCK_METRICS_COLLECTION = "silver_stock_war_metrics"
SILVER_HISTORICAL_COLLECTION = "silver_historical_daily"

GOLD_SECTOR_SUMMARY_COLLECTION = "gold_sector_war_summary"
GOLD_STOCK_RANKING_COLLECTION = "gold_stock_ranking"


def _to_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        try:
            return pd.to_datetime(value, errors="coerce").date()
        except Exception:
            return None
    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None


def build_gold_sector_summary():
    """Build gold summary of war impact by sector."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    try:
        sectors = list(db["silver_war_impact_analysis"].find({}, {"_id": 0}))
        all_stocks = list(db[SILVER_STOCK_METRICS_COLLECTION].find({}, {"_id": 0}))

        if not sectors:
            print("No silver war impact analysis data found for sector summary.")
            return

        by_sector = defaultdict(list)
        for stock in all_stocks:
            by_sector[stock.get("sector", "Unknown")].append(stock)

        gold_records = []
        for sector_info in sectors:
            sector = sector_info.get("sector", "Unknown")
            stocks = by_sector.get(sector, [])

            valid = [s for s in stocks if s.get("performance_shift") is not None]
            valid.sort(key=lambda x: x["performance_shift"], reverse=True)

            top_winners = [
                {"symbol": s.get("symbol"), "shift": s.get("performance_shift")}
                for s in valid[:5]
            ]
            top_losers = [
                {"symbol": s.get("symbol"), "shift": s.get("performance_shift")}
                for s in valid[-5:]
            ]

            gold_records.append({
                "sector": sector,
                "stock_count": sector_info.get("stock_count", 0),
                "war_impact_label": sector_info.get("war_impact_label", "unknown"),
                "median_performance_shift": sector_info.get("median_performance_shift", 0),
                "avg_war_cumulative_return": sector_info.get("avg_war_cumulative_return", 0),
                "avg_war_volatility": sector_info.get("avg_war_volatility", 0),
                "top_5_winners": top_winners,
                "top_5_losers": top_losers,
            })

        gold_records.sort(key=lambda x: x["median_performance_shift"], reverse=True)

        # ───── Data Quality Gate ─────
        dq = DataQualityChecker(stage="gold_sector_war_summary")
        dq.check_completeness(gold_records, ["sector", "war_impact_label"])
        dq.check_uniqueness(gold_records, ["sector"])
        dq.check_validity(gold_records, "stock_count", min_value=0)
        dq.run()

        target_collection = db[GOLD_SECTOR_SUMMARY_COLLECTION]
        target_collection.delete_many({})
        if gold_records:
            target_collection.insert_many(gold_records)
            target_collection.create_index("sector", unique=True)

        print(f"Loaded {len(gold_records)} records into {GOLD_SECTOR_SUMMARY_COLLECTION}.")

    except Exception as e:
        print(f"Error building gold sector summary: {e}")
        raise

    finally:
        client.close()


def build_gold_stock_ranking():
    """Build gold ranking of stocks by war impact."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    try:
        metrics = list(db[SILVER_STOCK_METRICS_COLLECTION].find({}, {"_id": 0}))
        profiles = {
            d["symbol"]: d
            for d in db[SILVER_COMPANY_ENRICHED_COLLECTION].find({}, {"_id": 0})
            if d.get("symbol")
        }

        if not metrics:
            print("No stock metrics data found for gold stock ranking.")
            return

        valid = [m for m in metrics if m.get("war_cumulative_return_pct") is not None]
        valid.sort(key=lambda x: x["war_cumulative_return_pct"], reverse=True)

        gold_records = []
        for rank, metric in enumerate(valid, 1):
            symbol = metric.get("symbol")
            profile = profiles.get(symbol, {})

            gold_records.append({
                "rank": rank,
                "symbol": symbol,
                "full_name": profile.get("full_name", ""),
                "sector": metric.get("sector", "Unknown"),
                "industry": metric.get("industry", "Unknown"),
                "war_impact": metric.get("war_impact", "unknown"),
                "performance_shift": metric.get("performance_shift"),
                "war_cumulative_return_pct": metric.get("war_cumulative_return_pct"),
                "pre_war_cumulative_return_pct": metric.get("pre_war_cumulative_return_pct"),
                "war_volatility": metric.get("war_volatility"),
                "pre_war_volatility": metric.get("pre_war_volatility"),
                "war_avg_daily_return": metric.get("war_avg_daily_return"),
            })

        # ───── Data Quality Gate ─────
        dq = DataQualityChecker(stage="gold_stock_ranking")
        dq.check_completeness(gold_records, ["symbol", "rank", "sector"])
        dq.check_uniqueness(gold_records, ["symbol"])
        dq.check_uniqueness(gold_records, ["rank"])
        dq.check_validity(gold_records, "rank", min_value=1)
        dq.run()

        target_collection = db[GOLD_STOCK_RANKING_COLLECTION]
        target_collection.delete_many({})
        if gold_records:
            target_collection.insert_many(gold_records)
            target_collection.create_index("rank")
            target_collection.create_index("symbol", unique=True)
            target_collection.create_index("sector")
            target_collection.create_index("war_impact")

        print(f"Loaded {len(gold_records)} records into {GOLD_STOCK_RANKING_COLLECTION}.")

    except Exception as e:
        print(f"Error building gold stock ranking: {e}")
        raise

    finally:
        client.close()



# ════════════════════════════════════════════════════════════════════════════
# STAR SCHEMA — Fact + Dimension Tables (Kimball pattern)
# ════════════════════════════════════════════════════════════════════════════
# โครงสร้าง:
#   fact_daily_prices  ← granular event data (1 row / symbol / date)
#       FK: date_sk, company_sk, sector_sk
#   dim_date           ← วันที่ + period attributes
#   dim_company        ← ข้อมูลบริษัท
#   dim_sector         ← sector + war_impact attributes
# ════════════════════════════════════════════════════════════════════════════

DIM_DATE_COLLECTION       = "dim_date"
DIM_COMPANY_COLLECTION    = "dim_company"
DIM_SECTOR_COLLECTION     = "dim_sector"
FACT_WAR_ANALYTICS_COLLECTION = "fact_war_analytics"


def build_dim_date():
    """
    Build dim_date — มิติของวันที่
    Surrogate Key: date_sk (YYYYMMDD format)
    Attributes: date, year, month, quarter, day_of_week, period (pre_war/war)
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    silver_daily = db[SILVER_HISTORICAL_COLLECTION]
    target = db[DIM_DATE_COLLECTION]

    try:
        # ดึง unique dates จาก fact source (silver_historical_daily)
        unique_dates = silver_daily.distinct("date")
        if not unique_dates:
            print("No dates in silver_historical_daily")
            return

        WAR_START = datetime.strptime('2026-01-01', '%Y-%m-%d').date()
        DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday",
                     "Friday", "Saturday", "Sunday"]

        # ───── Parse + Dedup ─────
        # Mongo distinct() อาจคืน date ในหลาย format (string + datetime)
        # → parse เป็น date object แล้ว dedup ก่อน build records
        parsed_dates = set()
        for d_raw in unique_dates:
            if isinstance(d_raw, datetime):
                parsed_dates.add(d_raw.date())
            elif isinstance(d_raw, date):
                parsed_dates.add(d_raw)
            elif isinstance(d_raw, str):
                try:
                    parsed_dates.add(datetime.strptime(d_raw[:10], '%Y-%m-%d').date())
                except ValueError:
                    continue

        records = []
        for d in sorted(parsed_dates):
            # surrogate key = YYYYMMDD (integer)
            date_sk = int(d.strftime("%Y%m%d"))
            quarter = (d.month - 1) // 3 + 1
            period  = "war" if d >= WAR_START else "pre_war"

            week_key = d.strftime("%Y-W%W")

            records.append({
                "date_sk": date_sk,
                "date": d.isoformat(),
                "year": d.year,
                "month": d.month,
                "quarter": quarter,
                "day_of_week": DAY_NAMES[d.weekday()],
                "is_weekend": d.weekday() >= 5,
                "period": period,
                "week_key": week_key,
            })

        # ───── DQ Gate ─────
        dq = DataQualityChecker(stage="dim_date")
        dq.check_completeness(records, ["date_sk", "date", "year", "period", "week_key"])
        dq.check_uniqueness(records, ["date_sk"])
        dq.check_validity(records, "year", min_value=2020, max_value=2030)
        dq.run()

        target.delete_many({})
        if records:
            target.insert_many(records)
            target.create_index("date_sk", unique=True)
            target.create_index("date", unique=True)
            target.create_index("period")
            target.create_index("week_key")

        print(f"Loaded {len(records)} records into {DIM_DATE_COLLECTION}")
    finally:
        client.close()


def build_dim_company():
    """
    Build dim_company — มิติของบริษัท (enriched)
    Surrogate Key: company_sk (sequential, sorted by symbol)
    Source: silver_company_enriched + gold_stock_ranking
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    source = db[SILVER_COMPANY_ENRICHED_COLLECTION]
    target = db[DIM_COMPANY_COLLECTION]

    try:
        profiles = list(source.find({}, {"_id": 0}))
        if not profiles:
            print("No data in silver_company_enriched")
            return

        # ───── Lookup: gold_stock_ranking (rank, metrics) ─────
        ranking_lookup = {}
        for doc in db[GOLD_STOCK_RANKING_COLLECTION].find({}, {"_id": 0}):
            ranking_lookup[doc.get("symbol")] = doc

        # ───── Lookup: silver_stock_war_metrics (pre_war_days, war_days) ─────
        war_metrics_lookup = {}
        for doc in db[SILVER_STOCK_METRICS_COLLECTION].find({}, {"_id": 0}):
            war_metrics_lookup[doc.get("symbol")] = doc

        # ───── Lookup: war_latest_close จาก silver_historical_daily ─────
        war_latest_close_lookup = {}
        try:
            pipeline = [
                {"$match": {"period": "war", "close": {"$ne": None}}},
                {"$sort": {"date": -1}},
                {"$group": {
                    "_id": "$symbol",
                    "latest_close": {"$first": "$close"},
                }},
            ]
            for doc in db[SILVER_HISTORICAL_COLLECTION].aggregate(pipeline):
                war_latest_close_lookup[doc["_id"]] = doc["latest_close"]
        except Exception as e:
            print(f"Warning: could not load war_latest_close: {e}")

        # surrogate key = sequential ID (1-based, sorted by symbol สำหรับ stability)
        records = []
        for idx, p in enumerate(sorted(profiles, key=lambda x: x.get("symbol", "")), start=1):
            symbol = p.get("symbol")
            rank_data = ranking_lookup.get(symbol, {})
            metrics_data = war_metrics_lookup.get(symbol, {})

            records.append({
                "company_sk": idx,
                "symbol": symbol,
                "full_name": p.get("full_name"),
                "sector": p.get("sector", "Unknown"),
                "industry": p.get("industry", "Unknown"),
                "market_cap": p.get("market_cap"),
                "war_impact": p.get("war_impact", "unknown"),
                # ── Enriched from gold_stock_ranking ──
                "rank": rank_data.get("rank"),
                "performance_shift": rank_data.get("performance_shift"),
                "war_cumulative_return_pct": rank_data.get("war_cumulative_return_pct"),
                "pre_war_cumulative_return_pct": rank_data.get("pre_war_cumulative_return_pct"),
                "war_volatility": rank_data.get("war_volatility"),
                "pre_war_volatility": rank_data.get("pre_war_volatility"),
                "war_avg_daily_return": rank_data.get("war_avg_daily_return"),
                # ── Enriched from silver_stock_war_metrics ──
                "pre_war_days": metrics_data.get("pre_war_days"),
                "war_days": metrics_data.get("war_days"),
                # ── Enriched from silver_historical_daily ──
                "war_latest_close": war_latest_close_lookup.get(symbol),
            })

        # ───── DQ Gate ─────
        dq = DataQualityChecker(stage="dim_company")
        dq.check_completeness(records, ["company_sk", "symbol", "sector"])
        dq.check_uniqueness(records, ["company_sk"])
        dq.check_uniqueness(records, ["symbol"])
        dq.run()

        target.delete_many({})
        if records:
            target.insert_many(records)
            target.create_index("company_sk", unique=True)
            target.create_index("symbol", unique=True)
            target.create_index("sector")
            target.create_index("rank")
            target.create_index("war_impact")

        print(f"Loaded {len(records)} records into {DIM_COMPANY_COLLECTION}")
    finally:
        client.close()


def build_dim_sector():
    """
    Build dim_sector — มิติของ sector (enriched)
    Surrogate Key: sector_sk (sequential ID)
    Source: gold_sector_war_summary (fully absorbed)
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    source = db[GOLD_SECTOR_SUMMARY_COLLECTION]
    target = db[DIM_SECTOR_COLLECTION]

    try:
        sectors = list(source.find({}, {"_id": 0}))
        if not sectors:
            print("No data in gold_sector_war_summary")
            return

        records = []
        for idx, s in enumerate(sorted(sectors, key=lambda x: x.get("sector", "")), start=1):
            records.append({
                "sector_sk": idx,
                "sector": s.get("sector"),
                "war_impact_label": s.get("war_impact_label", "neutral"),
                "stock_count": s.get("stock_count", 0),
                "median_performance_shift": s.get("median_performance_shift", 0),
                "avg_war_volatility": s.get("avg_war_volatility", 0),
                # ── Enriched: absorbed from gold_sector_war_summary ──
                "avg_war_cumulative_return": s.get("avg_war_cumulative_return", 0),
                "top_5_winners": s.get("top_5_winners", []),
                "top_5_losers": s.get("top_5_losers", []),
            })

        # ───── DQ Gate ─────
        dq = DataQualityChecker(stage="dim_sector")
        dq.check_completeness(records, ["sector_sk", "sector", "war_impact_label"])
        dq.check_uniqueness(records, ["sector_sk"])
        dq.check_uniqueness(records, ["sector"])
        dq.run()

        target.delete_many({})
        if records:
            target.insert_many(records)
            target.create_index("sector_sk", unique=True)
            target.create_index("sector", unique=True)

        print(f"Loaded {len(records)} records into {DIM_SECTOR_COLLECTION}")
    finally:
        client.close()


def build_fact_war_analytics():
    """
    Build fact_war_analytics — fact table ใหญ่ตัวเดียว (แทน fact_daily_prices)
    Grain: 1 row ต่อ (symbol, date)
    Foreign Keys: date_sk, company_sk, sector_sk
    Degenerate Dims: symbol, sector, date, period, week_key
    Measures: open, high, low, close, volume, daily_return_pct, moving_avg_*

    รวมข้อมูลจาก gold 4 ตัวเดิมเข้ามาเป็น denormalized fields:
    - gold_stock_ranking → อยู่ใน dim_company
    - gold_sector_war_summary → อยู่ใน dim_sector
    - gold_weekly_sector_performance → aggregation จาก fact
    - gold_war_daily_timeline → aggregation จาก fact

    Source: silver_historical_daily + dim tables (สำหรับ FK lookup)
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    silver_daily = db[SILVER_HISTORICAL_COLLECTION]
    target = db[FACT_WAR_ANALYTICS_COLLECTION]

    try:
        # ───── โหลด FK lookup tables ─────
        company_lookup = {}  # symbol -> company_sk
        for doc in db[DIM_COMPANY_COLLECTION].find({}, {"_id": 0, "symbol": 1, "company_sk": 1}):
            company_lookup[doc["symbol"]] = doc["company_sk"]

        sector_lookup = {}  # sector -> sector_sk
        for doc in db[DIM_SECTOR_COLLECTION].find({}, {"_id": 0, "sector": 1, "sector_sk": 1}):
            sector_lookup[doc["sector"]] = doc["sector_sk"]

        if not company_lookup or not sector_lookup:
            print("⚠️  Dimension tables empty — run dim builds first")
            return

        # ───── Build fact records ─────
        WAR_START = datetime.strptime('2026-01-01', '%Y-%m-%d').date()
        records_dict = {}  # ใช้ dict เพื่อ dedup โดย (date_sk, company_sk)
        skipped = 0

        for r in silver_daily.find({}, {"_id": 0}):
            symbol = r.get("symbol")
            sector = r.get("sector", "Unknown")
            d_raw  = r.get("date")

            # Parse date for date_sk
            if isinstance(d_raw, datetime):
                d = d_raw.date()
            elif isinstance(d_raw, date):
                d = d_raw
            elif isinstance(d_raw, str):
                try:
                    d = datetime.strptime(d_raw[:10], '%Y-%m-%d').date()
                except ValueError:
                    skipped += 1
                    continue
            else:
                skipped += 1
                continue

            # FK lookup
            company_sk = company_lookup.get(symbol)
            sector_sk  = sector_lookup.get(sector)

            if company_sk is None or sector_sk is None:
                skipped += 1
                continue

            date_sk  = int(d.strftime("%Y%m%d"))
            period   = "war" if d >= WAR_START else "pre_war"
            week_key = d.strftime("%Y-W%W")
            key = (date_sk, company_sk)  # composite PK

            # ถ้าซ้ำ → ใช้ตัวล่าสุด (overwrite)
            records_dict[key] = {
                # Foreign Keys
                "date_sk":    date_sk,
                "company_sk": company_sk,
                "sector_sk":  sector_sk,
                # Degenerate Dimensions (denormalized สำหรับ query convenience)
                "symbol":     symbol,
                "sector":     sector,
                "date":       d.isoformat(),
                "period":     period,
                "week_key":   week_key,
                # Measures (numeric facts)
                "open":               r.get("open"),
                "high":               r.get("high"),
                "low":                r.get("low"),
                "close":              r.get("close"),
                "volume":             r.get("volume"),
                "daily_return_pct":   r.get("daily_return_pct"),
                "moving_avg_7d":      r.get("moving_avg_7d"),
                "moving_avg_30d":     r.get("moving_avg_30d"),
            }

        records = list(records_dict.values())

        # ───── DQ Gate ─────
        dq = DataQualityChecker(stage="fact_war_analytics")
        dq.check_completeness(records, ["date_sk", "company_sk", "sector_sk", "close"])
        dq.check_uniqueness(records, ["date_sk", "company_sk"])  # composite key
        dq.check_validity(records, "close", min_value=0)
        dq.check_validity(records, "volume", min_value=0)
        dq.run()

        target.delete_many({})
        if records:
            target.insert_many(records)
            target.create_index([("date_sk", 1), ("company_sk", 1)], unique=True)
            target.create_index("date_sk")
            target.create_index("company_sk")
            target.create_index("sector_sk")
            # Indexes สำหรับ dashboard aggregation queries
            target.create_index("symbol")
            target.create_index([("sector", 1), ("week_key", 1)])  # weekly sector perf
            target.create_index("period")
            target.create_index("date")

        print(f"Loaded {len(records)} records into {FACT_WAR_ANALYTICS_COLLECTION} "
              f"(skipped {skipped} due to missing FK)")
    finally:
        client.close()