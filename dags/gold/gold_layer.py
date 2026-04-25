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
GOLD_WEEKLY_PERFORMANCE_COLLECTION = "gold_weekly_sector_performance"
GOLD_WAR_TIMELINE_COLLECTION = "gold_war_daily_timeline"


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
                "war_latest_close": metric.get("war_latest_close"),
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


def build_gold_weekly_sector_performance():
    """Build weekly sector performance summary from silver historical data."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    try:
        records = list(
            db[SILVER_HISTORICAL_COLLECTION].find(
                {"daily_return_pct": {"$ne": None}},
                {
                    "_id": 0,
                    "symbol": 1,
                    "date": 1,
                    "close": 1,
                    "daily_return_pct": 1,
                    "sector": 1,
                    "period": 1,
                },
            )
        )

        if not records:
            print("No silver historical data found for weekly sector performance.")
            return

        weekly = defaultdict(lambda: {"returns": [], "closes": []})

        for record in records:
            normalized_date = _to_date(record.get("date"))
            if normalized_date is None:
                continue

            week = normalized_date.strftime("%Y-W%W")
            key = (record.get("sector", "Unknown"), week)

            daily_return = record.get("daily_return_pct")
            if daily_return is not None:
                weekly[key]["returns"].append(daily_return)

            close_value = record.get("close")
            if close_value is not None:
                weekly[key]["closes"].append(close_value)

        gold_records = []
        for (sector, week), data in weekly.items():
            returns = data["returns"]
            closes = data["closes"]

            gold_records.append({
                "sector": sector,
                "week": week,
                "period": "war" if week >= "2026-W00" else "pre_war",
                "avg_daily_return_pct": round(sum(returns) / len(returns), 4) if returns else 0,
                "avg_close": round(sum(closes) / len(closes), 2) if closes else 0,
                "data_points": len(returns),
            })

        gold_records.sort(key=lambda x: (x["sector"], x["week"]))

        # ───── Data Quality Gate ─────
        dq = DataQualityChecker(stage="gold_weekly_sector_performance")
        dq.check_completeness(gold_records, ["sector", "week", "period"])
        dq.check_uniqueness(gold_records, ["sector", "week"])
        dq.check_validity(gold_records, "data_points", min_value=0)
        dq.run()

        target_collection = db[GOLD_WEEKLY_PERFORMANCE_COLLECTION]
        target_collection.delete_many({})
        if gold_records:
            target_collection.insert_many(gold_records)
            target_collection.create_index([("sector", 1), ("week", 1)], unique=True)
            target_collection.create_index("period")

        print(f"Loaded {len(gold_records)} records into {GOLD_WEEKLY_PERFORMANCE_COLLECTION}.")

    except Exception as e:
        print(f"Error building gold weekly sector performance: {e}")
        raise

    finally:
        client.close()


def build_gold_war_daily_timeline():
    """Build daily war timeline summary from silver historical data."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    try:
        # FIX: เอา "period": "war" ออก เพื่อให้ดึงทั้งก่อน/หลังสงคราม
        # และเพิ่ม period ใน projection เพื่อเก็บข้อมูลช่วงลงไปใน gold record
        records = list(
            db[SILVER_HISTORICAL_COLLECTION].find(
                {"daily_return_pct": {"$ne": None}},
                {"_id": 0, "date": 1, "sector": 1, "daily_return_pct": 1, "period": 1},
            )
        )

        if not records:
            print("No silver historical data found for war daily timeline.")
            return

        daily_sector = defaultdict(lambda: defaultdict(list))
        daily_period = {}  # เก็บ period ของแต่ละวัน (pre_war / war)

        for record in records:
            normalized_date = _to_date(record.get("date"))
            sector = record.get("sector", "Unknown")
            daily_return = record.get("daily_return_pct")

            if normalized_date is None or daily_return is None:
                continue

            date_key = normalized_date.strftime("%Y-%m-%d")
            daily_sector[date_key][sector].append(daily_return)
            daily_period[date_key] = record.get("period", "unknown")

        gold_records = []
        for date_key in sorted(daily_sector.keys()):
            sectors = daily_sector[date_key]
            sector_returns = {}
            all_returns = []

            for sector, returns in sectors.items():
                avg_return = round(sum(returns) / len(returns), 4)
                sector_returns[sector] = avg_return
                all_returns.extend(returns)

            market_avg = round(sum(all_returns) / len(all_returns), 4) if all_returns else 0
            best_sector = max(sector_returns, key=sector_returns.get) if sector_returns else ""
            worst_sector = min(sector_returns, key=sector_returns.get) if sector_returns else ""

            gold_records.append({
                "date": date_key,
                "period": daily_period.get(date_key, "unknown"),
                "market_avg_return": market_avg,
                "best_sector": best_sector,
                "best_sector_return": sector_returns.get(best_sector, 0),
                "worst_sector": worst_sector,
                "worst_sector_return": sector_returns.get(worst_sector, 0),
                "sector_returns": sector_returns,
            })

        # ───── Data Quality Gate ─────
        dq = DataQualityChecker(stage="gold_war_daily_timeline")
        dq.check_completeness(gold_records, ["date", "period"])
        dq.check_uniqueness(gold_records, ["date"])
        dq.run()

        target_collection = db[GOLD_WAR_TIMELINE_COLLECTION]
        target_collection.delete_many({})
        if gold_records:
            target_collection.insert_many(gold_records)
            target_collection.create_index("date", unique=True)
            target_collection.create_index("period")

        print(f"Loaded {len(gold_records)} records into {GOLD_WAR_TIMELINE_COLLECTION}.")

    except Exception as e:
        print(f"Error building gold war daily timeline: {e}")
        raise

    finally:
        client.close()