from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
from collections import defaultdict
import logging

MONGO_URI = 'mongodb://mongodb:27017/'
DB_NAME = 'stock_database'
WAR_START_DATE = '2026-01-01'


def build_sector_war_summary():
    """
    Gold: สรุปผลกระทบสงครามราย Sector พร้อม top winners/losers
    Input: silver_war_impact_analysis + silver_stock_war_metrics
    Output: gold_sector_war_summary
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    try:
        # ดึง sector summary
        sectors = list(db['silver_war_impact_analysis'].find({}, {"_id": 0}))

        # ดึง stock metrics จัดกลุ่มตาม sector
        all_stocks = list(db['silver_stock_war_metrics'].find({}, {"_id": 0}))
        by_sector = defaultdict(list)
        for s in all_stocks:
            by_sector[s.get('sector', 'Unknown')].append(s)

        gold_records = []
        for sector_info in sectors:
            sector = sector_info['sector']
            stocks = by_sector.get(sector, [])

            # เรียงตาม performance_shift
            valid = [s for s in stocks if s.get('performance_shift') is not None]
            valid.sort(key=lambda x: x['performance_shift'], reverse=True)

            top_winners = [{"symbol": s['symbol'], "shift": s['performance_shift']} for s in valid[:5]]
            top_losers = [{"symbol": s['symbol'], "shift": s['performance_shift']} for s in valid[-5:]]

            gold_records.append({
                "sector": sector,
                "stock_count": sector_info.get('stock_count', 0),
                "war_impact_label": sector_info.get('war_impact_label', 'unknown'),
                "median_performance_shift": sector_info.get('median_performance_shift', 0),
                "avg_war_cumulative_return": sector_info.get('avg_war_cumulative_return', 0),
                "avg_war_volatility": sector_info.get('avg_war_volatility', 0),
                "top_5_winners": top_winners,
                "top_5_losers": top_losers,
            })

        gold_records.sort(key=lambda x: x['median_performance_shift'], reverse=True)

        col = db['gold_sector_war_summary']
        col.delete_many({})
        if gold_records:
            col.insert_many(gold_records)
            col.create_index("sector", unique=True)
        logging.info(f"Gold sector summary: {len(gold_records)} sectors")

    finally:
        client.close()


def build_stock_ranking():
    """
    Gold: จัดอันดับหุ้นทุกตัวตามผลกระทบสงคราม
    Input: silver_stock_war_metrics + silver_company_enriched
    Output: gold_stock_ranking
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    try:
        metrics = list(db['silver_stock_war_metrics'].find({}, {"_id": 0}))
        profiles = {d['symbol']: d for d in db['silver_company_enriched'].find({}, {"_id": 0})}

        # เรียงตาม war_cumulative_return_pct (ผลตอบแทนสะสมตั้งแต่สงครามเริ่ม)
        valid = [m for m in metrics if m.get('war_cumulative_return_pct') is not None]
        valid.sort(key=lambda x: x['war_cumulative_return_pct'], reverse=True)

        gold_records = []
        for rank, m in enumerate(valid, 1):
            symbol = m['symbol']
            profile = profiles.get(symbol, {})

            gold_records.append({
                "rank": rank,
                "symbol": symbol,
                "full_name": profile.get("full_name", ""),
                "sector": m.get("sector", "Unknown"),
                "industry": m.get("industry", "Unknown"),
                "war_impact": m.get("war_impact", "unknown"),
                "performance_shift": m['performance_shift'],
                "war_cumulative_return_pct": m.get("war_cumulative_return_pct"),
                "pre_war_cumulative_return_pct": m.get("pre_war_cumulative_return_pct"),
                "war_volatility": m.get("war_volatility"),
                "pre_war_volatility": m.get("pre_war_volatility"),
                "war_avg_daily_return": m.get("war_avg_daily_return"),
            })

        col = db['gold_stock_ranking']
        col.delete_many({})
        if gold_records:
            col.insert_many(gold_records)
            col.create_index("rank")
            col.create_index("symbol", unique=True)
            col.create_index("sector")
            col.create_index("war_impact")
        logging.info(f"Gold stock ranking: {len(gold_records)} stocks")

    finally:
        client.close()


def build_weekly_sector_performance():
    """
    Gold: Performance รายสัปดาห์ แยกตาม sector (สำหรับกราฟ trend)
    Input: silver_historical_daily
    Output: gold_weekly_sector_performance
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    try:
        records = list(db['silver_historical_daily'].find(
            {"daily_return_pct": {"$ne": None}},
            {"_id": 0, "symbol": 1, "date": 1, "close": 1, "daily_return_pct": 1, "sector": 1, "period": 1}
        ))
        logging.info(f"Processing {len(records)} daily records for weekly aggregation")

        # จัดกลุ่มตาม sector + week
        from datetime import datetime as dt
        weekly = defaultdict(lambda: {"returns": [], "volumes": [], "closes": []})

        for r in records:
            try:
                d = dt.strptime(r['date'], '%Y-%m-%d')
                week = d.strftime('%Y-W%W')
                key = (r['sector'], week)
                weekly[key]['returns'].append(r['daily_return_pct'])
                if r.get('close'):
                    weekly[key]['closes'].append(r['close'])
            except (ValueError, KeyError):
                continue

        gold_records = []
        for (sector, week), data in weekly.items():
            returns = data['returns']
            closes = data['closes']
            period = "war" if week >= "2026-W00" else "pre_war"

            gold_records.append({
                "sector": sector,
                "week": week,
                "period": period,
                "avg_daily_return_pct": round(sum(returns) / len(returns), 4) if returns else 0,
                "avg_close": round(sum(closes) / len(closes), 2) if closes else 0,
                "data_points": len(returns),
            })

        gold_records.sort(key=lambda x: (x['sector'], x['week']))

        col = db['gold_weekly_sector_performance']
        col.delete_many({})
        if gold_records:
            col.insert_many(gold_records)
            col.create_index([("sector", 1), ("week", 1)], unique=True)
            col.create_index("period")
        logging.info(f"Gold weekly sector performance: {len(gold_records)} records")

    finally:
        client.close()


def build_war_daily_timeline():
    """
    Gold: Timeline รายวัน — แต่ละวันแต่ละ sector เป็นยังไง
    Input: silver_historical_daily
    Output: gold_war_daily_timeline
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    try:
        # เอาเฉพาะช่วงสงคราม
        records = list(db['silver_historical_daily'].find(
            {"period": "war", "daily_return_pct": {"$ne": None}},
            {"_id": 0, "date": 1, "sector": 1, "daily_return_pct": 1}
        ))
        logging.info(f"War period records: {len(records)}")

        daily_sector = defaultdict(lambda: defaultdict(list))
        for r in records:
            daily_sector[r['date']][r['sector']].append(r['daily_return_pct'])

        gold_records = []
        for date, sectors in sorted(daily_sector.items()):
            sector_returns = {}
            all_returns = []
            for sector, returns in sectors.items():
                avg = round(sum(returns) / len(returns), 4)
                sector_returns[sector] = avg
                all_returns.extend(returns)

            market_avg = round(sum(all_returns) / len(all_returns), 4) if all_returns else 0
            best = max(sector_returns, key=sector_returns.get) if sector_returns else ""
            worst = min(sector_returns, key=sector_returns.get) if sector_returns else ""

            gold_records.append({
                "date": date,
                "market_avg_return": market_avg,
                "best_sector": best,
                "best_sector_return": sector_returns.get(best, 0),
                "worst_sector": worst,
                "worst_sector_return": sector_returns.get(worst, 0),
                "sector_returns": sector_returns,
            })

        col = db['gold_war_daily_timeline']
        col.delete_many({})
        if gold_records:
            col.insert_many(gold_records)
            col.create_index("date", unique=True)
        logging.info(f"Gold war daily timeline: {len(gold_records)} days")

    finally:
        client.close()


# === DAG ===
default_args = {
    'owner': 'Ake',
    'start_date': datetime(2026, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='gold_layer_transform',
    default_args=default_args,
    description='Gold Layer: สรุปข้อมูลพร้อมใช้สำหรับ Dashboard',
    schedule_interval='0 19 * * 1-5',  # จ-ศ 19:00 (หลัง Silver 30 นาที)
    catchup=False,
    tags=['gold', 'sp500', 'us_iran_war'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    t1 = PythonOperator(task_id='build_sector_war_summary', python_callable=build_sector_war_summary)
    t2 = PythonOperator(task_id='build_stock_ranking', python_callable=build_stock_ranking)
    t3 = PythonOperator(task_id='build_weekly_sector_performance', python_callable=build_weekly_sector_performance)
    t4 = PythonOperator(task_id='build_war_daily_timeline', python_callable=build_war_daily_timeline)

    start >> [t1, t2, t3, t4] >> end
