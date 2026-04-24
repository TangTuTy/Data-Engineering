import os
import websocket
import json
import datetime
import time
from kafka import KafkaProducer
from pymongo import MongoClient

# --- CONFIGURATION ---
FINNHUB_TOKEN = os.environ.get("FINNHUB_TOKEN")
if not FINNHUB_TOKEN:
    raise RuntimeError(
        "FINNHUB_TOKEN environment variable is not set. "
        "กรุณาตั้งค่าใน .env หรือ docker-compose.yml"
    )

MAX_ROWS_PER_RUN = int(os.environ.get("MAX_ROWS_PER_RUN", 5000))
SEND_INTERVAL    = int(os.environ.get("SEND_INTERVAL", 5))
MAX_SYMBOLS      = int(os.environ.get("MAX_SYMBOLS", 50))  # Finnhub free tier limit

MONGO_URI    = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")


# --- ดึง Top 50 หุ้นจาก MongoDB (Smart Selection จาก batch results) ---
def get_top_symbols():
    """
    Smart symbol selection สำหรับ realtime watchlist
    เลือก 50 หุ้นจากผล batch analysis แบ่งเป็น 3 กลุ่ม:

    1) 30 War-Sensitive: หุ้นที่ |performance_shift| สูงสุด
       → "หุ้นที่กระทบสงครามจริง" — track impact realtime

    2) 15 Sector Flagships: หุ้นใหญ่จาก war-sensitive sectors
       → "หุ้น representative ของ sector" — ดู sector trend

    3) 5 Mega-cap Control: AAPL, MSFT, GOOGL, AMZN, BRK-B
       → "control group" — เทียบกับตลาดโดยรวม

    Fallback: ถ้า batch ยังไม่มี → ใช้ default symbols
    """
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client['stock_database']

        # ───── 1) War-Sensitive: top 30 by |performance_shift| ─────
        war_sensitive_symbols = []
        try:
            ranking = list(db['gold_stock_ranking'].find(
                {"performance_shift": {"$ne": None}},
                {"symbol": 1, "performance_shift": 1, "sector": 1, "_id": 0}
            ))
            ranking.sort(key=lambda x: abs(x.get('performance_shift', 0)), reverse=True)
            war_sensitive_symbols = [r['symbol'] for r in ranking[:30]]
            print(f"  ✓ War-sensitive (top 30): {war_sensitive_symbols[:5]}...")
        except Exception as e:
            print(f"  ⚠️ Could not load gold_stock_ranking: {e}")

        # ───── 2) Sector Flagships: 15 หุ้นใหญ่จาก war-sensitive sectors ─────
        flagship_symbols = []
        try:
            sectors = list(db['gold_sector_war_summary'].find(
                {"war_impact_label": {"$in": ["positive", "negative", "strong_positive", "strong_negative"]}},
                {"sector": 1, "_id": 0}
            ))
            war_sectors = [s['sector'] for s in sectors]

            for sector in war_sectors[:5]:  # 5 sectors แรก × 3 ตัว = 15
                top_in_sector = list(db['silver_company_enriched'].find(
                    {
                        "sector": sector,
                        "market_cap": {"$ne": None},
                        "symbol": {"$nin": war_sensitive_symbols + flagship_symbols},
                    },
                    {"symbol": 1, "_id": 0}
                ).sort("market_cap", -1).limit(3))
                flagship_symbols.extend([s['symbol'] for s in top_in_sector])

            print(f"  ✓ Sector flagships (15): {flagship_symbols[:5]}...")
        except Exception as e:
            print(f"  ⚠️ Could not load flagships: {e}")

        # ───── 3) Mega-cap Control Group ─────
        control_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "BRK-B"]
        control_symbols = [s for s in control_symbols
                          if s not in war_sensitive_symbols + flagship_symbols]

        client.close()

        # ───── รวมทั้งหมด ─────
        selected = war_sensitive_symbols + flagship_symbols + control_symbols
        selected = selected[:MAX_SYMBOLS]

        if len(selected) < MAX_SYMBOLS:
            print(f"  ⚠️ Got {len(selected)} from batch, padding with defaults...")
            defaults = get_default_symbols()
            for sym in defaults:
                if sym not in selected:
                    selected.append(sym)
                if len(selected) >= MAX_SYMBOLS:
                    break

        if not selected:
            print("  ⚠️ No batch data available, using all defaults")
            return get_default_symbols()[:MAX_SYMBOLS]

        print(f"📊 Total selected: {len(selected)} symbols")
        print(f"   - {len(war_sensitive_symbols[:30])} war-sensitive")
        print(f"   - {len(flagship_symbols)} sector flagships")
        print(f"   - {len([s for s in control_symbols if s in selected])} control")

        return selected

    except Exception as e:
        print(f"❌ Error in smart selection: {e}")
        return get_default_symbols()


def get_default_symbols():
    """Fallback symbols ถ้า MongoDB ยังไม่พร้อม"""
    return [
        "XOM", "CVX", "OXY", "COP", "HAL",
        "LMT", "RTX", "NOC", "GD", "LHX",
        "DAL", "UAL", "AAL", "LUV", "ALK",
        "CCL", "RCL", "NCLH", "MAR", "HLT",
        "NEM", "FCX", "CF",
        "AAPL", "MSFT", "GOOGL", "AMZN", "META",
        "JPM", "BAC", "GS",
        "JNJ", "UNH", "PFE",
        "PG", "KO", "WMT",
        "NEE", "DUK", "SO",
        "AMT", "PLD", "SPG",
        "TSLA", "HD", "MCD", "NKE", "SBUX",
        "T", "VZ", "DIS",
    ]


SYMBOLS = get_top_symbols()
print(f"Tracking {len(SYMBOLS)} symbols: {SYMBOLS[:10]}...")

last_sent_time = {}
rows_sent = 0

# เชื่อมต่อ Kafka
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka!")
    except Exception as e:
        print(f"Waiting for Kafka... {e}")
        time.sleep(5)


def on_message(ws, message):
    global last_sent_time, rows_sent

    if rows_sent >= MAX_ROWS_PER_RUN:
        print(f"Reached {MAX_ROWS_PER_RUN} rows. Closing...")
        ws.close()
        return

    data = json.loads(message)
    if data.get('type') == 'trade':
        for trade in data['data']:
            symbol = trade['s']
            current_time = datetime.datetime.now()

            if symbol not in last_sent_time or (current_time - last_sent_time[symbol]).total_seconds() >= SEND_INTERVAL:
                payload = {
                    "symbol": symbol,
                    "price": trade['p'],
                    "volume": trade['v'],
                    "timestamp": current_time.strftime('%Y-%m-%d %H:%M:%S')
                }
                producer.send('stock_topic', value=payload)
                last_sent_time[symbol] = current_time
                rows_sent += 1
                print(f"[Sent] {symbol} | ${trade['p']} | [{rows_sent}/{MAX_ROWS_PER_RUN}]")


def on_error(ws, error):
    print(f"WebSocket Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("Finnhub Connection Closed")


def on_open(ws):
    print(f"Connected to Finnhub! Subscribing to {len(SYMBOLS)} symbols...")
    for symbol in SYMBOLS:
        ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')


if __name__ == "__main__":
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()