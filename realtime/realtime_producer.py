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
    Symbol selection สำหรับ realtime watchlist — 50 slots แบ่งตาม use case:

    1) 15 Top Winners: performance_shift บวกสูงสุด
       → ตรงกับ Dashboard Winners panel

    2) 15 Top Losers: performance_shift ลบสูงสุด (ลบมากสุด)
       → ตรงกับ Dashboard Losers panel

    3) 10 Sector Flagships: market_cap สูงสุดต่อ sector (war-sensitive sectors)
       → ดู sector trend + alert ระดับ sector

    4) 5 Mega-cap Control: AAPL, MSFT, GOOGL, AMZN, BRK-B
       → baseline เทียบกับตลาดโดยรวม

    5) 5 High Volatility: หุ้น war-sensitive ที่ไม่อยู่กลุ่มข้างต้น
       → เพิ่ม coverage สำหรับ price alert

    Fallback: ถ้า batch ยังไม่มี → ใช้ default symbols
    """
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client['stock_database']

        ranking = list(db['dim_company'].find(
            {"performance_shift": {"$ne": None}},
            {"symbol": 1, "performance_shift": 1, "sector": 1, "_id": 0}
        ))
        ranking.sort(key=lambda x: x.get('performance_shift', 0), reverse=True)

        # ───── 1) Top 15 Winners (performance_shift บวกสูงสุด) ─────
        winner_symbols = [r['symbol'] for r in ranking if r.get('performance_shift', 0) > 0][:15]
        print(f"  ✓ Top Winners (15): {winner_symbols[:5]}...")

        # ───── 2) Top 15 Losers (performance_shift ลบมากสุด) ─────
        loser_symbols = [r['symbol'] for r in reversed(ranking) if r.get('performance_shift', 0) < 0][:15]
        print(f"  ✓ Top Losers (15): {loser_symbols[:5]}...")

        used = set(winner_symbols + loser_symbols)

        # ───── 3) Sector Flagships: market_cap สูงสุดต่อ sector (10 ตัว) ─────
        flagship_symbols = []
        try:
            sectors = list(db['dim_sector'].find(
                {"war_impact_label": {"$in": ["positive", "negative", "strong_positive", "strong_negative"]}},
                {"sector": 1, "_id": 0}
            ))
            war_sectors = [s['sector'] for s in sectors]
            seen_sectors = set()
            for sector in war_sectors:
                if len(flagship_symbols) >= 10:
                    break
                if sector in seen_sectors:
                    continue
                seen_sectors.add(sector)
                top = list(db['dim_company'].find(
                    {"sector": sector, "market_cap": {"$ne": None}, "symbol": {"$nin": list(used)}},
                    {"symbol": 1, "_id": 0}
                ).sort("market_cap", -1).limit(1))
                if top:
                    flagship_symbols.append(top[0]['symbol'])
                    used.add(top[0]['symbol'])
            print(f"  ✓ Sector flagships (10): {flagship_symbols}...")
        except Exception as e:
            print(f"  ⚠️ Could not load flagships: {e}")

        # ───── 4) Mega-cap Control (5 ตัว) ─────
        control_symbols = [s for s in ["AAPL", "MSFT", "GOOGL", "AMZN", "BRK-B"] if s not in used]

        # ───── 5) High Volatility buffer — เติมให้ครบ 50 ─────
        volatile_symbols = []
        remaining = MAX_SYMBOLS - len(winner_symbols) - len(loser_symbols) - len(flagship_symbols) - len(control_symbols)
        if remaining > 0:
            extra = [r['symbol'] for r in ranking if r['symbol'] not in used][:remaining]
            volatile_symbols = extra
            print(f"  ✓ High volatility buffer ({len(volatile_symbols)}): {volatile_symbols[:5]}...")

        client.close()

        selected = winner_symbols + loser_symbols + flagship_symbols + control_symbols + volatile_symbols
        selected = list(dict.fromkeys(selected))[:MAX_SYMBOLS]  # dedup + cap

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
        print(f"   - {len(winner_symbols)} winners | {len(loser_symbols)} losers")
        print(f"   - {len(flagship_symbols)} sector flagships | {len(control_symbols)} control | {len(volatile_symbols)} volatile")

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