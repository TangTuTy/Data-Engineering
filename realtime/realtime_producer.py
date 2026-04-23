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


# --- ดึง Top 50 หุ้นจาก MongoDB (เลือกจากแต่ละ sector) ---
def get_top_symbols():
    """เลือก Top symbols จากแต่ละ GICS sector ให้ครบ 50 ตัว"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client['stock_database']

        tickers = list(db['sp500_tickers'].find({}, {'symbol': 1, 'gics_sector': 1}))
        client.close()

        if not tickers:
            print("ไม่มีข้อมูล sp500_tickers ใช้ default symbols")
            return get_default_symbols()

        from collections import defaultdict
        sector_symbols = defaultdict(list)
        for t in tickers:
            sector = t.get('gics_sector', 'Unknown')
            sector_symbols[sector].append(t['symbol'])

        num_sectors = len(sector_symbols)
        per_sector = max(1, MAX_SYMBOLS // num_sectors)

        selected = []
        for sector, symbols in sorted(sector_symbols.items()):
            selected.extend(symbols[:per_sector])

        remaining = MAX_SYMBOLS - len(selected)
        if remaining > 0:
            all_remaining = []
            for sector, symbols in sector_symbols.items():
                all_remaining.extend(symbols[per_sector:])
            selected.extend(all_remaining[:remaining])

        return selected[:MAX_SYMBOLS]

    except Exception as e:
        print(f"Error reading MongoDB: {e}")
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