from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
from datetime import datetime, timezone

from alert_notifier import send_discord_alert


TARGET_BASELINE_DATE = "2024-12-31"   # วันฐานราคา (pre-war reference)

# Price alert thresholds (vs baseline)
SPIKE_THRESHOLD      =  5.0   # % ขึ้นจากราคาฐาน → alert_spike
CRASH_THRESHOLD      = -5.0   # % ลงจากราคาฐาน → alert_crash
EXTREME_UP_THRESHOLD =  10.0  # % ขึ้นรุนแรง     → alert_extreme_up
EXTREME_DN_THRESHOLD = -10.0  # % ลงรุนแรง       → alert_extreme_down

# Volume alert threshold
VOLUME_SPIKE_MULTIPLIER = 2.0  # volume > 2x baseline_volume → alert_volume_spike

# War-sensitive sectors (ดึงจาก silver_war_impact_analysis แต่ pre-define fallback ไว้)
WAR_SENSITIVE_LABELS = {"negative", "strong_negative"}

# ============================================================
# 1. เชื่อมต่อ MongoDB 
# ============================================================
while True:
    try:
        mongo_client = MongoClient('mongodb://mongodb:27017/')
        db = mongo_client['stock_database']
        collection = db['live_trades']
        print("เชื่อมต่อ MongoDB สำเร็จ!")
        # TTL index: ลบ live_trades อัตโนมัติหลัง 7 วัน ป้องกัน collection โตไม่หยุด
        collection.create_index("_created_at", expireAfterSeconds=86400 * 7)
        break
    except Exception as e:
        print(f"กำลังรอ MongoDB ... : {e}")
        time.sleep(5)

# ============================================================
# 2. เชื่อมต่อ Kafka Consumer  
# ============================================================
print("กำลังเตรียมเชื่อมต่อ Kafka...")
while True:
    try:
        consumer = KafkaConsumer(
            'stock_topic',
            bootstrap_servers=['kafka:29092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("เชื่อมต่อ Kafka สำเร็จ! Consumer พร้อมทำงาน...")
        break
    except Exception as e:
        print(f"กำลังรอสายพาน Kafka ... : {e}")
        time.sleep(5)

# ============================================================
# 3. ดึงราคาฐาน + volume ฐาน + war impact map
# ============================================================
print("กำลังดึงข้อมูลอ้างอิง (Baseline) จากฐานข้อมูล...")

# --- 3a. baseline price — ดึงจาก fact_war_analytics (star schema) ---
baseline_prices = {}
fact_col = db['fact_war_analytics']
cursor = fact_col.find({"date": TARGET_BASELINE_DATE}, {"_id": 0, "symbol": 1, "close": 1})
for doc in cursor:
    symbol = doc.get('symbol')
    close_price = doc.get('close')
    if symbol and close_price:
        baseline_prices[symbol] = close_price
print(f"ดึงราคา Baseline สำเร็จ: {len(baseline_prices)} ตัว")

# --- 3b. baseline volume — avg daily volume 30 วันก่อน war จาก fact_war_analytics ---
baseline_volumes = {}
try:
    pipeline = [
        {"$match": {"volume": {"$exists": True, "$gt": 0}}},
        {"$sort": {"symbol": 1, "date": -1}},
        {"$group": {
            "_id": "$symbol",
            "volumes": {"$push": "$volume"},
        }},
    ]
    for doc in fact_col.aggregate(pipeline):
        symbol = doc["_id"]
        vols = doc["volumes"][:30]  # 30 วันล่าสุด
        if vols:
            baseline_volumes[symbol] = sum(vols) / len(vols)
    print(f"ดึง Volume Baseline สำเร็จ: {len(baseline_volumes)} ตัว")
except Exception as e:
    print(f"[Warning] ดึง volume baseline ไม่ได้: {e} — volume alert จะถูก skip")

# --- 3c. war impact map จาก dim_company (star schema) ---
war_impact_map = {}  # symbol -> war_impact_label
sector_map = {}       # symbol -> sector
try:
    for doc in db['dim_company'].find({}, {"symbol": 1, "war_impact": 1, "sector": 1, "_id": 0}):
        sym = doc.get("symbol")
        if sym:
            war_impact_map[sym] = doc.get("war_impact", "unknown")
            sector_map[sym] = doc.get("sector", "Unknown")
    print(f"ดึง War Impact Map สำเร็จ: {len(war_impact_map)} ตัว")
except Exception as e:
    print(f"[Warning] ดึง war impact map ไม่ได้: {e} — war-sensitive alert จะถูก skip")

print("เริ่มรับข้อมูล Live Stream จาก Kafka...")
print("--" * 25)

# ============================================================
# 4. ประมวลผล Live Stream + Alert Logic
# ============================================================
def classify_alert(impact_pct, volume, baseline_vol, symbol, war_impact):
    """
    คืน list ของ alert types ที่ triggered
    แยกเป็น list เพราะหุ้นหนึ่งตัวอาจ trigger หลาย alert พร้อมกันได้
    """
    alerts = []

    # --- Price alerts (ขยายจาก logic เดิม) ---
    if impact_pct >= EXTREME_UP_THRESHOLD:
        alerts.append("alert_extreme_up")
    elif impact_pct >= SPIKE_THRESHOLD:
        alerts.append("alert_spike")

    if impact_pct <= EXTREME_DN_THRESHOLD:
        alerts.append("alert_extreme_down")
    elif impact_pct <= CRASH_THRESHOLD:
        alerts.append("alert_crash")

    # --- War-sensitive alert (ใหม่) ---
    # หุ้นที่ sector มี war_impact = negative/strong_negative แล้วราคาลง
    if war_impact in WAR_SENSITIVE_LABELS and impact_pct <= CRASH_THRESHOLD:
        alerts.append("alert_war_sensitive")

    # --- Volume spike alert (ใหม่) ---
    if baseline_vol and baseline_vol > 0 and volume is not None:
        if volume >= baseline_vol * VOLUME_SPIKE_MULTIPLIER:
            alerts.append("alert_volume_spike")

    if not alerts:
        alerts.append("normal")

    return alerts


for message in consumer:
    trade_data = message.value
    symbol     = trade_data.get('symbol')
    live_price = trade_data.get('price')
    live_volume = trade_data.get('volume')

    # --- baseline price (logic เดิม — ไม่เปลี่ยน) ---
    if symbol not in baseline_prices:
        baseline_prices[symbol] = live_price
    base_price = baseline_prices[symbol]

    # --- คำนวณ impact % (logic เดิม — ไม่เปลี่ยน) ---
    if base_price and base_price > 0:
        impact_pct = ((live_price - base_price) / base_price) * 100
    else:
        impact_pct = 0.0

    # --- ดึงข้อมูลเพิ่ม ---
    baseline_vol = baseline_volumes.get(symbol)
    war_impact   = war_impact_map.get(symbol, "unknown")
    sector       = sector_map.get(symbol, "Unknown")

    # --- classify alerts ---
    alert_types = classify_alert(impact_pct, live_volume, baseline_vol, symbol, war_impact)

    # backward-compatible: ยัด status ตัวเดียวเหมือนเดิม (primary alert)
    primary_status = alert_types[0]  # เอาตัวแรกที่ trigger เป็น primary

    # --- เตรียม record ---
    trade_data['base_price']          = base_price
    trade_data['live_war_return_pct'] = round(impact_pct, 4)
    trade_data['status']              = primary_status          
    trade_data['alert_types']         = alert_types             
    trade_data['baseline_volume']     = baseline_vol            
    trade_data['war_impact']          = war_impact              
    trade_data['sector']              = sector                  
    trade_data['alert_at']            = datetime.now(timezone.utc).isoformat()  
    trade_data['_created_at']         = datetime.now(timezone.utc)  # BSON Date for TTL index

    # --- เซฟลง MongoDB  ---
    try:
        collection.insert_one(trade_data)
        is_alert = primary_status != "normal"
        tag = "🚨 ALERT" if is_alert else "  saved"
        print(
            f" [{tag}] {symbol} @ {live_price:.2f}"
            f" | Base: {base_price:.2f}"
            f" | Impact: {impact_pct:+.2f}%"
            f" | Alerts: {alert_types}"
            f" | Vol: {live_volume} (base: {int(baseline_vol) if baseline_vol else '-'})"
        )

        # --- ส่ง Discord alert  ---
        if is_alert:
            for alert_type in alert_types:
                if alert_type == "normal":
                    continue
                send_discord_alert(
                    symbol=symbol,
                    alert_type=alert_type,
                    price=live_price,
                    impact_pct=impact_pct,
                    sector=sector,
                    volume=live_volume,
                    baseline_volume=baseline_vol,
                    war_impact=war_impact,
                )
    except Exception as e:
        print(f" [Error] ไม่สามารถบันทึกข้อมูล {symbol}: {e}")
        continue