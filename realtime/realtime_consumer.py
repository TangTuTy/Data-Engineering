from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time

# 1. เชื่อมต่อ MongoDB 
while True:
    try:
        mongo_client = MongoClient('mongodb://mongodb:27017/')
        db = mongo_client['stock_database']
        collection = db['live_trades']
        print("เชื่อมต่อ MongoDB สำเร็จ!")
        break
    except Exception as e:
        print(f"กำลังรอ MongoDB ... : {e}")
        time.sleep(5)

# 2. เชื่อมต่อ Kafka Consumer 
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

# 3. เริ่มดึงราคาฐานอ้างอิง
print("กำลังดึงราคาอ้างอิง (Baseline) จากฐานข้อมูล...")
baseline_prices = {}

# กำหนดวันที่ต้องการใช้เป็นฐาน (เช่น วันก่อนเริ่มเกิดสงคราม หรือ 1 ปีที่แล้ว)
# ปรับวันที่ให้ตรงกับข้อมูลที่มีใน Database ของคุณได้เลย
TARGET_BASELINE_DATE = "2024-12-31" 

# ดึงข้อมูลจาก collection 'historical_prices'
historical_col = db['historical_prices']

# Query หาราคาปิด (Close) ของทุกหุ้นในวันที่กำหนดมาเก็บไว้ใน Dictionary
cursor = historical_col.find({"Date": TARGET_BASELINE_DATE})
for doc in cursor:
    symbol = doc.get('symbol')
    close_price = doc.get('Close')
    if symbol and close_price:
        baseline_prices[symbol] = close_price

print(f"ดึงราคา Baseline สำเร็จจำนวน {len(baseline_prices)} ตัว")
print("เริ่มรับข้อมูล Live Stream จาก Kafka...")
print("--" * 25)

# 4. ประมวลผลข้อมูล Live Stream
for message in consumer:
    trade_data = message.value
    symbol = trade_data.get('symbol')
    live_price = trade_data.get('price')
    
    # 1. เช็คว่ามีราคาฐานจาก DB ไหม ถ้าหุ้นตัวไหนไม่มีข้อมูลอดีต ค่อยให้จำราคา Live แรกไว้แทน
    if symbol not in baseline_prices:
        baseline_prices[symbol] = live_price
        
    base_price = baseline_prices[symbol]
    
    # 2. คำนวณ Impact (%) เทียบกับราคาฐาน
    if base_price > 0:
        impact_pct = ((live_price - base_price) / base_price) * 100
    else:
        impact_pct = 0.0
        
    # 3. เช็คสถานะความผันผวน (ถ้าเกิน +/- 5% ให้แจ้งเตือน)
    if impact_pct > 5.0:
        status = "alert_spike"
    elif impact_pct < -5.0:
        status = "alert_crash"
    else:
        status = "normal"
        
    # 4. ยัดข้อมูลกลับเข้าไป
    trade_data['base_price'] = base_price
    trade_data['live_war_return_pct'] = round(impact_pct, 4)
    trade_data['status'] = status

    # เซฟลง MongoDB
    try:
        collection.insert_one(trade_data)
        print(f" [Saved] -> {symbol} @ {live_price} | Base: {base_price:.2f} | Impact: {trade_data['live_war_return_pct']}% | Status: {status}")
    except Exception as e:
        print(f" [Error] ไม่สามารถบันทึกข้อมูล {symbol}: {e}")
        continue