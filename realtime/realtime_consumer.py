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

# 3. เริ่มดูดข้อมูลลงฐานข้อมูล
for message in consumer:
    trade_data = message.value
    collection.insert_one(trade_data)
    # Log 
    print(f" [Saved] -> {trade_data.get('symbol')} @ {trade_data.get('price')}")