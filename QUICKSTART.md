# 🚀 Quick Start - Real-time Stock Price Dashboard

## ขั้นตอนการเริ่มต้น (5 นาที)

### 1️⃣ **ตรวจสอบ MongoDB**
```bash
# ในเทอร์มินัล 1 - เริ่ม MongoDB
mongod
# ควรเห็น: "waiting for connections on port 27017"
```

### 2️⃣ **เริ่มต้น Kafka (ถ้าใช้)**
```bash
# ในเทอร์มินัล 2 - เริ่ม Kafka (ถ้ามี)
docker-compose up -d
# หรือ
./start_kafka.sh
```

### 3️⃣ **รัน Realtime Consumer**
```bash
# ในเทอร์มินัล 3 - รับข้อมูลจาก Kafka
cd /Users/phruek/Desktop/dataend/Data-Engineering
python3 realtime/realtime_consumer.py
```

### 4️⃣ **รัน Realtime Producer (ถ้าต้องการข้อมูลสมมติ)**
```bash
# ในเทอร์มินัล 4 (เลือก) - ส่งข้อมูลแบบสมมติ
python3 realtime/realtime_producer.py
```

### 5️⃣ **เริ่มต้น Dashboard**
```bash
# ในเทอร์มินัล 5 - รัน Streamlit app
streamlit run app.py
```

ระบบจะเปิด browser ไปที่: `http://localhost:8501`

---

## 🎯 สิ่งที่ควรเห็นหลังจากเริ่ม

✅ **Dashboard หลัก** จะแสดง:
- 📊 S&P 500 War Impact Dashboard Header
- 🔴 Live Alert Monitor (ด้านบนสุด)
- 5️⃣ KPI Cards (Total Stocks, Sectors, Positive/Negative)

✅ **เมื่อขยาย Sector**:
- 📊 ตารางหุ้นในกลุ่ม
- 💰 **Real-time Prices Grid** (ใหม่!)
  - แต่ละการ์ดมีสี: 🟢 เขียว (up), 🔴 แดง (down)
  - แสดงเปอร์เซ็นต์การเปลี่ยนแปลง

✅ **ในแท็บ "Top Winners & Losers"**:
- 📈 Top 20 Winners + Live Prices
- 📉 Top 20 Losers + Live Prices

✅ **ในแท็บ "Stock Search"**:
- ค้นหาหุ้น (เช่น AAPL)
- 📊 **ราคา Real-time ขนาดใหญ่**
- 📈 ตารางราคาประวัติศาสตร์

---

## 📊 ตัวอย่างข้อมูลที่ควรมี

### ใน MongoDB
```bash
# ตรวจสอบ collections
mongo
use stock_database
db.live_trades.count()              # ควรมี > 0
db.gold_sector_war_summary.count()  # ควรมี > 0
db.gold_stock_ranking.count()       # ควรมี > 0
```

### ตัวอย่าง live_trades document:
```json
{
  "symbol": "AAPL",
  "price": 150.25,
  "base_price": 145.00,
  "live_war_return_pct": 3.62,
  "status": "normal",
  "timestamp": ISODate("2025-04-21T10:30:00Z")
}
```

---

## ⚠️ Common Issues & Fixes

### ❌ Issue: "Unable to connect to MongoDB"
```bash
# ✅ Fix:
1. ตรวจสอบ mongod กำลังทำงาน
   ps aux | grep mongod
   
2. ถ้าไม่ทำงาน ให้เริ่ม:
   mongod

3. ถ้า port ถูกใช้ไป:
   lsof -i :27017
   kill -9 <PID>
```

### ❌ Issue: "No module named 'streamlit'"
```bash
# ✅ Fix:
pip install streamlit pymongo pandas kafka-python
```

### ❌ Issue: "ไม่เห็น Real-time Prices"
```bash
# ✅ Fix:
1. ตรวจสอบ realtime_consumer.py กำลังทำงาน
2. ตรวจสอบ live_trades มีข้อมูล:
   mongo
   use stock_database
   db.live_trades.find().limit(1)
   
3. ถ้าว่างเปล่า ให้รัน realtime_producer.py
```

### ❌ Issue: "Streamlit port 8501 already in use"
```bash
# ✅ Fix:
streamlit run app.py --server.port 8502
# หรือ
lsof -i :8501
kill -9 <PID>
```

---

## 🔄 Data Flow

```
┌─────────────────────────────────────┐
│  Realtime Producer (สมมติ/Kafka)   │
└────────────┬────────────────────────┘
             │
             ↓
┌─────────────────────────────────────┐
│  Kafka Broker                       │
└────────────┬────────────────────────┘
             │
             ↓
┌─────────────────────────────────────┐
│  Realtime Consumer                  │
│  (realtime_consumer.py)             │
│  - ดึงข้อมูล Kafka                 │
│  - คำนวณ % change                  │
│  - บันทึก MongoDB live_trades      │
└────────────┬────────────────────────┘
             │
             ↓
┌─────────────────────────────────────┐
│  MongoDB (live_trades)              │
│  - เก็บข้อมูลล่าสุด               │
│  - indexed by symbol + timestamp    │
└────────────┬────────────────────────┘
             │
             ↓
┌─────────────────────────────────────┐
│  Streamlit Dashboard (app.py)       │
│  - Fetch realtime prices (30s cache)│
│  - Display with colors (green/red)  │
│  - Multiple views (Sector, Winners) │
└─────────────────────────────────────┘
```

---

## 🎮 ทดสอบด้วยตัวคุณเอง

### Test 1: ตรวจสอบ MongoDB Connection
```python
python3 << 'EOF'
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['stock_database']
print(f"✅ Connected! Collections: {db.list_collection_names()}")
EOF
```

### Test 2: ตรวจสอบ Real-time Data
```python
python3 << 'EOF'
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['stock_database']
latest = db['live_trades'].find_one({}, sort=[('timestamp', -1)])
if latest:
    print(f"✅ Latest: {latest['symbol']} @ ${latest['price']}")
else:
    print("❌ No data in live_trades")
EOF
```

### Test 3: ตรวจสอบ Streamlit
```bash
streamlit hello
# ควรเห็น demo app ทำงาน
```

---

## 📝 Logs to Monitor

### 1. realtime_consumer.py logs
```
[Saved] -> AAPL @ 150.25 | Base: 145.00 | Impact: +3.62% | Status: normal
[Saved] -> MSFT @ 245.50 | Base: 250.00 | Impact: -1.80% | Status: normal
```

### 2. app.py logs (Streamlit)
```
2025-04-21 10:30:00 - Fetching real-time prices
2025-04-21 10:30:05 - Dashboard rendered successfully
```

### 3. MongoDB logs
```
mongod --logpath /tmp/mongodb.log
tail -f /tmp/mongodb.log
```

---

## 🎉 Success Checklist

- [ ] MongoDB กำลังทำงาน
- [ ] realtime_consumer.py เปิดอยู่
- [ ] app.py (Streamlit) ทำงาน
- [ ] เห็นข้อมูลใน live_trades collection
- [ ] Dashboard แสดง Real-time Prices
- [ ] สีเปลี่ยนตามการเพิ่มขึ้น/ลดลง (🟢/🔴)
- [ ] ค้นหาหุ้นได้ และแสดงราคา real-time

---

## 📞 Support

ถ้ามีปัญหา:
1. ตรวจสอบ logs ด้านบน
2. ตรวจสอบ MongoDB connection
3. ตรวจสอบข้อมูล schema (MONGODB_SCHEMA.md)
4. อ่าน USAGE_GUIDE.md สำหรับรายละเอียด

---

**Ready to Start?** 🚀

```bash
cd /Users/phruek/Desktop/dataend/Data-Engineering
streamlit run app.py
```

Happy Analyzing! 📊✨
