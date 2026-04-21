# 📊 Real-time Stock Price Dashboard - Usage Guide

## 🎯 ความสามารถหลัก

### 1️⃣ **Sector Overview with Real-time Prices**
เมื่อคุณเปิด Sector expansion:
- ✅ แสดง dataframe ของหุ้นทั้งหมดในกลุ่ม
- ✅ แสดง **Real-time Prices Grid** ขนาดเล็ก
- ✅ แต่ละการ์ดมีสี:
  - 🟢 **เขียว**: ราคา +3% ขึ้นไป
  - 🔴 **แดง**: ราคา -3% ลงไป
  - 🟡 **เหลือง**: ราคาไม่เปลี่ยน

**ตัวอย่าง UI:**
```
💰 Real-time Prices
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   AAPL      │  │   MSFT      │  │   GOOG      │  │   AMZN      │
│ 🔼 $150.25  │  │ 🔽 $245.50  │  │ 🔼 $92.75   │  │ 🔼 $165.30  │
│   +3.62%    │  │   -2.15%    │  │   +1.45%    │  │   +0.85%    │
└─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘
```

---

### 2️⃣ **Top Winners & Losers with Live Prices**
ในแท็บ "Top Winners & Losers":

**ด้านบน (Winners):**
- ตารางแสดง Top 20 หุ้นที่มีผลตอบแทนดีสุด
- **ด้านล่างตาราง** มี "💰 Live Prices - Top Winners" cards

**ด้านล่าง (Losers):**
- ตารางแสดง Top 20 หุ้นที่มีผลตอบแทนแย่สุด
- **ด้านล่างตาราง** มี "💰 Live Prices - Top Losers" cards

---

### 3️⃣ **Individual Stock Search with Large Price Display**
ในแท็บ "Stock Search":

**ขั้นตอน:**
1. ค้นหาหุ้นด้วยการพิมพ์ symbol (เช่น AAPL, MSFT)
2. ระบบจะแสดง:
   - 📊 **ราคา Real-time ขนาดใหญ่** (ทันที)
   - 4 x Metrics (Rank, Sector, War Impact, Performance Shift)
   - 4 x Metrics (Cumulative Returns, Volatility, Avg Daily Return)
   - 📈 ตารางเทียบสำหรับราคาประวัติศาสตร์

**ตัวอย่าง Live Price Display:**
```
┌────────────────────────────────────────┐
│          🔼 $150.25                    │
│            +3.62%                      │
│          Live Price Data               │
└────────────────────────────────────────┘
```

---

## 🎨 Color Meanings

| สี | อักษร | ความหมาย | ตัวอย่าง |
|-------|--------|---------|---------|
| 🟢 **เขียว** | `#22c55e` | ราคา **เพิ่มขึ้น** 🔼 | +3.62%, +0.85% |
| 🔴 **แดง** | `#dc2626` | ราคา **ลดลง** 🔽 | -2.15%, -5.23% |
| 🟡 **เหลือง** | `#fbbf24` | ราคา **ไม่เปลี่ยน** ➡️ | 0.00%, ±0.01% |

---

## 📡 Data Refresh

- **Auto-refresh**: ทุก **30 วินาที** (Streamlit cache)
- **Data Source**: `live_trades` collection จาก MongoDB
- **Update Trigger**: เมื่อ realtime_consumer.py ทำการ push ข้อมูลใหม่

### วิธีการอัพเดตข้อมูล:
```bash
# ในอีกเทอร์มินัล ให้รัน Kafka Consumer
python realtime/realtime_consumer.py
```

---

## ⚙️ ฟังก์ชัน Helper ที่ใช้

### 1. `fetch_realtime_prices(symbols_list)`
```python
# ดึงราคา real-time สำหรับหุ้นชุดหนึ่ง
realtime_data = fetch_realtime_prices(['AAPL', 'MSFT', 'GOOG'])

# Returns:
{
  'AAPL': {
    'price': 150.25,
    'prev_close': 145.00,
    'change_pct': 3.62,
    'status': 'alert_spike',
    'timestamp': '2025-04-21T10:30:00Z'
  },
  'MSFT': { ... }
}
```

### 2. `format_price_change(current_price, prev_price, change_pct)`
```python
# Format ราคาพร้อมสี
result = format_price_change(150.25, 145.00, 3.62)

# Returns:
{
  'color': '#22c55e',      # สีเขียว
  'arrow': '🔼',           # ลูกศรขึ้น
  'current': 150.25,       # ราคา
  'change': 3.62,          # % เปลี่ยน
  'html': '<span style="color: #22c55e; ...">🔼 $150.25 (+3.62%)</span>'
}
```

---

## 🚨 Alert Status

เมื่อราคาเปลี่ยนแปลงเกินกำหนด:

| Status | เงื่อนไข | สีแสดง | อักษร |
|--------|----------|--------|--------|
| **alert_spike** | > +5% | 🟢 เขียว | 🚀 |
| **alert_crash** | < -5% | 🔴 แดง | 💥 |
| **normal** | -5% to +5% | 🟡 เหลือง | - |

---

## 💡 Tips & Tricks

### ✨ Tip 1: ค้นหาหุ้นอย่างเร็ว
- ไปที่แท็บ "Stock Search"
- พิมพ์ symbol ตัวพิมพ์เล็ก ระบบจะแปลงเป็นตัวใหญ่อัตโนมัติ

### ✨ Tip 2: ตรวจสอบ Real-time Data
- ถ้าเห็นข้อความ "📡 ยังไม่มีข้อมูล Real-time"
  - ให้ตรวจสอบว่า `realtime_consumer.py` กำลังทำงาน
  - ตรวจสอบ MongoDB connection

### ✨ Tip 3: สังเกตุ Alert Highlights
- Live Alert Monitor (ด้านบนสุด) แสดง Alert สูงสุด 4 หุ้น
- หมดดีเพราะ scroll ไม่ต้อง ระหว่างการดูข้อมูล

---

## 🔧 Troubleshooting

### ❌ ปัญหา: "MongoDB connection error"
```
✅ วิธีแก้:
1. ตรวจสอบ MongoDB กำลังทำงาน: mongod
2. ตรวจสอบ port: localhost:27017
3. ตรวจสอบ database: stock_database มีอยู่
```

### ❌ ปัญหา: "ไม่มีข้อมูล Real-time"
```
✅ วิธีแก้:
1. ตรวจสอบ live_trades collection มีข้อมูล:
   db.live_trades.count()
2. รัน realtime_consumer.py:
   python realtime/realtime_consumer.py
3. รัน realtime_producer.py (สำหรับส่งข้อมูล):
   python realtime/realtime_producer.py
```

### ❌ ปัญหา: "ราคาไม่อัพเดต"
```
✅ วิธีแก้:
1. Streamlit cache TTL = 30 วินาที
2. ถ้ายังไม่อัพเดต ให้กด F5 refresh
3. ตรวจสอบ realtime_consumer.py ทำงานอยู่
```

---

## 📊 Data Pipeline Overview

```
Kafka Producer (realtime_producer.py)
         ↓
    Kafka Broker
         ↓
Kafka Consumer (realtime_consumer.py)
         ↓
   live_trades (MongoDB)
         ↓
   Dashboard (app.py)
   - Sector Overview
   - Top Winners/Losers
   - Stock Search
```

---

## 🎯 Performance Notes

- **Cache Duration**: 30 วินาที (สำหรับ `fetch_realtime_prices`)
- **Database Queries**: Indexed by symbol + timestamp
- **UI Rendering**: Grid layout (4 columns) ปรับเปลี่ยนตามขนาด

---

## 📈 Future Enhancements

- [ ] Add price update timestamp แบบ live
- [ ] Add 24-hour high/low ราคา
- [ ] Add trading volume
- [ ] Add candlestick charts
- [ ] Auto-scroll ไปหาราคาที่สูง/ต่ำ
- [ ] Push notifications สำหรับ alerts

---

**Last Updated**: 21 เมษายน 2569  
**Dashboard Version**: 2.0 (with Real-time Features)
