# MongoDB Collections Schema for Real-time Features

## 1. live_trades Collection

```json
{
  "_id": ObjectId,
  "symbol": "AAPL",
  "price": 150.25,
  "base_price": 145.00,
  "live_war_return_pct": 3.62,
  "status": "alert_spike",  // or "alert_crash", "normal"
  "timestamp": ISODate("2025-04-21T10:30:00Z"),
  "date": "2025-04-21",
  "volume": 2500000,
  "bid": 150.23,
  "ask": 150.27
}
```

### Fields:
- **symbol**: Stock ticker (e.g., AAPL, MSFT, TSLA)
- **price**: Current real-time price
- **base_price**: Baseline price (เปิดของวัน or previous close)
- **live_war_return_pct**: Percentage change from baseline
- **status**: Alert status (alert_spike > 5%, alert_crash < -5%, normal)
- **timestamp**: ISO timestamp of the data point
- **date**: Date in YYYY-MM-DD format

---

## 2. gold_sector_war_summary Collection

```json
{
  "_id": ObjectId,
  "sector": "Technology",
  "stock_count": 45,
  "war_cumulative_return_pct": 8.75,
  "avg_war_cumulative_return": 8.75,
  "median_performance_shift": 0.45,
  "war_impact_label": "strong_positive",  // or positive, neutral, negative, strong_negative
  "avg_war_volatility": 0.0234,
  "top_5_winners": [
    {"symbol": "NVDA", "shift": 1.25},
    {"symbol": "MSFT", "shift": 1.10}
  ],
  "top_5_losers": [
    {"symbol": "XYZ", "shift": -0.95},
    {"symbol": "ABC", "shift": -0.85}
  ]
}
```

---

## 3. gold_stock_ranking Collection

```json
{
  "_id": ObjectId,
  "rank": 1,
  "symbol": "NVDA",
  "full_name": "NVIDIA Corporation",
  "sector": "Technology",
  "war_cumulative_return_pct": 45.23,
  "pre_war_cumulative_return_pct": 12.50,
  "performance_shift": 1.34,
  "war_impact": "strong_positive",
  "war_volatility": 0.0345,
  "war_avg_daily_return": 0.235
}
```

---

## 4. gold_live_war_monitor Collection

```json
{
  "_id": ObjectId,
  "symbol": "AAPL",
  "live_price": 150.25,
  "base_price": 145.00,
  "live_war_return_pct": 3.62,
  "status": "alert_spike",  // Only stores non-normal alerts
  "timestamp": ISODate("2025-04-21T10:30:00Z"),
  "alert_triggered_at": ISODate("2025-04-21T10:29:55Z")
}
```

### สำคัญ:
- เก็บเฉพาะ **alert_spike** (> 5%) และ **alert_crash** (< -5%)
- ใช้สำหรับการแสดง Live Alert Monitor ส่วน

---

## 5. silver_historical_daily Collection

```json
{
  "_id": ObjectId,
  "symbol": "AAPL",
  "date": ISODate("2025-04-21T00:00:00Z"),
  "open": 145.50,
  "high": 152.00,
  "low": 144.75,
  "close": 150.25,
  "volume": 45000000,
  "period": "war"  // or "pre_war"
}
```

---

## Indexes ที่ควรมี (สำหรับ Performance)

```javascript
// live_trades collection
db.live_trades.createIndex({ "symbol": 1, "timestamp": -1 })
db.live_trades.createIndex({ "status": 1, "live_war_return_pct": -1 })
db.live_trades.createIndex({ "timestamp": -1 })

// gold_live_war_monitor collection
db.gold_live_war_monitor.createIndex({ "status": 1, "live_war_return_pct": -1 })
db.gold_live_war_monitor.createIndex({ "symbol": 1 })

// silver_historical_daily collection
db.silver_historical_daily.createIndex({ "symbol": 1, "date": -1 })
```

---

## Query Examples สำหรับ Streamlit

### 1. ดึงข้อมูล Real-time ล่าสุดของหุ้น
```python
latest = db["live_trades"].find_one(
    {"symbol": "AAPL"},
    {"_id": 0},
    sort=[("timestamp", -1)]
)
```

### 2. ดึง Alert หุ้นที่มีการผันผวน
```python
alerts = list(db["gold_live_war_monitor"].find(
    {"status": {"$ne": "normal"}}, 
    {"_id": 0}
).sort("live_war_return_pct", -1))
```

### 3. ดึงราคาย้อนหลังของหุ้น
```python
hist = list(db["silver_historical_daily"].find(
    {"symbol": "AAPL"},
    {"_id": 0, "date": 1, "close": 1, "period": 1}
).sort("date", 1))
```

---

## ข้อสังเกต (Important Notes)

1. **live_trades** ควรมีข้อมูลจาก Kafka Consumer (realtime_consumer.py)
2. **gold_live_war_monitor** ควรเป็น subset ของ live_trades ที่มี status != "normal"
3. **gold_sector_war_summary** & **gold_stock_ranking** มาจาก Gold DAG
4. **silver_historical_daily** มาจาก Silver DAG

---

**MongoDB Connection**: `mongodb://localhost:27017/`  
**Database**: `stock_database`
