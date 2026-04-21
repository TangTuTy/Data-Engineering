#!/usr/bin/env python3
"""
Script ทดสอบและติดตั้งข้อมูล MongoDB สำหรับ Real-time Features
"""

from pymongo import MongoClient
from datetime import datetime
import json

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["stock_database"]

print("=" * 60)
print("🔧 MongoDB Test & Setup Script")
print("=" * 60)

# Test 1: Check MongoDB Connection
print("\n[1️⃣] Testing MongoDB Connection...")
try:
    db.command('ping')
    print("✅ MongoDB is running!")
except Exception as e:
    print(f"❌ MongoDB Connection Failed: {e}")
    exit(1)

# Test 2: Create Sample Data in live_trades
print("\n[2️⃣] Creating Sample Data in live_trades...")
live_trades_sample = [
    {
        "symbol": "AAPL",
        "price": 150.25,
        "base_price": 145.00,
        "live_war_return_pct": 3.62,
        "status": "alert_spike",
        "timestamp": datetime.now(),
        "date": "2025-04-21",
        "volume": 2500000,
        "bid": 150.23,
        "ask": 150.27
    },
    {
        "symbol": "MSFT",
        "price": 245.50,
        "base_price": 250.00,
        "live_war_return_pct": -1.80,
        "status": "normal",
        "timestamp": datetime.now(),
        "date": "2025-04-21",
        "volume": 1800000,
        "bid": 245.48,
        "ask": 245.52
    },
    {
        "symbol": "GOOG",
        "price": 92.75,
        "base_price": 91.40,
        "live_war_return_pct": 1.48,
        "status": "normal",
        "timestamp": datetime.now(),
        "date": "2025-04-21",
        "volume": 1200000,
        "bid": 92.73,
        "ask": 92.77
    },
    {
        "symbol": "AMZN",
        "price": 165.30,
        "base_price": 163.75,
        "live_war_return_pct": 0.95,
        "status": "normal",
        "timestamp": datetime.now(),
        "date": "2025-04-21",
        "volume": 900000,
        "bid": 165.28,
        "ask": 165.32
    },
    {
        "symbol": "NVDA",
        "price": 950.00,
        "base_price": 910.00,
        "live_war_return_pct": 4.40,
        "status": "alert_spike",
        "timestamp": datetime.now(),
        "date": "2025-04-21",
        "volume": 3200000,
        "bid": 949.98,
        "ask": 950.02
    },
]

try:
    db["live_trades"].insert_many(live_trades_sample)
    print(f"✅ Inserted {len(live_trades_sample)} sample records")
except Exception as e:
    print(f"⚠️ Could not insert (may already exist): {e}")

# Test 3: Create Indexes
print("\n[3️⃣] Creating MongoDB Indexes...")
try:
    db["live_trades"].create_index([("symbol", 1), ("timestamp", -1)])
    print("✅ Index created: symbol + timestamp")
    
    db["live_trades"].create_index([("status", 1), ("live_war_return_pct", -1)])
    print("✅ Index created: status + live_war_return_pct")
except Exception as e:
    print(f"⚠️ Index creation: {e}")

# Test 4: Verify Data
print("\n[4️⃣] Verifying Data...")
count = db["live_trades"].count_documents({})
print(f"✅ live_trades collection has {count} documents")

latest = db["live_trades"].find_one({}, sort=[("timestamp", -1)])
if latest:
    print(f"✅ Latest record: {latest['symbol']} @ ${latest['price']} ({latest['live_war_return_pct']:+.2f}%)")

# Test 5: Test Query Performance
print("\n[5️⃣] Testing Query Performance...")
import time

symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"]
start = time.time()

results = {}
for symbol in symbols:
    latest = db["live_trades"].find_one(
        {"symbol": symbol},
        {"_id": 0},
        sort=[("timestamp", -1)]
    )
    if latest:
        results[symbol] = {
            "price": latest.get("price"),
            "change": latest.get("live_war_return_pct")
        }

end = time.time()
print(f"✅ Fetched {len(results)} stocks in {(end-start)*1000:.2f}ms")
print(f"   Sample: {json.dumps(results, indent=2)}")

# Test 6: Collection Info
print("\n[6️⃣] Collection Information...")
collections = db.list_collection_names()
print(f"✅ Total collections: {len(collections)}")
print(f"   Collections: {', '.join(collections[:5])}{'...' if len(collections) > 5 else ''}")

# Test 7: Alert Status Distribution
print("\n[7️⃣] Alert Status Distribution...")
alerts = db["live_trades"].aggregate([
    {
        "$group": {
            "_id": "$status",
            "count": {"$sum": 1}
        }
    }
])
for alert in alerts:
    print(f"   {alert['_id']}: {alert['count']} records")

print("\n" + "=" * 60)
print("✅ All Tests Completed!")
print("=" * 60)

print("\n📊 Next Steps:")
print("1. Run: streamlit run app.py")
print("2. Check Sector Overview > expand any sector")
print("3. Verify 'Real-time Prices' cards appear")
print("4. Colors should be:")
print("   - 🟢 Green for positive change")
print("   - 🔴 Red for negative change")
print("   - 🟡 Yellow for no change")

print("\n💡 Tips:")
print("- Prices update every 30 seconds (Streamlit cache)")
print("- Force refresh with browser F5")
print("- Run realtime_consumer.py to get live data from Kafka")
