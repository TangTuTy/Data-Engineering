# S&P 500 — War Impact Analysis Pipeline

ระบบ Data Engineering แบบ End-to-End สำหรับวิเคราะห์ผลกระทบของสงคราม US-Iran (เริ่ม 1 ม.ค. 2026) ต่อหุ้นในดัชนี S&P 500 ครอบคลุม 503 หุ้น 11 sectors ใช้แนวคิด **Medallion Architecture** (Bronze → Silver → Gold) พร้อม **Star Schema** ใน Gold Layer มีทั้งฝั่ง **Batch** และ **Real-time Streaming**

---

## Architecture Overview

### Batch Pipeline
```
Sources : Wikipedia, yfinance
   ↓
Bronze  : sp500_tickers, historical_prices, company_profiles
   ↓
Silver  : silver_historical_daily, silver_stock_war_metrics,
          silver_war_impact_analysis, silver_company_enriched
   ↓
Gold    : gold_sector_war_summary, gold_stock_ranking
   ↓
Star    : fact_war_analytics, dim_date, dim_company, dim_sector [Kimball Star Schema]
```

### Real-time Pipeline
```
Finnhub WebSocket → Kafka Producer → Kafka Topic
                                   → Kafka Consumer
                                   → MongoDB (live_trades, TTL 7 days)
                                   → Discord Price Alerts
```

### Monitoring
```
Airflow Task Failure (ทุก task) → on_failure_callback → Discord Pipeline Alert
Realtime Price Anomaly          → Kafka Consumer       → Discord Price Alert
```

**Stack:** Apache Airflow 2.7.1 · MongoDB · Apache Kafka (KRaft) · Streamlit · Docker Compose · Python 3.11

---

## Key Features

- ✅ **Medallion Architecture** — Bronze + Silver + Gold layers
- ✅ **Star Schema (Kimball)** — fact_war_analytics + 3 dimensions (date, company, sector)
- ✅ **Incremental Processing** — Watermark pattern + Bulk Upsert (idempotent)
- ✅ **Data Quality Gate** — 5 rules (completeness, uniqueness, validity, freshness, referential integrity)
- ✅ **Real-time Streaming** — Continuous Kafka consumer + Finnhub WebSocket
- ✅ **Smart Symbol Selection** — เลือก 50 หุ้นจากผล Batch Analysis (dim_company + dim_sector)
- ✅ **Discord Alert System** — Price alerts (6 ประเภท) + Pipeline failure alerts
- ✅ **TTL Auto-cleanup** — live_trades ลบอัตโนมัติหลัง 7 วัน ป้องกัน collection โตไม่หยุด
- ✅ **Timezone-aware Schedule** — DAG รันตาม America/New_York (18:00 ET หลังตลาดปิด)

---

## Setup

### 1. Clone project
```bash
git clone <repo-url>
cd Data-Engineering
```

### 2. สร้างไฟล์ `.env`
```bash
cp .env.example .env
```

### 3. ตั้งค่า API Tokens

**Finnhub API** (สำหรับ realtime):
- สมัครที่ https://finnhub.io/register (ฟรี)
- Copy API token จาก Dashboard

**Discord Webhook** (สำหรับ alerts):
- ไปที่ Discord channel → Edit Channel → Integrations → Webhooks → New Webhook
- Copy Webhook URL

ใส่ใน `.env`:
```bash
FINNHUB_TOKEN=<your_finnhub_token>
DISCORD_WEBHOOK_URL=<your_discord_webhook_url>
ALERT_COOLDOWN_SECONDS=300
```

> **หมายเหตุ:** ถ้าไม่ตั้ง DISCORD_WEBHOOK_URL ระบบ alert จะไม่ส่ง แต่ pipeline ยังทำงานได้ปกติ

### 4. Start ระบบ
```bash
docker-compose up -d
```

### 5. เปิด Web UI
- **Airflow:** http://localhost:8080 (user: `airflow` / pass: `airflow`)
- **Streamlit Dashboard:** http://localhost:8501

---

## ลำดับการรัน DAG

### กรณีเริ่มระบบใหม่ (ครั้งแรก)
1. `sp500_reference_data_pipeline` — ดึงรายชื่อหุ้นจาก Wikipedia
2. `sp500_backfill_pipeline` — ดึง historical prices ย้อนหลัง 1 ปี 
3. `sp500_company_profiles_pipeline` — ดึงข้อมูลบริษัทจาก yfinance 
4. `sp500_daily_analytics_pipeline` — Build Silver + Gold + Star Schema

### กรณีใช้งานปกติ

| DAG | Schedule | หน้าที่ |
|---|---|---|
| `sp500_reference_data_pipeline` | `0 9 * * 1` (จันทร์ 09:00 UTC) | อัปเดตรายชื่อหุ้น S&P 500 |
| `sp500_company_profiles_pipeline` | `@monthly` | Smart Refresh (skip ถ้า < 30 วัน) |
| `sp500_daily_analytics_pipeline` | `0 18 * * 1-5` (จ-ศ 18:00 ET) | Build Silver + Gold + Star Schema |
| `sp500_backfill_pipeline` | manual | Backfill กรณีต้องการ |

> **Note:** Daily pipeline ใช้ timezone `America/New_York` — 18:00 ET = 2 ชม. หลัง NYSE ปิด (16:00 ET)

---

## MongoDB Collections

### Bronze Layer (raw data)
| Collection | จำนวน | คำอธิบาย |
|---|---|---|
| `sp500_tickers` | ~503 | รายชื่อหุ้น S&P 500 + GICS Sector จาก Wikipedia |
| `historical_prices` | ~129,000 | OHLCV รายวันจาก yfinance |
| `company_profiles` | ~503 | ข้อมูลบริษัทจาก yfinance |
| `live_trades` | streaming | Real-time prices จาก Finnhub (TTL 7 วัน) |

### Silver Layer (cleaned + enriched)
| Collection | จำนวน | คำอธิบาย |
|---|---|---|
| `silver_historical_daily` | ~129,644 | + daily_return, MA7, MA30, period (pre_war/war) |
| `silver_stock_war_metrics` | ~503 | metrics ระดับหุ้น (pre-war vs war) |
| `silver_war_impact_analysis` | 11 | sector-level war impact analysis |
| `silver_company_enriched` | ~503 | + war_impact + sector fallback |
| `silver_live_trades` | streaming | + sector + war_impact |

### Gold Layer (aggregation → absorbed into Star Schema)
| Collection | คำอธิบาย | ถูกใช้โดย |
|---|---|---|
| `gold_sector_war_summary` | สรุปรายกลุ่มอุตสาหกรรม | → `dim_sector` |
| `gold_stock_ranking` | จัดอันดับหุ้น winners/losers | → `dim_company` |

### Star Schema (Kimball)
| Collection | จำนวน | บทบาท |
|---|---|---|
| `fact_war_analytics` | ~127,129 | **Fact Table** — FK: date_sk, company_sk, sector_sk |
| `dim_date` | ~253 | มิติวันที่ + period + week_key |
| `dim_company` | ~503 | มิติบริษัท + war_impact + war_latest_close |
| `dim_sector` | 11 | มิติ sector + war_impact_label + top_5 |

### Operational
| Collection | คำอธิบาย |
|---|---|
| `pipeline_state` | Watermark สำหรับ Incremental Load |
| `dq_check_results` | ประวัติ Data Quality checks |

---

## Project Structure

```
Data-Engineering/
├── docker-compose.yml          # 7 services orchestration
├── Dockerfile.consumer         # Realtime consumer
├── Dockerfile.producer         # Realtime producer
├── .env.example                # Environment template
├── .gitignore
│
├── app.py                      # Streamlit Dashboard (9 sections)
├── requirements-streamlit.txt
├── requirements-airflow.txt
│
├── .streamlit/
│   └── config.toml             # Dark mode theme
│
├── dags/
│   ├── bronze/
│   │   ├── bronze_layer.py             # Daily price ingestion (5d window)
│   │   ├── backfill_layer.py           # 1-year historical backfill
│   │   ├── company_profiles_layer.py   # Smart refresh (30-day TTL)
│   │   └── reference_data.py           # Wikipedia S&P 500 list
│   │
│   ├── silver/
│   │   └── silver_layer.py             # 4 transformations + DQ
│   │
│   ├── gold/
│   │   └── gold_layer.py               # 2 Gold tables + 4 Star Schema tables
│   │
│   ├── pipelines/
│   │   ├── sp500_daily_analytics_pipeline.py   # Main DAG (11 tasks, TZ-aware)
│   │   ├── sp500_backfill_pipeline.py
│   │   ├── sp500_company_profiles_pipeline.py
│   │   └── sp500_reference_data_pipeline.py
│   │
│   └── utils/
│       ├── __init__.py
│       ├── data_quality.py             # 5-rule DQ checker + MongoDB persist
│       └── alert_notifier.py           # Discord webhook (Airflow side)
│
├── realtime/
│   ├── realtime_producer.py    # Finnhub → Kafka (Smart 50 from dim_company)
│   ├── realtime_consumer.py    # Kafka → MongoDB + Alerts (baseline from fact)
│   └── alert_notifier.py       # Discord webhook (Consumer side)
│
├── test_alert.py               # Manual alert testing
├── ALERT_TESTING.md            # Alert system testing guide
└── README.md
```

---

## Real-time Streaming

### Smart Symbol Selection (50 หุ้น)
Producer เลือกหุ้นอัตโนมัติจาก **dim_company** + **dim_sector** (Star Schema):
- 15 Top Winners (performance_shift บวกสูงสุด)
- 15 Top Losers (performance_shift ลบมากสุด)
- 10 Sector Flagships (market_cap สูงสุดใน war-sensitive sectors)
- 5 Mega-cap Control (AAPL, MSFT, GOOGL, AMZN, BRK-B)
- 5 High Volatility Buffer

### Baseline Comparison
Consumer ดึง baseline price + volume จาก **fact_war_analytics** (ไม่ใช่ Bronze) เพื่อเทียบกับราคา realtime

### Continuous Streaming
Consumer ใช้ `for message in consumer:` วนรับ message จาก Kafka ตลอดเวลา ไม่ใช่ micro-batch

---

## Alert System

### Price Alert (6 ประเภท)
| Alert | เงื่อนไข |
|---|---|
| `alert_extreme_up` | ราคา > +10% |
| `alert_spike` | ราคา > +5% |
| `alert_extreme_down` | ราคา < -10% |
| `alert_crash` | ราคา < -5% |
| `alert_war_sensitive` | sector negative + ราคาตก > 5% |
| `alert_volume_spike` | volume > 2× baseline (30 วัน) |

มี **Cooldown 5 นาที** ป้องกัน spam (หุ้นตัวเดียวกัน + alert type เดียวกัน)

### Pipeline Failure Alert
- ตั้งค่า `on_failure_callback` ในทุก task ของ Airflow DAG
- Retry 2 ครั้ง × 2 นาที ก่อนแจ้งเตือน
- Embed แสดง DAG, Task, Error, Log link

ดูคู่มือทดสอบ alert ที่ [ALERT_TESTING.md](./ALERT_TESTING.md)

---

## Data Quality

ระบบตรวจสอบข้อมูลด้วย `DataQualityChecker` (5 rules):

| Rule | คำอธิบาย | Severity |
|---|---|---|
| Completeness | ตรวจ null ใน fields สำคัญ | CRITICAL (>5%) / WARNING (<5%) |
| Uniqueness | ตรวจ duplicate ตาม composite key | CRITICAL |
| Validity | ตรวจค่าอยู่ใน valid range | WARNING |
| Freshness | ตรวจอายุข้อมูลไม่เกิน 7 วัน | WARNING |
| Referential Integrity | ตรวจ FK มีอยู่ใน reference table | WARNING |

DQ Gate ทำงานก่อน insert ทุกครั้ง — ถ้า CRITICAL → raise exception → Airflow retry → Discord alert

ผลทุกครั้งถูกบันทึกลง `dq_check_results` สำหรับ audit

---

## Star Schema Query Example

ตัวอย่าง query หา Top 5 หุ้นใน Energy sector ช่วงสงคราม:

```javascript
db.fact_war_analytics.aggregate([
  { $lookup: {
      from: "dim_company",
      localField: "company_sk",
      foreignField: "company_sk",
      as: "co"
  }},
  { $lookup: {
      from: "dim_sector",
      localField: "sector_sk",
      foreignField: "sector_sk",
      as: "sec"
  }},
  { $lookup: {
      from: "dim_date",
      localField: "date_sk",
      foreignField: "date_sk",
      as: "dt"
  }},
  { $unwind: "$co" }, { $unwind: "$sec" }, { $unwind: "$dt" },
  { $match: { "sec.sector": "Energy", "dt.period": "war" }},
  { $group: {
      _id: "$co.symbol",
      full_name: { $first: "$co.full_name" },
      avg_return: { $avg: "$daily_return_pct" }
  }},
  { $sort: { avg_return: -1 }},
  { $limit: 5 }
])
```

---

## Testing Idempotency

ทดสอบว่า Pipeline รันซ้ำได้โดยไม่เกิดข้อมูลซ้ำ:

```bash
# Run #1 (First Run)
# → Bulk upsert: 129,644 new + 0 updated
# → Watermark: 2026-04-24

# Run #2 (Incremental — รันซ้ำทันที)
# → Bulk upsert: 0 new + 10,060 updated  ← idempotent ผ่าน
# → records ใน MongoDB เท่าเดิม
```

---

## Troubleshooting

### Airflow ไม่เห็น DAG
```bash
docker exec data-engineering-airflow-scheduler-1 airflow dags list-import-errors
```

### Discord alert ไม่ส่ง (HTTP 403/404)
1. เช็ค webhook ใน .env: `cat .env | grep DISCORD`
2. เช็ค container ได้ URL: `docker exec realtime_consumer printenv DISCORD_WEBHOOK_URL`
3. ถ้า URL ไม่ตรง: `docker-compose up -d --force-recreate realtime-consumer airflow-scheduler airflow-webserver`

### Kafka ไม่ทำงาน
```bash
docker-compose logs kafka --tail=50
docker-compose restart kafka
```

### MongoDB query
```bash
docker exec -it mongodb mongosh
use stock_database
db.getCollectionNames()
```

---

## Documentation

- [ALERT_TESTING.md](./ALERT_TESTING.md) — คู่มือทดสอบ Alert System ทั้ง 2 ระบบ

---

## License

Educational project — Mahidol University ITDS344 Data Engineering
