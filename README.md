# S&P 500 — War Impact Analysis Pipeline

Data Engineering pipeline สำหรับวิเคราะห์ผลกระทบของสงคราม/ความตึงเครียดทางภูมิรัฐศาสตร์ต่อหุ้นในดัชนี S&P 500 ใช้แนวคิด **Medallion Architecture** (Bronze → Silver → Gold) มีทั้งฝั่ง **Batch** และ **Realtime**

---

## Architecture Overview

**Batch Pipeline** (S&P 500 ทั้งหมด ~503 หุ้น)
```
Wikipedia → sp500_tickers (Bronze)
yfinance  → historical_prices, company_profiles (Bronze)
          → silver_historical_daily, silver_stock_war_metrics (Silver)
          → gold_sector_war_summary, gold_stock_ranking (Gold)
```

**Realtime Pipeline** (watchlist ~50 หุ้น)
```
Finnhub WebSocket → Kafka → MongoDB (live_trades) → Alerts & Dashboard
```

**Stack:** Apache Airflow · MongoDB · Kafka · Streamlit · Docker Compose

---

## Setup สำหรับสมาชิกในกลุ่ม

### 1. Clone project
```bash
git clone <repo-url>
cd data-engineering
```

### 2. สร้างไฟล์ `.env`
```bash
cp .env.example .env
```

### 3. (Optional) สมัคร Finnhub API — เฉพาะคนที่จะรัน realtime
- ไปที่ https://finnhub.io/register
- สมัครบัญชี (ฟรี)
- Copy API token จาก Dashboard
- ใส่ใน `.env`:
  ```
  FINNHUB_TOKEN=<your_token_here>
  ```

> **หมายเหตุ:** ถ้าไม่รัน realtime producer ก็ไม่จำเป็นต้องใส่ FINNHUB_TOKEN

### 4. Start ระบบ
```bash
docker-compose up -d
```

### 5. เปิด Web UI
- Airflow: http://localhost:8080
- Streamlit Dashboard: http://localhost:8501

---

## ลำดับการรัน DAG

### กรณีเริ่มระบบใหม่ (ครั้งแรก)
1. `sp500_reference_data_pipeline` — ดึงรายชื่อหุ้นจาก Wikipedia
2. `sp500_backfill_pipeline` — ดึง historical prices ย้อนหลัง 1 ปี (ใช้เวลานาน)
3. `sp500_company_profiles_pipeline` — ดึงข้อมูลบริษัทจาก yfinance
4. `sp500_daily_analytics_pipeline` — Build Silver + Gold layers

### กรณีใช้งานปกติ
| DAG | Schedule | หน้าที่ |
|---|---|---|
| `sp500_reference_data_pipeline` | weekly | อัปเดตรายชื่อหุ้น |
| `sp500_company_profiles_pipeline` | monthly | อัปเดต company profiles |
| `sp500_daily_analytics_pipeline` | daily | Build Silver + Gold ใหม่ |
| `sp500_backfill_pipeline` | manual | Trigger เอง ถ้าต้อง backfill |

---

## โครงสร้างไฟล์

```
data-engineering/
├── dags/
│   ├── bronze/
│   │   ├── reference_data.py       # ดึง S&P 500 จาก Wikipedia
│   │   ├── backfill_layer.py       # ดึง historical prices
│   │   ├── bronze_layer.py         # ดึง daily prices
│   │   └── company_profiles_layer.py
│   ├── silver/
│   │   └── silver_layer.py         # transform + war impact analysis
│   ├── gold/
│   │   └── gold_layer.py           # aggregate summaries
│   └── pipelines/                  # DAG definitions
├── realtime/
│   ├── realtime_producer.py        # Finnhub → Kafka
│   └── realtime_consumer.py        # Kafka → MongoDB + alerts
├── app.py                          # Streamlit dashboard
├── docker-compose.yml
├── requirements.txt
├── .env                            # Secrets (ห้าม commit)
└── .env.example                    # Template
```

---

## MongoDB Collections

### Bronze (raw)
- `sp500_tickers` — รายชื่อ S&P 500 จาก Wikipedia
- `historical_prices` — ราคาหุ้น daily ย้อนหลัง
- `company_profiles` — ข้อมูลบริษัทจาก yfinance
- `live_trades` — realtime จาก Finnhub

### Silver (cleaned + enriched)
- `silver_historical_daily` — daily prices + period (pre_war / war)
- `silver_stock_war_metrics` — metrics ราย symbol
- `silver_war_impact_analysis` — sector-level analysis
- `silver_company_enriched` — company profiles + war impact
- `silver_live_trades` — live trades + sector

### Gold (business-ready)
- `gold_sector_war_summary` — สรุปรายกลุ่ม
- `gold_stock_ranking` — จัดอันดับหุ้น
- `gold_weekly_sector_performance` — สัปดาห์
- `gold_war_daily_timeline` — รายวัน timeline

---

## Realtime Alert Types

Consumer ตรวจจับ alert 6 ประเภทและเก็บใน `live_trades.status`:

| Alert | เงื่อนไข |
|---|---|
| `alert_extreme_up` | ราคา > +10% vs baseline |
| `alert_spike` | ราคา > +5% vs baseline |
| `alert_extreme_down` | ราคา < -10% vs baseline |
| `alert_crash` | ราคา < -5% vs baseline |
| `alert_war_sensitive` | หุ้นใน sector negative + ราคาตก |
| `alert_volume_spike` | volume > 2x baseline |

---

## Troubleshooting

**Backfill ช้า**
Bottleneck อยู่ที่ MongoDB write (CPU ~100%) ไม่ใช่ Yahoo API

**Silver พังเพราะ Date type ไม่ตรง**
`silver_layer.py` มี `_to_date()` แปลงทั้ง datetime, date, และ string แล้ว

**Realtime ครอบคลุมแค่ ~50 หุ้น**
Finnhub free tier จำกัด จึงใช้เป็น watchlist ไม่ใช่ทั้ง S&P 500

---

## Team

Data Engineering Project — S&P 500 War Impact Analysis
