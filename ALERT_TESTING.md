# Alert System — Testing Guide

วิธีทดสอบระบบ Alert ทั้ง 2 แบบในโปรเจกต์

---

## ภาพรวมระบบ Alert

โปรเจกต์มี alert 2 ระบบที่แยกกันชัด:

| ระบบ | ทำงานเมื่อ | ส่งจาก | ใช้เพื่อ |
|---|---|---|---|
| **Realtime Price Alert** | ราคาหุ้น/volume เกินเกณฑ์ | `realtime_consumer.py` | แจ้งนักลงทุน |
| **Pipeline Failure Alert** | Airflow task fail | `dags/pipelines/*.py` (callback) | แจ้ง admin/devops |

ทั้ง 2 ระบบส่งผ่าน **Discord Webhook** เดียวกัน

---

## Pre-requisites

ก่อน test ให้แน่ใจว่า:

1. **Discord webhook ตั้งค่าใน `.env`:**
   ```
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
   ```

2. **ทุก service รันอยู่:**
   ```bash
   docker-compose ps
   ```
   ต้องเห็น: kafka, mongodb, realtime-producer, realtime-consumer, airflow-scheduler, airflow-webserver

3. **Discord channel เปิดดูได้** — เพื่อรับ alert

---

## Test 1: Realtime Price Alert

### วัตถุประสงค์
ทดสอบว่าระบบส่ง alert เข้า Discord เมื่อราคาหุ้นเคลื่อนไหวเกินเกณฑ์

### เงื่อนไข Alert
- `alert_spike` → ราคา > +5% vs baseline
- `alert_crash` → ราคา < -5% vs baseline
- `alert_extreme_up` → ราคา > +10%
- `alert_extreme_down` → ราคา < -10%
- `alert_war_sensitive` → หุ้นใน sector negative + ราคาตก
- `alert_volume_spike` → volume > 2x baseline

### วิธี Test แบบ Manual (ไม่ต้องรอตลาด)

**Step 1: Copy test script เข้า container**
```bash
docker cp test_alert.py realtime_consumer:/app/test_alert.py
```

**Step 2: รัน test**
```bash
docker exec realtime_consumer python test_alert.py
```

**ผลลัพธ์ที่คาดหวัง:**
```
Test 1: Price alert (alert_spike)
  → Result: True

Test 2: Extreme down alert
  → Result: True

Test 3: Pipeline failure alert
  → Result: True

✅ Done — เช็ค Discord channel
```

**ใน Discord ควรเห็น 3 embeds:**
- 🟢 ⬆️ Price Spike — TEST +7.50%
- 🔴 💥 Extreme Crash — TEST2 -12.30%
- 🚨 Pipeline Critical Error

### วิธี Test แบบรอเหตุการณ์จริง

ถ้าตลาดเปิดและมีการเคลื่อนไหว ระบบจะส่ง alert อัตโนมัติ:

**ดู log consumer:**
```bash
docker-compose logs -f realtime-consumer
```

มองหาบรรทัด:
```
[🚨 ALERT] XOM @ 142.50 | Base: 135.00 | Impact: +5.56% | Alerts: ['alert_spike']
📨 Discord alert sent: XOM alert_spike
```

### Cooldown Mechanism

ระบบมี cooldown 5 นาที (ตั้งใน `.env` ด้วย `ALERT_COOLDOWN_SECONDS=300`)

หุ้นตัวเดียวกัน + alert type เดียวกัน จะถูกส่งซ้ำใน 5 นาทีไม่ได้
ป้องกัน spam Discord channel

---

## Test 2: Pipeline Failure Alert

### วัตถุประสงค์
ทดสอบว่าระบบส่ง alert เข้า Discord เมื่อ Airflow DAG fail

### เงื่อนไข Alert
- Task error/exception
- DQ rule CRITICAL fail (เช่น null > 5%)
- Retry หมดแล้วยัง fail → ส่ง Discord

### วิธี Test แบบ Direct (เร็วสุด — ไม่ต้องรอ retry)

ทดสอบว่า callback function ทำงานถูกต้อง:

```bash
docker exec data-engineering-airflow-scheduler-1 bash -c "cd /opt/airflow/dags && python -c '
from utils.alert_notifier import send_critical_pipeline_alert
result = send_critical_pipeline_alert(\"TEST: simulated failure\", \"transform_historical_to_silver\")
print(f\"Result: {result}\")
'"
```

**ผลลัพธ์ที่คาดหวัง:**
```
Result: True
```

**ใน Discord:** 🚨 Pipeline Critical Error embed สีแดง

### วิธี Test แบบ End-to-End (จำลอง pipeline พังจริง)

จำลอง MongoDB outage:

**Step 1: หยุด MongoDB**
```bash
docker-compose stop mongodb
```

**Step 2: Trigger DAG**

ไป http://localhost:8080 → `sp500_daily_analytics_pipeline` → กด trigger

**Step 3: รอประมาณ 5 นาที**

DAG flow เมื่อ MongoDB down:
```
load_sp500_daily_to_bronze → fail (ServerSelectionTimeoutError)
  ↓ retry #1 (รอ 2 นาที) → fail
  ↓ retry #2 (รอ 2 นาที) → fail
  ↓ on_failure_callback → ส่ง Discord 🚨
```

**Step 4: เช็ค Discord**

ควรเห็น:
```
🚨 Pipeline Critical Error
DAG: sp500_daily_analytics_pipeline
Task: load_sp500_daily_to_bronze
Attempt: 3
Error: mongodb:27017: [Errno -2] Name or service not known...
Log: http://localhost:8080/log?...
```

**Step 5: กู้คืนระบบ**
```bash
docker-compose start mongodb
```

รอ ~10 วินาที

**Step 6: Trigger DAG อีกครั้ง**

ทุก task ต้อง success — ยืนยันระบบกลับมาทำงานปกติ

---

## Troubleshooting

### Discord ส่งไม่ได้ — HTTP 403 Forbidden

**สาเหตุ:** Webhook URL ถูก revoke (อาจเพราะถูกแชร์ในที่สาธารณะ)

**แก้:**
1. สร้าง webhook ใหม่ใน Discord
2. แก้ `.env` ใส่ URL ใหม่
3. Recreate container ที่เกี่ยวข้อง:
   ```bash
   docker-compose up -d --force-recreate realtime-consumer airflow-scheduler airflow-webserver
   ```

### Discord ส่งไม่ได้ — HTTP 404 Not Found

**สาเหตุ:** Webhook URL ผิด หรือ container ใช้ URL เก่า

**เช็คว่า container ใช้ URL อะไร:**
```bash
docker exec realtime_consumer printenv DISCORD_WEBHOOK_URL
docker exec data-engineering-airflow-scheduler-1 printenv DISCORD_WEBHOOK_URL
```

ทั้ง 2 ต้องตรงกัน และตรงกับใน `.env`

ถ้าต่างกัน → recreate ตามด้านบน

### Pipeline fail แต่ไม่มี alert

**เช็ค 1:** ไฟล์ `dags/utils/alert_notifier.py` มีอยู่ไหม
```bash
ls dags/utils/alert_notifier.py
```

**เช็ค 2:** DAG file มี import + callback
```bash
grep -n "alert_notifier\|on_failure_callback" dags/pipelines/sp500_daily_analytics_pipeline.py
```

ต้องเห็น 2 บรรทัด

**เช็ค 3:** test callback ตรงๆ (ดู Test แบบ Direct ด้านบน)

### Alert ส่งซ้ำๆ มากเกินไป

ปรับ cooldown ใน `.env`:
```
ALERT_COOLDOWN_SECONDS=600   # 10 นาที
```

แล้ว recreate consumer:
```bash
docker-compose up -d --force-recreate realtime-consumer
```

---

## Reference

### Discord Embed Format

ระบบใช้ Discord Embed สำหรับ alert ทุกแบบ:

**Realtime Price Alert:**
- สีตามประเภท alert (เขียว = ขึ้น, แดง = ลง)
- Fields: Symbol, Price, Impact %, Sector, War Impact, Volume
- Footer: "S&P 500 War Impact Monitor"

**Pipeline Alert:**
- สีแดงเข้ม (#991B1B)
- Description: DAG, Task, Attempt, Error, Log link
- Footer: "Airflow Pipeline Alert"

### Files Involved

```
realtime/
├── alert_notifier.py        # Discord client (consumer side)
└── realtime_consumer.py     # เรียก send_discord_alert()

dags/
├── utils/
│   └── alert_notifier.py    # Discord client (Airflow side, สำเนาเดียวกัน)
└── pipelines/
    └── sp500_daily_analytics_pipeline.py  # มี on_failure_callback

.env                          # DISCORD_WEBHOOK_URL, ALERT_COOLDOWN_SECONDS
```

### Configuration

ใน `.env`:
```bash
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/xxx/yyy
ALERT_COOLDOWN_SECONDS=300
```

ใน `realtime_consumer.py` (price thresholds):
```python
SPIKE_THRESHOLD      =  5.0    # %
CRASH_THRESHOLD      = -5.0
EXTREME_UP_THRESHOLD =  10.0
EXTREME_DN_THRESHOLD = -10.0
VOLUME_SPIKE_MULTIPLIER = 2.0
```

ใน DAG file (retry config):
```python
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,
}
```
