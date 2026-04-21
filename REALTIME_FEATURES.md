# 🚀 Real-time Stock Price Features - Dashboard Updates

## ✨ ฟีเจอร์ใหม่ที่เพิ่มมา

### 1. **Real-time Price Display by Sector**
ในแต่ละ Sector expansion panel:
- แสดงราคาหุ้นล่าสุด (Live Price) ในรูปแบบ **Grid Cards**
- สีเขียว (🟢) หากราคา **เพิ่มขึ้น** `↑`
- สีแดง (🔴) หากราคา **ลดลง** `↓`
- สีเหลือง (🟡) หากราคา **ไม่เปลี่ยน** `→`

### 2. **Live Prices - Top Winners & Losers**
ในแท็บ "Top Winners & Losers":
- แสดง 5-20 หุ้นที่มี performance ดีสุดและแย่สุด
- มี Real-time price cards ขนาดเล็กสำหรับแต่ละหุ้น
- Color-coded ตามการเปลี่ยนแปลงราคา

### 3. **Individual Stock Live Price**
ในแท็บ "Stock Search":
- แสดงราคา Real-time **ขนาดใหญ่** สำหรับหุ้นที่ค้นหา
- บอร์ดด้านข้างสวย ๆ ที่ highlight ตัวเลขราคา
- แสดงเปอร์เซ็นต์การเปลี่ยนแปลง
- สีเปลี่ยนแปลงไปตามการเพิ่มขึ้น/ลดลง

---

## 📊 ข้อมูลที่ใช้

### Data Sources:
1. **live_trades Collection** - ข้อมูล Real-time จาก Kafka Consumer
   - `symbol`: รหัสหุ้น
   - `price`: ราคาปัจจุบัน
   - `base_price`: ราคาเปิด (baseline)
   - `live_war_return_pct`: เปอร์เซ็นต์การเปลี่ยนแปลง
   - `status`: สถานะ (alert_spike, alert_crash, normal)

2. **gold_stock_ranking** - ข้อมูลจากการวิเคราะห์
   - สเตตุสการจัดอันดับ
   - War Impact Labels

---

## 🎨 Design Details

### Color Scheme:
- **🟢 Green (#22c55e)**: Price Up / Positive Impact
- **🔴 Red (#dc2626)**: Price Down / Negative Impact
- **🟡 Yellow (#fbbf24)**: Neutral / No Change

### Card Styling:
- Bordered cards with 2-3px solid border
- Semi-transparent background (rgba with 0.1 opacity)
- Rounded corners (8-12px border-radius)
- Responsive grid layout (4 columns on desktop)
- Emoji indicators: 🔼 Up, 🔽 Down, ➡️ Neutral

---

## 🔄 Refresh Logic

- **Streamlit Rerun**: ทุก 30 วินาที (default)
- **Live Data Source**: `live_trades` collection
- **Query**: Last document by timestamp สำหรับแต่ละ symbol

---

## ⚙️ Technical Implementation

### Helper Functions:

#### `fetch_realtime_prices(symbols_list)`
```python
def fetch_realtime_prices(symbols_list):
    """ดึงราคา real-time จาก live_trades collection"""
    # Returns dict with symbol -> {price, prev_close, change_pct, status}
```

#### `format_price_change(current_price, prev_price, change_pct)`
```python
def format_price_change(current_price, prev_price, change_pct):
    """Format ราคาและการเปลี่ยนแปลงพร้อมสี"""
    # Returns dict with color, arrow, HTML formatted string
```

---

## 📍 Location in Dashboard

| Page/Tab | Feature | Location |
|----------|---------|----------|
| **Sector Overview** | Real-time Prices Grid | Under each sector expansion |
| **Top Winners & Losers** | Top Winner Prices | Below Top 20 Winners table |
| **Top Winners & Losers** | Top Loser Prices | Below Top 20 Losers table |
| **Stock Search** | Large Price Display | Above metrics (m1-m8) |

---

## 🚀 Future Enhancements

- [ ] Add price update timestamp
- [ ] Add 24-hour high/low
- [ ] Add trading volume
- [ ] Add candlestick charts
- [ ] Auto-refresh rate slider
- [ ] Alert notifications for price movements
- [ ] Historical comparison (vs last month, last year)

---

## 📝 Notes

- ข้อมูล Real-time ขึ้นอยู่กับ `live_trades` collection ที่มีข้อมูลจาก Kafka Consumer
- ถ้าไม่มีข้อมูล Real-time จะแสดง "📡 ยังไม่มีข้อมูล Real-time" 
- สีจะเปลี่ยนตามเปอร์เซ็นต์การเปลี่ยนแปลง (`live_war_return_pct`)

---

**Last Updated**: 21 เมษายน 2569  
**Version**: 1.0
