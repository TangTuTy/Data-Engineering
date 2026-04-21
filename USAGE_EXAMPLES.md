# 🎯 ตัวอย่างการใช้งาน Real-time Features

## 📸 Visual Examples

### Example 1: Sector Overview with Real-time Prices

**Before (Old):**
```
🏭 Technology Sector — Shift: +0.45%/day | Impact: strong_positive | 45 stocks

[Table with 45 stocks...]
```

**After (New):**
```
🏭 Technology Sector — Shift: +0.45%/day | Impact: strong_positive | 45 stocks

[Table with 45 stocks...]

💰 Real-time Prices
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   AAPL       │  │   MSFT       │  │   GOOG       │  │   NVDA       │
│ 🔼 $150.25   │  │ 🔽 $245.50   │  │ 🔼 $92.75    │  │ 🔼 $950.00   │
│  +3.62%      │  │  -1.80%      │  │  +1.48%      │  │  +4.40%      │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   AMZN       │  │   TSLA       │  │   META       │
│ 🔼 $165.30   │  │ 🔽 $210.75   │  │ 🔼 $385.50   │
│  +0.95%      │  │  -0.55%      │  │  +2.10%      │
└──────────────┘  └──────────────┘  └──────────────┘
```

---

### Example 2: Top Winners & Losers

**Before (Old):**
```
🚀 Top 20 Winners                          📉 Top 20 Losers
[Table: Rank, Symbol, Name, Sector, Return]   [Table: Rank, Symbol, Name, Sector, Return]
```

**After (New):**
```
🚀 Top 20 Winners
[Table: Rank, Symbol, Name, Sector, Return]

💰 Live Prices - Top Winners
┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐
│ NVDA   │  │ AAPL   │  │ MSFT   │  │ TSLA   │  │ AMZN   │
│ 🔼 $950│  │ 🔼 $150│  │ 🔽 $245│  │ 🔼 $280│  │ 🔼 $165│
│ +4.40% │  │ +3.62% │  │ -1.80% │  │ +2.85% │  │ +0.95% │
└────────┘  └────────┘  └────────┘  └────────┘  └────────┘

---

📉 Top 20 Losers
[Table: Rank, Symbol, Name, Sector, Return]

💰 Live Prices - Top Losers
┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐
│ XYZ    │  │ ABC    │  │ DEF    │  │ GHI    │  │ JKL    │
│ 🔽 $50 │  │ 🔽 $75 │  │ 🔽 $120│  │ 🔽 $45 │  │ 🔽 $80 │
│ -5.23% │  │ -3.45% │  │ -2.10% │  │ -1.85% │  │ -1.50% │
└────────┘  └────────┘  └────────┘  └────────┘  └────────┘
```

---

### Example 3: Stock Search with Large Price Board

**Before (Old):**
```
🔍 Search Individual Stock
[Text input: Enter symbol]

AAPL — Apple Inc.
[4 metrics] [4 metrics]
[Price history chart]
```

**After (New):**
```
🔍 Search Individual Stock
[Text input: Enter symbol]

🟢 AAPL — Apple Inc.

┌────────────────────────────────────────────┐
│                                            │
│          🔼 $150.25                        │
│            +3.62%                          │
│          Live Price Data                   │
│                                            │
└────────────────────────────────────────────┘

[4 metrics: Rank, Sector, War Impact, Performance]
[4 metrics: War Return, Pre-War Return, Volatility, Daily Return]

Price History — AAPL
[Line chart showing price over time]
```

---

## 🎨 Color Reference

### Color Codes Used

```python
# Green (Positive)
color = "#22c55e"  # RGB: 34, 197, 94
emoji = "🔼"
text = "Price UP"
example = "+3.62%"

# Red (Negative)
color = "#dc2626"  # RGB: 220, 38, 38
emoji = "🔽"
text = "Price DOWN"
example = "-2.15%"

# Yellow (Neutral)
color = "#fbbf24"  # RGB: 251, 191, 36
emoji = "➡️"
text = "No Change"
example = "±0.05%"
```

---

## 🎯 Data Examples

### Example Real-time Price Data

```json
{
  "symbol": "AAPL",
  "price": 150.25,
  "base_price": 145.00,
  "live_war_return_pct": 3.62,
  "status": "alert_spike",
  "timestamp": "2025-04-21T10:30:00Z"
}
```

**Calculated:**
- Change: 150.25 - 145.00 = 5.25
- % Change: (5.25 / 145.00) * 100 = 3.62%
- **Color**: 🟢 Green (positive)
- **Arrow**: 🔼 (up)

---

## 🔄 Update Sequence

### Real-time Flow

```
Time: 10:30:00 → Kafka sends: {"AAPL": 150.25}
                    ↓
         realtime_consumer.py processes
                    ↓
         MongoDB live_trades updated
                    ↓
         fetch_realtime_prices() called
                    ↓
         format_price_change() returns:
         {color: "#22c55e", arrow: "🔼", change_pct: 3.62}
                    ↓
         Dashboard renders card:
         ┌──────────┐
         │ AAPL     │
         │ 🔼 $150  │
         │ +3.62%   │
         └──────────┘

Time: 10:30:30 → (Cache expires - refreshes above process)
Time: 10:31:00 → (Cache expires - refreshes above process)
```

---

## 📊 Performance Examples

### Query Performance

```
Query 1 symbol:   ~50ms
Query 5 symbols:  ~150ms
Query 50 symbols: ~500ms (with proper index)

With @st.cache_data(ttl=30):
- First call:   ~500ms (queries MongoDB)
- Next 29s:     ~5ms (returns cached data)
```

---

## 💬 User Interaction Examples

### User Story 1: Tech Sector Analysis
```
1. User opens Dashboard
2. Clicks "🏭 Technology Sector" expander
3. Sees table with 45 stocks
4. Scrolls down → sees "💰 Real-time Prices"
5. Identifies:
   - 🟢 AAPL up 3.62%
   - 🔴 MSFT down 1.80%
   - 🟢 NVDA up 4.40% (highest)
6. Decides to research NVDA
7. Goes to Stock Search tab
8. Searches "NVDA"
9. Sees large price board: 🔼 $950.00 (+4.40%)
10. Checks metrics and price history
```

### User Story 2: Portfolio Monitoring
```
1. User has portfolio: AAPL, MSFT, GOOG
2. Goes to "Top Winners & Losers" tab
3. Quickly sees which stocks are performing well/poorly
4. For each position:
   - Scrolls to "💰 Live Prices" cards
   - Notes current price and % change
   - Makes decision (hold/sell/buy more)
```

---

## 🎌 Responsive Design Examples

### Desktop (1920px)
```
💰 Real-time Prices
[Card] [Card] [Card] [Card]  ← 4 cards per row
[Card] [Card] [Card]         ← 3 cards per row (if needed)
```

### Tablet (768px)
```
💰 Real-time Prices
[Card] [Card]          ← 2 cards per row
[Card] [Card]
[Card]                 ← 1 card last row
```

### Mobile (480px)
```
💰 Real-time Prices
[Card]                 ← 1 card per row
[Card]
[Card]
[Card]
```

---

## ⚠️ Error Handling Examples

### Scenario 1: No Real-time Data
```
User: Opens Sector Overview
       Expands Technology
Expected: "💰 Real-time Prices" section with cards
Actual: "📡 ยังไม่มีข้อมูล Real-time"

Reason: live_trades collection is empty
Fix: Run realtime_consumer.py to populate data
```

### Scenario 2: MongoDB Connection Error
```
User: Opens Dashboard
Streamlit: Attempts to connect to MongoDB
Error: ConnectionFailure
Display: "⚠️ เกิดข้อผิดพลาดในการดึงข้อมูล AAPL: ..."

Fix: Start MongoDB: mongod
```

### Scenario 3: Stale Cache
```
User: Opens Dashboard at 10:00:00
      Prices loaded: AAPL $150.00 (+3.0%)
User: Waits 10 seconds
      Prices still: AAPL $150.00 (+3.0%)
      (Same because cache not expired)

Reason: @st.cache_data(ttl=30) still valid
Fix: Press F5 to force refresh or wait 30 seconds
```

---

## 🔧 Customization Examples

### Change 1: Update Frequency
```python
# Current: 30 seconds
@st.cache_data(ttl=30)

# Make faster: 10 seconds (more frequent updates)
@st.cache_data(ttl=10)

# Make slower: 60 seconds (less DB load)
@st.cache_data(ttl=60)
```

### Change 2: Card Layout
```python
# Current: 4 columns
st.columns(min(4, len(realtime_prices)))

# Make 2 columns (larger cards)
st.columns(min(2, len(realtime_prices)))

# Make 6 columns (smaller cards)
st.columns(min(6, len(realtime_prices)))
```

### Change 3: Colors
```python
# Current colors
if change_pct > 0:
    color = "#22c55e"  # Green

# Custom colors
if change_pct > 0:
    color = "#0066cc"  # Blue instead

# Threshold-based
if change_pct > 5:
    color = "#ff00ff"  # Pink for big gains
elif change_pct > 0:
    color = "#00ff00"  # Green for small gains
```

---

## 📈 Real Data Example

### Sample Sector with Prices

```
Technology Sector (Stock Count: 5)

Stock Data:
┌─────────────────────────────────────────────────────┐
│ Rank │ Symbol │ Name         │ War Return % Return% │
├─────────────────────────────────────────────────────┤
│  1   │ AAPL   │ Apple Inc.   │ +5.23%       +3.62% │
│  2   │ MSFT   │ Microsoft    │ +3.45%       -1.80% │
│  3   │ GOOG   │ Google       │ +2.10%       +1.48% │
│  4   │ NVDA   │ NVIDIA       │ +8.95%       +4.40% │
│  5   │ AMZN   │ Amazon       │ +1.50%       +0.95% │
└─────────────────────────────────────────────────────┘

Live Prices from MongoDB:
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ AAPL         │  │ MSFT         │  │ GOOG         │
│ 🔼 $150.25   │  │ 🔽 $245.50   │  │ 🔼 $92.75    │
│  +3.62%      │  │  -1.80%      │  │  +1.48%      │
└──────────────┘  └──────────────┘  └──────────────┘

┌──────────────┐  ┌──────────────┐
│ NVDA         │  │ AMZN         │
│ 🔼 $950.00   │  │ 🔼 $165.30   │
│  +4.40%      │  │  +0.95%      │
└──────────────┘  └──────────────┘
```

---

## ✨ Tips for Best Experience

1. **Keep Browser Tab Open**: Price updates every 30s automatically
2. **Refresh if Needed**: F5 for immediate update
3. **Check Timestamps**: Verify data is current (within 30s)
4. **Use Search**: Quickest way to find specific stock
5. **Compare**: Use Winners/Losers tab for quick overview

---

**Created**: 21 เมษายน 2569  
**Version**: Examples v1.0
