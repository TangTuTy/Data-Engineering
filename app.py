import streamlit as st
import pandas as pd
from pymongo import MongoClient
import yfinance as yf
import altair as alt

st.set_page_config(
    page_title="S&P 500 — War Impact Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Minimal CSS for dark mode readability ───────────────────────────────────
st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background: #0e0f12; }
[data-testid="block-container"] { padding-top: 1.5rem; padding-bottom: 3rem; max-width: 1400px; }

/* KPI metric cards */
div[data-testid="stMetric"] {
    background: #1a1d23;
    border: 1px solid #2a2d35;
    border-radius: 10px;
    padding: 1rem 1.1rem;
}
div[data-testid="stMetricValue"] { font-size: 1.5rem; font-weight: 600; }
div[data-testid="stMetricLabel"] { color: #8a8f99 !important; font-size: 0.78rem; }
div[data-testid="stMetricDelta"] { font-size: 0.72rem; }

/* Section headers */
.section-head {
    font-size: 0.72rem; font-weight: 600; letter-spacing: 0.1em;
    text-transform: uppercase; color: #6a6f78;
    margin: 2.25rem 0 0.75rem; padding-bottom: 0.5rem;
    border-bottom: 1px solid #2a2d35;
}

/* Insight box */
.insight-box {
    background: linear-gradient(135deg, #1a1d23 0%, #1f2228 100%);
    border: 1px solid #2a2d35;
    border-left: 3px solid #63c41a;
    border-radius: 10px;
    padding: 1.1rem 1.3rem;
    font-size: 0.95rem; line-height: 1.8; color: #e8e6e0;
}
.insight-box strong { color: #fff; }

.pill {
    display: inline-block; font-size: 0.78rem; font-weight: 600;
    padding: 3px 10px; border-radius: 99px; margin: 0 3px;
}
.pill-up   { background: rgba(99, 196, 26, 0.18); color: #a8d87e; border: 1px solid rgba(99, 196, 26, 0.35); }
.pill-down { background: rgba(226, 75, 75, 0.18); color: #e89a9a; border: 1px solid rgba(226, 75, 75, 0.35); }

/* Sector card */
.sector-card {
    background: #1a1d23;
    border: 1px solid #2a2d35;
    border-radius: 10px;
    padding: 0.95rem 1.1rem;
    margin-bottom: 10px;
    transition: border-color 0.15s;
}
.sector-card:hover { border-color: #3a3d45; }
.sc-name  { font-size: 0.82rem; color: #8a8f99; margin: 0 0 4px 0; font-weight: 500; }
.sc-shift { font-size: 1.5rem; font-weight: 700; margin: 0; line-height: 1.2; }
.sc-meta  { font-size: 0.72rem; color: #6a6f78; margin: 4px 0 0 0; }
.bar-bg   { height: 4px; background: #2a2d35; border-radius: 2px; margin-top: 10px; overflow: hidden; }
.bar-fill { height: 4px; border-radius: 2px; transition: width 0.4s; }

/* Stock row */
.stock-row {
    display: flex; align-items: center; gap: 10px;
    padding: 9px 12px; border-radius: 6px;
    font-size: 0.85rem;
    margin-bottom: 2px;
    transition: background 0.1s;
}
.stock-row:hover { background: #1a1d23; }
.sr-rank  { width: 22px; color: #5a5f68; font-size: 0.72rem; font-variant-numeric: tabular-nums; flex-shrink: 0; }
.sr-sym   { width: 48px; font-weight: 600; color: #e8e6e0; flex-shrink: 0; font-family: ui-monospace, monospace; }
.sr-name  { flex: 1; color: #8a8f99; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.sr-sec   { width: 100px; color: #6a6f78; font-size: 0.72rem; flex-shrink: 0; text-align: right; }
.sr-shift { width: 64px; font-weight: 600; text-align: right; flex-shrink: 0; font-variant-numeric: tabular-nums; }
.sr-price { width: 76px; color: #c8cdd8; text-align: right; flex-shrink: 0; font-variant-numeric: tabular-nums; font-size: 0.82rem; }
.sr-today { width: 64px; font-weight: 600; text-align: right; flex-shrink: 0; font-variant-numeric: tabular-nums; font-size: 0.8rem; }

/* Ticker card grid */
.ticker-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 8px;
    margin-top: 0.5rem;
}
.ticker-card {
    background: #1a1d23;
    border: 1px solid #2a2d35;
    border-radius: 8px;
    padding: 0.65rem 0.8rem;
    display: flex; flex-direction: column; gap: 3px;
}
.ticker-card:hover { border-color: #3a3d45; }
.tc-sym   { font-size: 0.82rem; font-weight: 700; color: #e8e6e0; font-family: ui-monospace, monospace; }
.tc-price { font-size: 1.05rem; font-weight: 600; color: #c8cdd8; font-variant-numeric: tabular-nums; }
.tc-pct   { font-size: 0.75rem; font-weight: 600; font-variant-numeric: tabular-nums; }

/* Live badge */
.live-badge {
    display: inline-block; font-size: 0.68rem; font-weight: 700;
    background: #e24b4b; color: #fff;
    border-radius: 4px; padding: 2px 7px; margin-left: 8px; vertical-align: middle;
    letter-spacing: 0.05em;
}
.live-dot {
    display: inline-block; width: 6px; height: 6px; border-radius: 50%;
    background: #e24b4b; margin-right: 5px;
    animation: pulse 1.5s infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.3; }
}

/* Alert row */
.alert-row {
    background: rgba(226, 75, 75, 0.08);
    border: 1px solid rgba(226, 75, 75, 0.25);
    border-radius: 8px;
    padding: 0.7rem 1rem; font-size: 0.85rem; margin-bottom: 6px;
    display: flex; align-items: center; gap: 10px;
}
.ar-sym  { font-weight: 600; width: 48px; flex-shrink: 0; color: #e8e6e0; font-family: ui-monospace, monospace; }
.ar-desc { flex: 1; color: #a0a5ae; font-size: 0.8rem; }
.ar-val  { font-weight: 700; color: #e89a9a; flex-shrink: 0; font-variant-numeric: tabular-nums; }

.war-note {
    font-size: 0.72rem; color: #6a6f78;
    border-left: 2px solid #2a2d35; padding-left: 10px; margin-top: 0.6rem;
}

h1, h2, h3, h4 { color: #e8e6e0 !important; }
</style>
""", unsafe_allow_html=True)


# ── DB helpers ───────────────────────────────────────────────────────────────
@st.cache_resource
def get_db():
    client = MongoClient("mongodb://mongodb:27017/")
    return client["stock_database"]

@st.cache_data(ttl=60)
def load_col(name):
    return list(db[name].find({}, {"_id": 0}))

@st.cache_data(ttl=60)
def load_weekly_from_fact():
    """Aggregate weekly sector performance จาก fact_war_analytics"""
    pipeline = [
        {"$match": {"daily_return_pct": {"$ne": None}}},
        {"$group": {
            "_id": {"sector": "$sector", "week": "$week_key"},
            "avg_daily_return_pct": {"$avg": "$daily_return_pct"},
            "avg_close": {"$avg": "$close"},
            "data_points": {"$sum": 1},
            "first_period": {"$first": "$period"},
        }},
        {"$project": {
            "_id": 0,
            "sector": "$_id.sector",
            "week": "$_id.week",
            "avg_daily_return_pct": {"$round": ["$avg_daily_return_pct", 4]},
            "avg_close": {"$round": ["$avg_close", 2]},
            "data_points": 1,
            "period": "$first_period",
        }},
        {"$sort": {"sector": 1, "week": 1}},
    ]
    return list(db["fact_war_analytics"].aggregate(pipeline))

@st.cache_data(ttl=60)
def load_timeline_from_fact():
    """Aggregate daily market timeline จาก fact_war_analytics"""
    pipeline = [
        {"$match": {"daily_return_pct": {"$ne": None}}},
        {"$group": {
            "_id": "$date",
            "market_avg_return": {"$avg": "$daily_return_pct"},
            "period": {"$first": "$period"},
        }},
        {"$project": {
            "_id": 0,
            "date": "$_id",
            "market_avg_return": {"$round": ["$market_avg_return", 4]},
            "period": 1,
        }},
        {"$sort": {"date": 1}},
    ]
    return list(db["fact_war_analytics"].aggregate(pipeline))

@st.cache_data(ttl=30)
def fetch_live_prices(symbols):
    out = {}
    for sym in symbols:
        doc = db["live_trades"].find_one({"symbol": sym}, {"_id": 0}, sort=[("timestamp", -1)])
        if doc:
            out[sym] = {"price": doc.get("price", 0), "volume": doc.get("volume")}
    return out

@st.cache_data(ttl=15)
def fetch_alerts(limit=20):
    return list(
        db["live_trades"]
        .find({"status": {"$ne": "normal"}},
              {"_id": 0, "symbol": 1, "price": 1, "live_war_return_pct": 1,
               "status": 1, "sector": 1, "timestamp": 1})
        .sort("timestamp", -1).limit(limit)
    )

@st.cache_data(ttl=300)
def fetch_finnhub_symbols():
    """ดึง distinct symbols ที่ Finnhub producer กำลัง track อยู่จาก live_trades"""
    docs = db["live_trades"].distinct("symbol")
    return set(docs) if docs else set()

@st.cache_data(ttl=15)
def fetch_recent_trades(limit=10):
    return list(
        db["live_trades"]
        .find({}, {"_id": 0, "symbol": 1, "price": 1, "live_war_return_pct": 1, "timestamp": 1})
        .sort("timestamp", -1).limit(limit)
    )

@st.cache_data(ttl=15)
def fetch_latest_per_symbol():
    """ดึงราคาล่าสุด 1 record ต่อ symbol — แสดงทุกตัวใน watchlist"""
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$symbol",
            "price": {"$first": "$price"},
            "live_war_return_pct": {"$first": "$live_war_return_pct"},
            "timestamp": {"$first": "$timestamp"},
        }},
        {"$sort": {"_id": 1}},
    ]
    return list(db["live_trades"].aggregate(pipeline))

@st.cache_data(ttl=3600)
def fetch_latest_close_yf(symbols_tuple):
    """ดึงราคาปิดล่าสุดจาก yfinance — ใช้เมื่อตลาดปิดและไม่มี live price"""
    try:
        df = yf.download(list(symbols_tuple), period="5d", interval="1d",
                         auto_adjust=True, progress=False)
        if df.empty:
            return {}
        close = df["Close"] if "Close" in df.columns else df.xs("Close", axis=1, level=0)
        result = {}
        for sym in symbols_tuple:
            if sym in close.columns:
                s = close[sym].dropna()
                if not s.empty:
                    result[sym] = float(s.iloc[-1])
        return result
    except Exception:
        return {}

@st.cache_data(ttl=60)
def fetch_intraday_price_yf(symbols_tuple):
    """ดึงราคาล่าสุด intraday (1m) จาก yfinance — ใช้ตอนตลาดเปิด, cache 60 วิ"""
    try:
        df = yf.download(list(symbols_tuple), period="1d", interval="1m",
                         auto_adjust=True, progress=False)
        if df.empty:
            return {}
        close = df["Close"] if "Close" in df.columns else df.xs("Close", axis=1, level=0)
        result = {}
        for sym in symbols_tuple:
            if sym in close.columns:
                s = close[sym].dropna()
                if not s.empty:
                    result[sym] = float(s.iloc[-1])
        return result
    except Exception:
        return {}

@st.cache_data(ttl=3600)
def fetch_daily_change_yf(symbols_tuple):
    """ดึง % เปลี่ยนแปลงวันนี้ = (close วันล่าสุด / close วันก่อน - 1) * 100"""
    try:
        df = yf.download(list(symbols_tuple), period="5d", interval="1d",
                         auto_adjust=True, progress=False)
        if df.empty:
            return {}
        close = df["Close"] if "Close" in df.columns else df.xs("Close", axis=1, level=0)
        result = {}
        for sym in symbols_tuple:
            if sym in close.columns:
                s = close[sym].dropna()
                if len(s) >= 2:
                    result[sym] = float((s.iloc[-1] / s.iloc[-2] - 1) * 100)
        return result
    except Exception:
        return {}


# ── Insight generator ────────────────────────────────────────────────────────
def build_insight(df_sec, df_stk):
    if df_sec.empty:
        return "ยังไม่มีข้อมูลเพียงพอสำหรับสรุป insight"

    pos = df_sec[df_sec["median_performance_shift"] > 0].sort_values("median_performance_shift", ascending=False)
    neg = df_sec[df_sec["median_performance_shift"] < 0].sort_values("median_performance_shift")

    pos_pills = " ".join(
        f'<span class="pill pill-up">{r["sector"]} {r["median_performance_shift"]:+.1f}%</span>'
        for _, r in pos.head(2).iterrows()
    )
    neg_pills = " ".join(
        f'<span class="pill pill-down">{r["sector"]} {r["median_performance_shift"]:+.1f}%</span>'
        for _, r in neg.head(2).iterrows()
    )

    # Top 3 winners — กรองหุ้นใหม่ออก (pre_war_days < 100 = เข้าตลาดช้าเกินไป เทียบไม่ยุติธรรม)
    top3 = ""
    if not df_stk.empty and "performance_shift" in df_stk.columns:
        df_fair = df_stk.copy()
        if "pre_war_days" in df_fair.columns:
            df_fair = df_fair[df_fair["pre_war_days"].fillna(0) >= 100]

        top_df = df_fair.sort_values("performance_shift", ascending=False).head(3)

        # format: "Exxon Mobil (XOM)"
        top_items = []
        for _, r in top_df.iterrows():
            sym  = r.get("symbol", "-")
            name = r.get("full_name", "") or sym
            # ตัดชื่อยาวๆ เพื่อความกระชับ
            if len(name) > 25:
                name = name[:25].rstrip() + "…"
            top_items.append(f"<strong>{name} ({sym})</strong>")

        if top_items:
            top3 = f" — หุ้นที่ได้ประโยชน์สูงสุดคือ {', '.join(top_items)}"

    neu_count = len(df_sec[df_sec["median_performance_shift"].abs() < 0.05])
    neu_note  = f" อีก {neu_count} sector เคลื่อนไหวเล็กน้อย" if neu_count else ""

    parts = []
    if pos_pills:
        parts.append(f"{pos_pills} ได้รับผลบวก")
    if neg_pills:
        parts.append(f"{neg_pills} ได้รับผลลบ")

    body = " ขณะที่ ".join(parts) or "ตลาดเคลื่อนไหวในภาพรวมปกติ"
    return f"ช่วงสงคราม US-Iran: {body}{neu_note}{top3}"


# ── Load data ────────────────────────────────────────────────────────────────
db = get_db()

raw_sectors  = load_col("dim_sector")          # ← replaces gold_sector_war_summary
raw_stocks   = load_col("dim_company")         # ← replaces gold_stock_ranking
raw_weekly   = load_weekly_from_fact()          # ← aggregation from fact_war_analytics
raw_timeline = load_timeline_from_fact()        # ← aggregation from fact_war_analytics

if not raw_sectors:
    st.warning("⏳ Dimension tables ยังไม่มีข้อมูล — กรุณา trigger pipeline ก่อน")
    st.stop()

df_sec = pd.DataFrame(raw_sectors).sort_values("median_performance_shift", ascending=False)
df_stk = pd.DataFrame(raw_stocks)   if raw_stocks   else pd.DataFrame()
df_wk  = pd.DataFrame(raw_weekly)   if raw_weekly   else pd.DataFrame()
df_tl  = pd.DataFrame(raw_timeline) if raw_timeline else pd.DataFrame()

# dim_company มี pre_war_days อยู่แล้ว (ไม่ต้อง merge จาก silver แยกอีกต่อไป)

# live price overlay
live_syms = df_stk["symbol"].tolist() if not df_stk.empty else []
live_px   = fetch_live_prices(live_syms)

# yfinance fallback price (ราคาปิดล่าสุด — ใช้เมื่อตลาดปิดหรือ Finnhub ไม่มีข้อมูล)
yf_close_px: dict = {}
yf_intraday_px: dict = {}
yf_daily_chg: dict = {}
if not df_stk.empty:
    yf_close_px    = fetch_latest_close_yf(tuple(live_syms))
    yf_intraday_px = fetch_intraday_price_yf(tuple(live_syms))
    yf_daily_chg   = fetch_daily_change_yf(tuple(live_syms))

def get_display_price(sym, row):
    """คืนราคาล่าสุดที่มีอยู่: live(Finnhub) > intraday(yf 1m) > war_latest_close > daily close"""
    lp = live_px.get(sym, {})
    pv = lp.get("price") if lp else None
    if pv is None or (isinstance(pv, float) and pd.isna(pv)):
        pv = yf_intraday_px.get(sym)
    if pv is None or (isinstance(pv, float) and pd.isna(pv)):
        pv = row.get("war_latest_close") if hasattr(row, "get") else None
    if pv is None or (isinstance(pv, float) and pd.isna(pv)):
        pv = yf_close_px.get(sym)
    return pv

# KPI values
total_stocks = len(df_stk)
n_sectors    = len(df_sec)
n_pos        = len(df_sec[df_sec["median_performance_shift"] > 0.05])
n_neg        = len(df_sec[df_sec["median_performance_shift"] < -0.05])
best_sec     = df_sec.iloc[0]["sector"]  if not df_sec.empty else "-"
worst_sec    = df_sec.iloc[-1]["sector"] if not df_sec.empty else "-"
best_shift   = df_sec.iloc[0]["median_performance_shift"]  if not df_sec.empty else 0
worst_shift  = df_sec.iloc[-1]["median_performance_shift"] if not df_sec.empty else 0

# alert banner
active_alerts = fetch_alerts()
if active_alerts:
    crit = [a for a in active_alerts if a.get("status") in ("alert_extreme_up", "alert_extreme_down")]
    warn = [a for a in active_alerts if a.get("status") in ("alert_spike", "alert_crash", "alert_war_sensitive")]
    if crit:
        syms = ", ".join(set(a["symbol"] for a in crit[:4]))
        st.error(f"🚨 Critical alert: **{syms}** — เคลื่อนไหวเกิน ±10% vs baseline", icon="🚨")
    elif warn:
        syms = ", ".join(set(a["symbol"] for a in warn[:4]))
        st.warning(f"⚠️ Price alert: **{syms}** — เคลื่อนไหวเกิน ±5% vs baseline", icon="⚠️")


# ════════════════════════════════════════════════════════════════════════════
# HEADER
# ════════════════════════════════════════════════════════════════════════════
st.markdown("## 📊 S&P 500 — War Impact Dashboard")
st.caption("Batch: S&P 500 ทั้งหมด (yfinance + Airflow)  ·  Realtime: Finnhub WebSocket  ·  Star Schema Architecture")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 1 — INSIGHT
# ════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="section-head">💡 สรุป insight</p>', unsafe_allow_html=True)
st.markdown(
    f'<div class="insight-box">{build_insight(df_sec, df_stk)}</div>',
    unsafe_allow_html=True,
)


# ════════════════════════════════════════════════════════════════════════════
# SECTION 2 — KPI BAR
# ════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="section-head">📈 ภาพรวมตลาด</p>', unsafe_allow_html=True)
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("หุ้นที่วิเคราะห์",  f"{total_stocks:,}", "S&P 500 ทั้งหมด")
k2.metric("Sector บวก",  n_pos,  f"จาก {n_sectors} sector")
k3.metric("Sector ลบ",   n_neg,  f"จาก {n_sectors} sector")
k4.metric("Sector ดีสุด",  best_sec,  f"{best_shift:+.2f}%")
k5.metric("Sector แย่สุด", worst_sec, f"{worst_shift:+.2f}%")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 3 — WAR TIMELINE
# ════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="section-head">🗓️ timeline ตลาด — ก่อนและหลังสงคราม</p>', unsafe_allow_html=True)

if not df_tl.empty:
    df_tl["date"] = pd.to_datetime(df_tl["date"], errors="coerce")
    df_tl = df_tl.sort_values("date").reset_index(drop=True)
    WAR_START = pd.Timestamp("2026-01-01")

    # แยกก่อน/หลังสงคราม
    pre_df = df_tl[df_tl["date"] < WAR_START].copy()
    war_df = df_tl[df_tl["date"] >= WAR_START].copy()

    # ── เชื่อมกราฟ: เอาจุดสุดท้ายของ pre_war ไปใส่ใน war line ด้วย ──
    # ทำให้เส้นแดงเริ่มจากจุดเดียวกับจุดสุดท้ายของเส้นเขียว → ดูต่อเนื่อง
    if not pre_df.empty and not war_df.empty:
        bridge_row = pre_df.iloc[[-1]].copy()  # จุดสุดท้ายของ pre_war
        war_df = pd.concat([bridge_row, war_df], ignore_index=True)

    pre_tl = pre_df[["date", "market_avg_return"]].copy()
    pre_tl["ช่วงเวลา"] = "ก่อนสงคราม"
    war_tl = war_df[["date", "market_avg_return"]].copy()
    war_tl["ช่วงเวลา"] = "ช่วงสงคราม"
    melted_tl = pd.concat([pre_tl, war_tl], ignore_index=True).dropna(subset=["market_avg_return"])

    tl_chart = (
        alt.Chart(melted_tl)
        .mark_line(strokeWidth=2, interpolate="monotone")
        .encode(
            x=alt.X("date:T", axis=alt.Axis(format="%b %Y", labelAngle=-45, title=None, tickCount="month")),
            y=alt.Y("market_avg_return:Q", title="Avg Daily Return (%)"),
            color=alt.Color("ช่วงเวลา:N", scale=alt.Scale(
                domain=["ก่อนสงคราม", "ช่วงสงคราม"], range=["#63c41a", "#e24b4b"]
            ), legend=alt.Legend(orient="top", title=None)),
        )
        .properties(height=280)
    )
    st.altair_chart(tl_chart, use_container_width=True)
    st.markdown(
        '<p class="war-note">🟢 ก่อนสงคราม &nbsp;·&nbsp; 🔴 ช่วงสงคราม (เริ่ม 1 ม.ค. 2026)</p>',
        unsafe_allow_html=True,
    )
else:
    st.info("ไม่มีข้อมูล timeline")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 4 — SECTOR CARDS
# ════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="section-head">🏭 ผลกระทบต่อ sector — เรียงจากบวกมากสุด</p>', unsafe_allow_html=True)

if "drill_sector" not in st.session_state:
    st.session_state["drill_sector"] = None

max_abs  = df_sec["median_performance_shift"].abs().max() or 1
cols_sec = st.columns(3)

for i, (_, row) in enumerate(df_sec.iterrows()):
    shift    = row.get("median_performance_shift", 0)
    sector   = row.get("sector", "Unknown")
    n_stocks = row.get("stock_count", 0)
    label    = row.get("war_impact_label", "neutral")

    if shift > 0.05:
        color, bar_color = "#8cd864", "#63c41a"
    elif shift < -0.05:
        color, bar_color = "#ff7a7a", "#e24b4b"
    else:
        color, bar_color = "#a0a5ae", "#5a5f68"

    bar_pct   = min(abs(shift) / max_abs * 100, 100)
    is_sel    = st.session_state["drill_sector"] == sector
    card_border = "border-color:#63c41a;" if is_sel else ""

    with cols_sec[i % 3]:
        st.markdown(
            f'<div class="sector-card" style="{card_border}">'
            f'<p class="sc-name">{sector}</p>'
            f'<p class="sc-shift" style="color:{color};">{shift:+.2f}%</p>'
            f'<p class="sc-meta">{n_stocks} หุ้น · {label}</p>'
            f'<div class="bar-bg"><div class="bar-fill" style="width:{bar_pct:.0f}%;background:{bar_color};"></div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        btn_label = "✕ ปิด" if is_sel else "Stock list sector"
        if st.button(btn_label, key=f"sec_btn_{i}", use_container_width=True):
            st.session_state["drill_sector"] = None if is_sel else sector
            st.rerun()

# ── Sector Drill-down ────────────────────────────────────────────────────────
if st.session_state.get("drill_sector") and not df_stk.empty:
    sel_sec    = st.session_state["drill_sector"]
    sec_stocks = df_stk[df_stk["sector"] == sel_sec].sort_values("performance_shift", ascending=False)
    st.markdown(f'<p class="section-head">🔎 หุ้นทั้งหมดใน {sel_sec}</p>', unsafe_allow_html=True)
    if sec_stocks.empty:
        st.info("ไม่พบหุ้นใน sector นี้")
    else:
        for rank, (_, r) in enumerate(sec_stocks.iterrows(), 1):
            shift = r.get("performance_shift", 0)
            sym   = r.get("symbol", "-")
            pv    = get_display_price(sym, r)
            price_str = f'${pv:.2f}' if pv and not pd.isna(pv) else '—'
            color = "#8cd864" if shift > 0 else "#ff7a7a" if shift < 0 else "#a0a5ae"
            st.markdown(
                f'<div class="stock-row">'
                f'<span class="sr-rank">{rank}</span>'
                f'<span class="sr-sym">{sym}</span>'
                f'<span class="sr-name">{r.get("full_name","-")}</span>'
                f'<span class="sr-price">{price_str}</span>'
                f'<span class="sr-shift" style="color:{color};">{shift:+.2f}%</span>'
                f'</div>',
                unsafe_allow_html=True,
            )


# ════════════════════════════════════════════════════════════════════════════
# SECTION 5 — TOP WINNERS / LOSERS
# ════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="section-head">🏆 หุ้นที่ได้ประโยชน์ vs เสียประโยชน์สูงสุด</p>', unsafe_allow_html=True)

if not df_stk.empty and "performance_shift" in df_stk.columns:
    col_w, col_l = st.columns(2)

    # กรองเฉพาะ Finnhub watchlist (~50 ตัว) เพื่อให้ได้ราคา realtime
    finnhub_syms = fetch_finnhub_symbols()
    if finnhub_syms:
        df_fin = df_stk[df_stk["symbol"].isin(finnhub_syms)]
        watchlist_note = f"จาก Finnhub watchlist {len(finnhub_syms)} ตัว"
    else:
        df_fin = df_stk
        watchlist_note = "S&P 500 ทั้งหมด (ยังไม่มี live trades)"

    top_win = df_fin.sort_values("performance_shift", ascending=False).head(10)
    top_los = df_fin.sort_values("performance_shift", ascending=True).head(10)

    with col_w:
        st.markdown(f"##### 🟢 Winners — บวกสูงสุด 10 ตัว")
        #st.caption(f"ราคา · Δ วันนี้ · War Shift &nbsp;·&nbsp; {watchlist_note}")
        for rank, (_, r) in enumerate(top_win.iterrows(), 1):
            shift = r.get("performance_shift", 0)
            sym = r.get("symbol", "-")
            price_val = get_display_price(sym, r)
            price_str = f'${price_val:.2f}' if price_val and not pd.isna(price_val) else '—'
            chg = yf_daily_chg.get(sym)
            chg_str = f'{chg:+.2f}%' if chg is not None else '—'
            chg_color = "#8cd864" if chg and chg > 0 else "#ff7a7a" if chg and chg < 0 else "#a0a5ae"
            st.markdown(
                f'<div class="stock-row">'
                f'<span class="sr-rank">{rank}</span>'
                f'<span class="sr-sym">{sym}</span>'
                f'<span class="sr-name">{r.get("full_name","-")}</span>'
                f'<span class="sr-price">{price_str}</span>'
                f'<span class="sr-today" style="color:{chg_color};">{chg_str}</span>'
                f'<span class="sr-shift" style="color:#8cd864;" title="War Performance Shift">{shift:+.2f}%↑</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

    with col_l:
        st.markdown("##### 🔴 Losers — ลบสูงสุด 10 ตัว")
        #st.caption(f"ราคา · Δ วันนี้ · War Shift &nbsp;·&nbsp; {watchlist_note}")
        for rank, (_, r) in enumerate(top_los.iterrows(), 1):
            shift = r.get("performance_shift", 0)
            sym = r.get("symbol", "-")
            price_val = get_display_price(sym, r)
            price_str = f'${price_val:.2f}' if price_val and not pd.isna(price_val) else '—'
            chg = yf_daily_chg.get(sym)
            chg_str = f'{chg:+.2f}%' if chg is not None else '—'
            chg_color = "#8cd864" if chg and chg > 0 else "#ff7a7a" if chg and chg < 0 else "#a0a5ae"
            st.markdown(
                f'<div class="stock-row">'
                f'<span class="sr-rank">{rank}</span>'
                f'<span class="sr-sym">{sym}</span>'
                f'<span class="sr-name">{r.get("full_name","-")}</span>'
                f'<span class="sr-price">{price_str}</span>'
                f'<span class="sr-today" style="color:{chg_color};">{chg_str}</span>'
                f'<span class="sr-shift" style="color:#ff7a7a;" title="War Performance Shift">{shift:+.2f}%↓</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
else:
    st.info("ไม่มีข้อมูล stock ranking")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 6 — WEEKLY SECTOR TREND
# ════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="section-head">📉 weekly sector trend</p>', unsafe_allow_html=True)

if not df_wk.empty:
    sectors_avail = sorted(df_wk["sector"].dropna().unique().tolist())
    selected_secs = st.multiselect(
        "เลือก sector ที่ต้องการดู",
        sectors_avail,
        default=sectors_avail[:5],
        key="weekly_sel",
    )
    if selected_secs:
        pivot = (
            df_wk[df_wk["sector"].isin(selected_secs)]
            .pivot_table(index="week", columns="sector", values="avg_daily_return_pct", aggfunc="first")
            .sort_index()
        )
        def _fmt_week(w):
            try:
                return pd.to_datetime(f"{w}-1", format="%Y-W%W-%w").strftime("%-d %b %Y")
            except Exception:
                return str(w)
        # แปลง week key → datetime จริง เพื่อให้ Streamlit เรียง x-axis ตามเวลา
        sorted_weeks = sorted(pivot.index.tolist())
        pivot = pivot.loc[sorted_weeks]
        date_index = []
        valid_rows = []
        for i, w in enumerate(sorted_weeks):
            try:
                date_index.append(pd.to_datetime(f"{w}-1", format="%Y-W%W-%w"))
                valid_rows.append(i)
            except Exception:
                continue
        pivot = pivot.iloc[valid_rows]
        pivot.index = date_index
        pivot = pivot.apply(pd.to_numeric, errors="coerce")
        pivot = pivot.dropna(how="all")
        if not pivot.empty:
            wk_melted = pivot.reset_index().rename(columns={pivot.index.name or "index": "date"})
            wk_melted = wk_melted.melt(id_vars="date", var_name="Sector", value_name="avg_return")
            wk_melted = wk_melted.dropna(subset=["avg_return"])
            wk_chart = (
                alt.Chart(wk_melted)
                .mark_line(strokeWidth=2, interpolate="monotone")
                .encode(
                    x=alt.X("date:T", axis=alt.Axis(format="%b %Y", labelAngle=-45, title=None, tickCount="month")),
                    y=alt.Y("avg_return:Q", title="Avg Daily Return (%)"),
                    color=alt.Color("Sector:N"),
                )
                .properties(height=320)
            )
            st.altair_chart(wk_chart, use_container_width=True)
        else:
            st.info("ไม่มีข้อมูล weekly performance")
    else:
        st.info("เลือก sector อย่างน้อย 1 กลุ่ม")
else:
    st.info("ไม่มีข้อมูล weekly performance")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 7 — STOCK SEARCH
# ════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="section-head">🔍 ค้นหาหุ้นรายตัว</p>', unsafe_allow_html=True)

search = st.text_input(
    "พิมพ์ symbol เช่น AAPL, XOM, LMT",
    placeholder="AAPL",
    key="stock_search",
).upper().strip()

if search and not df_stk.empty:
    match = df_stk[df_stk["symbol"] == search]
    if not match.empty:
        s       = match.iloc[0]
        shift   = s.get("performance_shift", 0)
        pre_cum = s.get("pre_war_cumulative_return_pct", None)
        war_cum = s.get("war_cumulative_return_pct", None)
        impact  = s.get("war_impact", "neutral")
        lp      = live_px.get(search)

        impact_emoji = {"positive": "🟢", "negative": "🔴", "neutral": "🟡"}.get(
            impact.split("_")[-1] if "_" in impact else impact, "⚪")

        st.markdown(f"#### {impact_emoji} {s['symbol']} — {s.get('full_name', '')}")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Live Price",      f"${lp['price']:.2f}" if lp else "-")
        c2.metric("War Impact",      impact)
        c3.metric("Pre-War Return",  f"{pre_cum:+.2f}%" if pre_cum is not None else "-")
        c4.metric("War Return",      f"{war_cum:+.2f}%"  if war_cum  is not None else "-")

        c5, c6, c7 = st.columns([1, 1, 2])
        c5.metric("Performance Shift", f"{shift:+.2f}%")
        c6.metric("Sector",            s.get("sector", "-"))
        c7.metric("Industry",          s.get("industry", "-"))

        hist = list(
            db["fact_war_analytics"]
            .find({"symbol": search}, {"_id": 0, "date": 1, "close": 1, "period": 1})
            .sort("date", 1)
        )
        if hist:
            hdf = pd.DataFrame(hist)
            hdf["date"] = pd.to_datetime(hdf["date"], errors="coerce")
            hdf = hdf.dropna(subset=["date"]).sort_values("date")
            hdf["close"] = pd.to_numeric(hdf["close"], errors="coerce")
            hdf = hdf.dropna(subset=["close"])

            if not hdf.empty:
                pre_h = hdf[hdf["period"] == "pre_war"].copy()
                war_h = hdf[hdf["period"] == "war"].copy()

                # ── bridge row: เชื่อมจุดสุดท้ายของ pre_war กับเส้น war ──
                if not pre_h.empty and not war_h.empty:
                    bridge = pre_h.iloc[[-1]].copy()
                    war_h = pd.concat([bridge, war_h], ignore_index=True)

                pre_line = pre_h[["date", "close"]].rename(columns={"close": "ก่อนสงคราม"})
                war_line = war_h[["date", "close"]].rename(columns={"close": "ช่วงสงคราม"})
                chart_h = pd.merge(pre_line, war_line, on="date", how="outer").set_index("date").sort_index()
                chart_h = chart_h.apply(pd.to_numeric, errors="coerce")

                st.markdown("**Price History**")
                if not chart_h.dropna(how="all").empty:
                    hist_melted = chart_h.reset_index().rename(columns={"index": "date"}) if chart_h.index.name is None else chart_h.reset_index()
                    hist_melted = hist_melted.melt(id_vars="date", var_name="ช่วงเวลา", value_name="close")
                    hist_melted = hist_melted.dropna(subset=["close"])
                    hist_chart = (
                        alt.Chart(hist_melted)
                        .mark_line(strokeWidth=2, interpolate="monotone")
                        .encode(
                            x=alt.X("date:T", axis=alt.Axis(format="%b %Y", labelAngle=-45, title=None, tickCount="month")),
                            y=alt.Y("close:Q", title="Price ($)"),
                            color=alt.Color("ช่วงเวลา:N", scale=alt.Scale(
                                domain=["ก่อนสงคราม", "ช่วงสงคราม"], range=["#63c41a", "#e24b4b"]
                            ), legend=alt.Legend(orient="top", title=None)),
                        )
                        .properties(height=280)
                    )
                    st.altair_chart(hist_chart, use_container_width=True)
                else:
                    st.info("ไม่มีข้อมูลราคาย้อนหลัง")
    elif search:
        st.warning(f"ไม่พบ symbol: {search}")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 8 — LIVE WATCHLIST + ALERTS
# ════════════════════════════════════════════════════════════════════════════
st.markdown('<p class="section-head">⚡ live watchlist & alerts</p>', unsafe_allow_html=True)
st.markdown(
    '<span class="live-dot"></span><strong style="color:#e24b4b;">LIVE</strong> '
    '<span style="color:#8a8f99; font-size:0.82rem;">— Finnhub WebSocket Realtime</span>',
    unsafe_allow_html=True,
)

col_live, col_alert = st.columns(2)

ALERT_ICON = {
    "alert_extreme_up":    "🔥",
    "alert_spike":         "⬆️",
    "alert_extreme_down":  "💥",
    "alert_crash":         "⬇️",
    "alert_war_sensitive": "⚔️",
    "alert_volume_spike":  "📊",
}

with col_live:
    st.markdown("##### ราคาล่าสุด — Live")
    watchlist_prices = fetch_latest_per_symbol()
    if watchlist_prices:
        MAX_LIVE = 50  # FIFO: แสดงแค่ 50 ตัวล่าสุด
        capped = watchlist_prices[:MAX_LIVE]

        if "live_show_all" not in st.session_state:
            st.session_state["live_show_all"] = False

        INITIAL_SHOW = 8  # 2 rows × 4 cols = 8 cards — เท่ากับ alerts ฝั่งขวา
        show_all = st.session_state["live_show_all"]
        items_to_show = capped if show_all else capped[:INITIAL_SHOW]

        cards_html = '<div class="ticker-grid">'
        for t in items_to_show:
            sym   = t.get("_id", "-")
            price = t.get("price", 0) or 0
            imp   = t.get("live_war_return_pct") or 0
            color = "#8cd864" if imp > 0 else "#ff7a7a" if imp < 0 else "#6a6f78"
            pct_str = f'{imp:+.2f}%'
            cards_html += (
                f'<div class="ticker-card">'
                f'<span class="tc-sym">{sym}</span>'
                f'<span class="tc-price">${price:.2f}</span>'
                f'<span class="tc-pct" style="color:{color};">{pct_str}</span>'
                f'</div>'
            )
        cards_html += '</div>'
        st.markdown(cards_html, unsafe_allow_html=True)

        if len(capped) > INITIAL_SHOW:
            if not show_all:
                if st.button(f"⬇️ ดูทั้งหมด ({len(capped)} ตัว)", key="live_show_more", use_container_width=True):
                    st.session_state["live_show_all"] = True
                    st.rerun()
            else:
                if st.button("⬆️ ย่อกลับ", key="live_show_less", use_container_width=True):
                    st.session_state["live_show_all"] = False
                    st.rerun()
    else:
        st.info("ยังไม่มี live trades")

with col_alert:
    st.markdown("##### alerts ล่าสุด")
    if active_alerts:
        for a in active_alerts[:8]:
            status = a.get("status", "normal")
            icon   = ALERT_ICON.get(status, "❓")
            imp    = a.get("live_war_return_pct", 0)
            st.markdown(
                f'<div class="alert-row">'
                f'<span style="font-size:1.05rem;">{icon}</span>'
                f'<span class="ar-sym">{a.get("symbol","-")}</span>'
                f'<span class="ar-desc">{a.get("sector","-")} · {status.replace("alert_","")}</span>'
                f'<span class="ar-val">{imp:+.2f}%</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
    else:
        st.success("✅ ไม่มี alert ในขณะนี้")


# ════════════════════════════════════════════════════════════════════════════
# FOOTER
# ════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.caption(
    f"S&P 500 · {total_stocks:,} stocks · {n_sectors} sectors · "
    "Star Schema: fact_war_analytics + dim tables · Apache Airflow + MongoDB · "
    "Realtime: Finnhub WebSocket via Kafka"
)