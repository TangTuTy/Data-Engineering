import streamlit as st
import pandas as pd
from pymongo import MongoClient
import datetime as _dt

st.set_page_config(page_title="S&P 500 — US-Iran War Impact Dashboard", layout="wide")

IMPACT_COLORS = {
    "strong_positive": "#22c55e",
    "positive": "#86efac",
    "neutral": "#fbbf24",
    "negative": "#f87171",
    "strong_negative": "#dc2626",
}

IMPACT_EMOJI = {
    "strong_positive": "🟢",
    "positive": "🟡",
    "neutral": "⚪",
    "negative": "🟠",
    "strong_negative": "🔴",
}


@st.cache_resource
def get_db():
    client = MongoClient("mongodb://localhost:27017/") 
    return client["stock_database"]

db = get_db()

# ==========================================
# 🌟 ฟังก์ชันดึงข้อมูล Live Alerts
# ==========================================
def fetch_live_alerts():
    alerts = list(db["gold_live_war_monitor"].find(
        {"status": {"$ne": "normal"}}, 
        {"_id": 0}
    ).sort("live_war_return_pct", -1))
    return alerts

# ==========================================
# 🌟 ฟังก์ชันดึงราคา Real-time
# ==========================================
@st.cache_data(ttl=30)
def fetch_realtime_prices(symbols_list):
    if not symbols_list:
        return {}
    
    realtime_data = {}
    for symbol in symbols_list:
        try:
            latest = db["live_trades"].find_one(
                {"symbol": symbol},
                {"_id": 0},
                sort=[("timestamp", -1)]
            )
            if latest:
                realtime_data[symbol] = {
                    "price": latest.get("price", 0),
                    "prev_close": latest.get("base_price", latest.get("price", 0)),
                    "change_pct": latest.get("live_war_return_pct", 0),
                    "status": latest.get("status", "normal")
                }
        except:
            pass
    return realtime_data

# ──────────────── Load Gold Data ────────────────
sector_summary = list(db["gold_sector_war_summary"].find({}, {"_id": 0}))
stock_ranking = list(db["gold_stock_ranking"].find({}, {"_id": 0}))
weekly_perf = list(db["gold_weekly_sector_performance"].find({}, {"_id": 0}))
war_timeline = list(db["gold_war_daily_timeline"].find({}, {"_id": 0}))

if not sector_summary:
    st.warning("⏳ Gold Layer ยังไม่มีข้อมูล — กรุณา trigger Gold DAG ก่อน")
    st.stop()

df_sectors = pd.DataFrame(sector_summary)
df_stocks = pd.DataFrame(stock_ranking) if stock_ranking else pd.DataFrame()
df_weekly = pd.DataFrame(weekly_perf) if weekly_perf else pd.DataFrame()
df_timeline = pd.DataFrame(war_timeline) if war_timeline else pd.DataFrame()

# ==========================================
# 🌟 ดึงข้อมูลราคา Live ล่วงหน้าและเตรียมคอลัมน์ให้พร้อมสำหรับทุก Tab
# ==========================================
if not df_stocks.empty:
    all_symbols = df_stocks["symbol"].tolist()
    rt_prices = fetch_realtime_prices(all_symbols)
    
    # เพิ่มคอลัมน์ Live Price และ Live Change ลงในตารางหลัก
    df_stocks["💰 Live Price"] = df_stocks["symbol"].apply(
        lambda x: f"${rt_prices[x]['price']:.2f}" if x in rt_prices and "price" in rt_prices[x] else "-"
    )
    df_stocks["📈 Change %"] = df_stocks["symbol"].apply(
        lambda x: f"{rt_prices[x]['change_pct']:+.2f}%" if x in rt_prices and "change_pct" in rt_prices[x] else "-"
    )

if not df_sectors.empty:
    df_sectors_sorted = df_sectors.sort_values("avg_war_cumulative_return", ascending=False, na_position="last")
    best_sector_row = df_sectors_sorted.iloc[0] if len(df_sectors_sorted) > 0 else None
    worst_sector_row = df_sectors_sorted.iloc[-1] if len(df_sectors_sorted) > 0 else None
else:
    best_sector_row = None
    worst_sector_row = None

# ──────────────── HEADER ────────────────
st.title("📊 S&P 500 — US-Iran War Impact Dashboard")
st.caption("Medallion Architecture: Bronze → Silver → Gold | WAR_START_DATE = 2026-01-01")

# ──────────────── KPI Row ────────────────
total_stocks = len(df_stocks) if not df_stocks.empty else 0
positive_sectors = len(df_sectors[df_sectors["war_impact_label"].isin(["strong_positive", "positive"])])
negative_sectors = len(df_sectors[df_sectors["war_impact_label"].isin(["strong_negative", "negative"])])

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Stocks", total_stocks)
k2.metric("Sectors Analyzed", len(df_sectors))
k3.metric("Positive Sectors", positive_sectors)
k4.metric("Negative Sectors", negative_sectors)
if best_sector_row is not None:
    k5.metric(
        "Best / Worst Sector",
        f"{best_sector_row['sector']} / {worst_sector_row['sector']}",
    )

st.markdown("---")

# ==========================================
# 🌟 UI สำหรับ Live Alert Monitor (แถบกระพริบด้านบน)
# ==========================================
st.markdown("### 🔴 Live Alert & Monitor (Streaming)")
live_alerts = fetch_live_alerts()

if live_alerts:
    st.error(f"🚨 ด่วน! พบหุ้นผันผวนรุนแรง {len(live_alerts)} ตัวในขณะนี้")
    cols = st.columns(min(len(live_alerts), 4))
    for i, alert in enumerate(live_alerts[:4]):
        with cols[i]:
            symbol = alert.get('symbol', 'N/A')
            impact = alert.get('live_war_return_pct', 0)
            price = alert.get('live_price', 0)
            status = alert.get('status', 'normal')
            
            if status == "alert_spike":
                color = "#00FF00"
                icon = "🚀"
            else:
                color = "#FF4B4B"
                icon = "🔻"
                
            st.markdown(f"""
                <div style="border: 1px solid {color}; border-radius: 8px; padding: 10px; background-color: rgba(0,0,0,0.2); text-align: center;">
                    <h4 style="margin: 0; color: white;">
                        {icon} {symbol}
                        <span style="height: 10px; width: 10px; background-color: {color}; border-radius: 50%; display: inline-block; animation: blink 1s linear infinite;"></span>
                    </h4>
                    <p style="margin: 0; color: {color}; font-size: 1.2em; font-weight: bold;">
                        {impact:+.2f}%
                    </p>
                    <p style="margin: 0; color: gray; font-size: 0.9em;">
                        Live Price: ${price:.2f}
                    </p>
                </div>
                <style>
                @keyframes blink {{
                    0% {{ opacity: 1; }}
                    50% {{ opacity: 0; }}
                    100% {{ opacity: 1; }}
                }}
                </style>
            """, unsafe_allow_html=True)
else:
    st.info("✅ ตลาดปกติ — ยังไม่พบการเคลื่อนไหวของราคาที่รุนแรงในขณะนี้ (Streaming Active)")

st.markdown("---")

# ──────────────── TAB Layout ────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏭 Sector Overview",
    "📈 Top Winners & Losers",
    "📉 Weekly Trend",
    "🗓️ War Timeline",
    "🔍 Stock Search",
])

# ==================== TAB 1: Sector Overview ====================
with tab1:
    st.subheader("Sector War Impact Summary")
    st.caption("คลิกที่ Sector เพื่อดูหุ้นทั้งหมดในกลุ่ม")

    for _, row in df_sectors.iterrows():
        label = row["war_impact_label"]
        emoji = IMPACT_EMOJI.get(label, "⚪")
        shift = row["median_performance_shift"]
        sector_name = row["sector"]

        with st.expander(f"{emoji} {sector_name}  —  Shift: {shift:+.2f}%/day  |  Impact: {label}  |  {row['stock_count']} stocks"):
            if not df_stocks.empty:
                sector_stocks = df_stocks[df_stocks["sector"] == sector_name].copy()
                if not sector_stocks.empty:
                    sector_stocks = sector_stocks.sort_values("rank")
                    
                    # 🌟 แสดงตารางพร้อมคอลัมน์ Live Price
                    show_cols = ["rank", "symbol", "full_name", "💰 Live Price", "📈 Change %", 
                                 "war_cumulative_return_pct", "performance_shift", "war_impact"]
                    available_cols = [col for col in show_cols if col in sector_stocks.columns]
                    
                    display_df = sector_stocks[available_cols].copy()
                    display_df = display_df.rename(columns={
                        "war_cumulative_return_pct": "War Return %",
                        "performance_shift": "Shift %/day",
                        "war_impact": "Impact"
                    })
                    
                    st.dataframe(display_df, width="stretch", hide_index=True)
                else:
                    st.info("ไม่มีหุ้นใน sector นี้")

            # Top winners/losers ของ sector
            winners = row.get("top_5_winners", [])
            losers = row.get("top_5_losers", [])
            if winners or losers:
                wc, lc = st.columns(2)
                with wc:
                    st.markdown("**🚀 Top 5 Winners**")
                    for w in winners:
                        st.markdown(f"- {w['symbol']}: `{w['shift']:+.4f}%/day`")
                with lc:
                    st.markdown("**📉 Top 5 Losers**")
                    for l in losers:
                        st.markdown(f"- {l['symbol']}: `{l['shift']:+.4f}%/day`")

    st.markdown("---")
    st.subheader("Median Performance Shift by Sector")
    chart_df = df_sectors[["sector", "median_performance_shift"]].set_index("sector")
    st.bar_chart(chart_df)

    if "avg_war_volatility" in df_sectors.columns:
        st.subheader("Average War-Period Volatility by Sector")
        vol_df = df_sectors[["sector", "avg_war_volatility"]].set_index("sector")
        st.bar_chart(vol_df)

# ==================== TAB 2: Top Winners & Losers ====================
with tab2:
    if not df_stocks.empty:
        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("🚀 Top 20 Winners")
            # 🌟 แทรก Live Price ไว้ในตาราง Top Winners
            cols_w = ["rank", "symbol", "full_name", "💰 Live Price", "📈 Change %", "sector", "war_cumulative_return_pct", "war_impact"]
            top_w = df_stocks.head(20)[cols_w].rename(columns={
                "war_cumulative_return_pct": "War Return %", "war_impact": "Impact"
            })
            st.dataframe(top_w, width="stretch", hide_index=True)

        with col_b:
            st.subheader("📉 Top 20 Losers")
            # 🌟 แทรก Live Price ไว้ในตาราง Top Losers
            cols_l = ["rank", "symbol", "full_name", "💰 Live Price", "📈 Change %", "sector", "war_cumulative_return_pct", "war_impact"]
            top_l = df_stocks.tail(20).iloc[::-1][cols_l].rename(columns={
                "war_cumulative_return_pct": "War Return %", "war_impact": "Impact"
            })
            st.dataframe(top_l, width="stretch", hide_index=True)

        st.markdown("---")
        st.subheader("Full Ranking Table")

        sectors_list = ["All"] + sorted(df_stocks["sector"].unique().tolist())
        selected_sector = st.selectbox("Filter by Sector", sectors_list, key="rank_sector")
        filtered = df_stocks if selected_sector == "All" else df_stocks[df_stocks["sector"] == selected_sector]

        # 🌟 แทรก Live Price ไว้ในตาราง Full Ranking
        display_cols = ["rank", "symbol", "full_name", "sector", "💰 Live Price", "📈 Change %", "war_impact",
                        "performance_shift", "war_cumulative_return_pct",
                        "pre_war_cumulative_return_pct", "war_volatility"]
        available_cols = [col for col in display_cols if col in filtered.columns]
        
        display_full = filtered[available_cols].rename(columns={
            "war_cumulative_return_pct": "War Return %"
        })
        st.dataframe(display_full, width="stretch", hide_index=True)
    else:
        st.info("ไม่มีข้อมูล stock ranking")

# ==================== TAB 3: Weekly Trend ====================
with tab3:
    if not df_weekly.empty:
        st.subheader("Weekly Avg Daily Return by Sector")

        sectors_avail = sorted(df_weekly["sector"].unique().tolist())
        selected = st.multiselect("Select Sectors", sectors_avail, default=sectors_avail[:5], key="weekly_sectors")

        if selected:
            wk = df_weekly[df_weekly["sector"].isin(selected)].copy()
            def week_to_date(w):
                try:
                    parts = w.split("-W")
                    return _dt.datetime.strptime(f"{parts[0]}-W{parts[1]}-1", "%Y-W%W-%w").strftime("%d %b %Y")
                except Exception:
                    return w
            wk["week_label"] = wk["week"].apply(week_to_date)
            pivot = wk.pivot_table(index="week_label", columns="sector", values="avg_daily_return_pct", aggfunc="first")
            try:
                pivot.index = pd.to_datetime(pivot.index, format="%d %b %Y")
                pivot = pivot.sort_index()
                pivot.index = pivot.index.strftime("%d %b %Y")
            except:
                pivot = pivot.sort_index()
            st.line_chart(pivot)
        else:
            st.info("เลือก Sector อย่างน้อย 1 กลุ่ม")

        st.markdown("---")
        st.subheader("Pre-War vs War Average Return by Sector")
        period_agg = df_weekly.groupby(["sector", "period"])["avg_daily_return_pct"].mean().unstack("period")
        if "pre_war" in period_agg.columns and "war" in period_agg.columns:
            period_agg = period_agg[["pre_war", "war"]].sort_values("war", ascending=False)
            st.dataframe(period_agg.style.format("{:.4f}"), width="stretch")
    else:
        st.info("ไม่มีข้อมูล weekly performance")

# ==================== TAB 4: War Timeline ====================
with tab4:
    if not df_timeline.empty:
        st.subheader("Daily Market Return During War Period")
        tl = df_timeline.copy()
        tl["date"] = pd.to_datetime(tl["date"])
        tl = tl.sort_values("date")

        st.line_chart(tl.set_index("date")["market_avg_return"])

        st.markdown("---")
        st.subheader("Best & Worst Sectors Each Day")
        display_tl = tl[["date", "market_avg_return", "best_sector", "best_sector_return",
                          "worst_sector", "worst_sector_return"]].copy()
        display_tl["date"] = display_tl["date"].dt.strftime("%Y-%m-%d")
        st.dataframe(display_tl, width="stretch", hide_index=True)

        st.markdown("---")
        st.subheader("Sector Returns Over Time")
        sector_series = []
        for _, row in tl.iterrows():
            sr = row.get("sector_returns", {})
            if isinstance(sr, dict):
                for sec, val in sr.items():
                    sector_series.append({"date": row["date"], "sector": sec, "return": val})
        if sector_series:
            hm = pd.DataFrame(sector_series)
            hm_pivot = hm.pivot(index="date", columns="sector", values="return")
            hm_pivot = hm_pivot.sort_index()
            st.line_chart(hm_pivot)
    else:
        st.info("ไม่มีข้อมูล war timeline")

# ==================== TAB 5: Stock Search ====================
with tab5:
    if not df_stocks.empty:
        st.subheader("🔍 Search Individual Stock")
        search = st.text_input("Enter stock symbol (e.g. AAPL, MSFT)", key="stock_search").upper().strip()

        if search:
            match = df_stocks[df_stocks["symbol"] == search]
            if not match.empty:
                s = match.iloc[0]
                emoji = IMPACT_EMOJI.get(s["war_impact"], "⚪")
                st.markdown(f"### {emoji} {s['symbol']} — {s.get('full_name', '')}")

                # 🌟 นำ Live Price มาแสดงเป็น Metric แถวแรกสุดให้ดูกลืนกับข้อมูลอื่น
                realtime_single = fetch_realtime_prices([search])
                current_price_str = "-"
                change_pct_str = "-"
                if realtime_single and search in realtime_single:
                    rt = realtime_single[search]
                    current_price_str = f"${rt.get('price', 0):.2f}"
                    change_pct_str = f"{rt.get('change_pct', 0):+.2f}%"

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("💰 Live Price", current_price_str, change_pct_str)
                m2.metric("Rank", f"#{s['rank']}")
                m3.metric("Sector", s["sector"])
                m4.metric("War Impact", s["war_impact"])

                m5, m6, m7, m8 = st.columns(4)
                m5.metric("War Cumulative Return", f"{s.get('war_cumulative_return_pct', 0):.2f}%")
                m6.metric("Pre-War Cumulative Return", f"{s.get('pre_war_cumulative_return_pct', 0):.2f}%")
                m7.metric("War Volatility", f"{s.get('war_volatility', 0):.4f}")
                m8.metric("Performance Shift", f"{s['performance_shift']:+.2f}%")

                hist = list(db["silver_historical_daily"].find(
                    {"symbol": search},
                    {"_id": 0, "date": 1, "close": 1, "period": 1}
                ).sort("date", 1))
                if hist:
                    hdf = pd.DataFrame(hist)
                    hdf["date"] = pd.to_datetime(hdf["date"])
                    st.markdown("---")
                    st.subheader(f"Price History — {search}")
                    st.line_chart(hdf.set_index("date")["close"])
            else:
                st.warning(f"ไม่พบ symbol: {search}")
    else:
        st.info("ไม่มีข้อมูล stock ranking")

# ──────────────── Footer ────────────────
st.markdown("---")
st.caption(f"S&P 500: {total_stocks} stocks | {len(df_sectors)} sectors | Gold Layer powered by Apache Airflow + MongoDB")