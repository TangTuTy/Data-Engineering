import streamlit as st
import pandas as pd
import time
from pymongo import MongoClient

st.set_page_config(page_title="S&P 500 — US-Iran War Impact Dashboard", layout="wide")

IMPACT_COLORS = {
    "strong_positive": "#16a34a",
    "positive": "#22c55e",
    "neutral": "#fbbf24",
    "negative": "#ef4444",
    "strong_negative": "#b91c1c",
}

IMPACT_EMOJI = {
    "strong_positive": "🟢",
    "positive": "🟩",
    "neutral": "🟡",
    "negative": "🟥",
    "strong_negative": "🔴",
}


@st.cache_resource
def get_db():
    import os
    mongo_host = os.environ.get("MONGO_HOST", "localhost")
    client = MongoClient(f"mongodb://{mongo_host}:27017/")
    return client["stock_database"]


@st.cache_data(ttl=30)
def load_collection(collection_name: str):
    return list(db[collection_name].find({}, {"_id": 0}))


@st.cache_data(ttl=30)
def fetch_realtime_prices(symbols_list):
    if not symbols_list:
        return {}

    realtime_data = {}
    for symbol in symbols_list:
        latest = db["live_trades"].find_one(
            {"symbol": symbol},
            {"_id": 0},
            sort=[("timestamp", -1)],
        )
        if latest:
            realtime_data[symbol] = {
                "price": latest.get("price", 0),
                "volume": latest.get("volume", 0),
                "timestamp": latest.get("timestamp"),
            }
    return realtime_data


@st.cache_data(ttl=3600)
def fetch_previous_closes(symbols_tuple):
    """ดึง previous close (ราคาปิดเมื่อวาน) จาก yfinance — cache 1 ชม. เพราะค่าไม่เปลี่ยนระหว่างวัน"""
    symbols = list(symbols_tuple)
    if not symbols:
        return {}
    try:
        import yfinance as yf
        df = yf.download(symbols if len(symbols) > 1 else symbols[0],
                         period="5d", interval="1d", progress=False, auto_adjust=True)
        if df.empty:
            return {}
        prev_closes = {}
        if len(symbols) == 1:
            closes = df["Close"].dropna()
            if len(closes) >= 2:
                prev_closes[symbols[0]] = float(closes.iloc[-2])
        else:
            for sym in symbols:
                try:
                    closes = df[sym]["Close"].dropna() if sym in df.columns.get_level_values(0) else pd.Series()
                    if len(closes) >= 2:
                        prev_closes[sym] = float(closes.iloc[-2])
                except Exception:
                    pass
        return prev_closes
    except Exception:
        return {}


@st.cache_data(ttl=30)
def fetch_intraday_movers():
    """ดึง latest price ของทุก symbol ใน live_trades แล้วคำนวณ Intraday % เทียบ previous close จริง"""
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$symbol",
            "live_price": {"$first": "$price"},
            "volume": {"$first": "$volume"},
            "timestamp": {"$first": "$timestamp"},
        }},
        {"$project": {"_id": 0, "symbol": "$_id", "live_price": 1, "volume": 1, "timestamp": 1}},
    ]
    live_data = {d["symbol"]: d for d in db["live_trades"].aggregate(pipeline)}
    if not live_data:
        return [], []

    symbols = list(live_data.keys())
    # ดึง previous close จาก yfinance (ราคาปิดเมื่อวานจริงๆ)
    prev_closes = fetch_previous_closes(tuple(sorted(symbols)))

    # fallback: ถ้า yfinance ไม่ได้ ใช้ war_latest_close จาก MongoDB
    if not prev_closes:
        prev_closes = {
            d["symbol"]: d.get("war_latest_close")
            for d in db["gold_stock_ranking"].find(
                {"symbol": {"$in": symbols}},
                {"symbol": 1, "war_latest_close": 1, "_id": 0}
            )
            if d.get("war_latest_close")
        }

    rows = []
    for sym, d in live_data.items():
        try:
            live_price = float(d["live_price"])
            base = prev_closes.get(sym)
            if live_price and base and float(base) != 0 and not pd.isna(live_price):
                intraday_pct = round((live_price - float(base)) / float(base) * 100, 2)
                rows.append({
                    "symbol": sym,
                    "Live Price": f"${live_price:.2f}",
                    "Intraday %": intraday_pct,
                    "Volume": int(d["volume"]) if d.get("volume") else 0,
                    "Updated": d.get("timestamp"),
                })
        except Exception:
            continue

    rows.sort(key=lambda x: x["Intraday %"], reverse=True)
    gainers = rows[:5]
    losers = sorted(rows, key=lambda x: x["Intraday %"])[:5]

    for r in gainers + losers:
        r["_intraday_num"] = r["Intraday %"]
        r["Intraday %"] = f"{r['Intraday %']:+.2f}%"
        r["Volume"] = f"{r['Volume']:,}"

    return gainers, losers


@st.cache_data(ttl=60)
def fetch_yfinance_fallback(symbols_tuple):
    """ดึงราคาล่าสุดจาก yfinance สำหรับ symbols ที่ไม่มีใน live_trades (cache 60s)"""
    symbols = list(symbols_tuple)
    if not symbols:
        return {}
    try:
        import yfinance as yf
        if len(symbols) == 1:
            df = yf.download(symbols[0], period="1d", interval="1m", progress=False, auto_adjust=True)
            if not df.empty and "Close" in df.columns:
                last = df["Close"].dropna()
                if not last.empty:
                    return {symbols[0]: float(last.iloc[-1])}
            return {}
        else:
            df = yf.download(
                symbols, period="1d", interval="1m",
                progress=False, auto_adjust=True, group_by="ticker",
            )
            prices = {}
            if df.empty:
                return {}
            for sym in symbols:
                try:
                    last = df[sym]["Close"].dropna()
                    if not last.empty:
                        prices[sym] = float(last.iloc[-1])
                except Exception:
                    pass
            return prices
    except Exception:
        return {}



def safe_metric(value, decimals=2, suffix=""):
    try:
        if pd.isna(value):
            return "-"
        return f"{float(value):,.{decimals}f}{suffix}"
    except Exception:
        return str(value)



def style_impact_label(label):
    emoji = IMPACT_EMOJI.get(label, "⚪")
    return f"{emoji} {label}"



db = get_db()

sector_summary = load_collection("gold_sector_war_summary")
stock_ranking = load_collection("gold_stock_ranking")
weekly_perf = load_collection("gold_weekly_sector_performance")
war_timeline = load_collection("gold_war_daily_timeline")

if not sector_summary:
    st.warning("⏳ Gold Layer ยังไม่มีข้อมูล — กรุณา trigger pipeline ก่อน")
    st.stop()


df_sectors = pd.DataFrame(sector_summary)
df_stocks = pd.DataFrame(stock_ranking) if stock_ranking else pd.DataFrame()
df_weekly = pd.DataFrame(weekly_perf) if weekly_perf else pd.DataFrame()
df_timeline = pd.DataFrame(war_timeline) if war_timeline else pd.DataFrame()

if not df_stocks.empty:
    all_symbols = df_stocks["symbol"].tolist()
    rt_prices = fetch_realtime_prices(all_symbols)
    df_stocks["Live Price"] = df_stocks["symbol"].apply(
        lambda x: f"${rt_prices[x]['price']:.2f}" if x in rt_prices else "-"
    )
    df_stocks["Live Volume"] = df_stocks["symbol"].apply(
        lambda x: f"{rt_prices[x]['volume']:,}" if x in rt_prices and rt_prices[x].get("volume") is not None else "-"
    )

if not df_sectors.empty:
    df_sectors = df_sectors.sort_values("median_performance_shift", ascending=False, na_position="last")
    best_sector_row = df_sectors.iloc[0] if len(df_sectors) > 0 else None
    worst_sector_row = df_sectors.iloc[-1] if len(df_sectors) > 0 else None
else:
    best_sector_row = None
    worst_sector_row = None

st.title("📊 S&P 500 — US-Iran War Impact Dashboard")
st.caption("Medallion Architecture: Bronze → Silver → Gold | Source: yfinance + MongoDB + Airflow")

# Auto-refresh control
_r_col, _t_col = st.columns([1, 5])
with _r_col:
    auto_refresh = st.toggle("🔄 Auto-refresh", value=True)
with _t_col:
    if auto_refresh:
        refresh_interval = st.select_slider(
            "Interval (seconds)", options=[15, 30, 60, 120], value=30, label_visibility="collapsed"
        )
        st.caption(f"Refreshing every {refresh_interval}s")

total_stocks = len(df_stocks) if not df_stocks.empty else 0
positive_sectors = len(df_sectors[df_sectors["war_impact_label"] == "positive"]) if not df_sectors.empty else 0
negative_sectors = len(df_sectors[df_sectors["war_impact_label"] == "negative"]) if not df_sectors.empty else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Stocks", total_stocks)
k2.metric("Sectors Analyzed", len(df_sectors))
k3.metric("Positive Sectors", positive_sectors)
k4.metric("Negative Sectors", negative_sectors)
if best_sector_row is not None and worst_sector_row is not None:
    k5.metric("Best / Worst Sector", f"{best_sector_row['sector']} / {worst_sector_row['sector']}")

st.markdown("---")

def style_intraday_df(df):
    """ใส่สีเขียว/แดงให้คอลัมน์ Intraday % ตามค่า numeric ใน _intraday_num"""
    def color_row(row):
        val = row.get("_intraday_num", 0)
        color = "color: #22c55e" if val >= 0 else "color: #ef4444"
        return [color if col == "Intraday %" else "" for col in row.index]
    display_cols = ["symbol", "Live Price", "Intraday %", "Volume"]
    styled = df.style.apply(color_row, axis=1)
    return styled, display_cols


st.markdown("### 🔴 Live Trade Monitor — Intraday Biggest Movers")
gainers, losers = fetch_intraday_movers()
if gainers or losers:
    col_g, col_l = st.columns(2)
    with col_g:
        st.markdown("**🔺 Top 5 Gainers**")
        if gainers:
            gdf = pd.DataFrame(gainers)
            styled_g, cols = style_intraday_df(gdf)
            st.dataframe(styled_g, column_order=cols, use_container_width=True, hide_index=True)
        else:
            st.info("ยังไม่มีข้อมูล")
    with col_l:
        st.markdown("**🔻 Top 5 Losers**")
        if losers:
            ldf = pd.DataFrame(losers)
            styled_l, cols = style_intraday_df(ldf)
            st.dataframe(styled_l, column_order=cols, use_container_width=True, hide_index=True)
        else:
            st.info("ยังไม่มีข้อมูล")
else:
    st.info("ยังไม่มีข้อมูล live trades — ตรวจสอบว่า realtime_producer กำลังรันอยู่")

st.markdown("---")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏭 Sector Overview",
    "📈 Stock Ranking",
    "📉 Weekly Trend",
    "🗓️ War Timeline",
    "🔍 Stock Search",
])

with tab1:
    st.subheader("Sector War Impact Summary")

    for _, row in df_sectors.iterrows():
        label = row.get("war_impact_label", "neutral")
        emoji = IMPACT_EMOJI.get(label, "⚪")
        sector_name = row.get("sector", "Unknown")
        avg_change = row.get("median_performance_shift", 0)
        stock_count = row.get("stock_count", 0)

        with st.expander(f"{emoji} {sector_name} — Median Performance Shift: {avg_change:+.2f}% | {stock_count} stocks"):
            if not df_stocks.empty:
                sector_stocks = df_stocks[df_stocks["sector"] == sector_name].copy()
                if not sector_stocks.empty:
                    sector_stocks = sector_stocks.sort_values("rank")
                    show_cols = [
                        "rank",
                        "symbol",
                        "full_name",
                        "industry",
                        "Live Price",
                        "Live Volume",
                        "pre_war_cumulative_return_pct",
                        "war_cumulative_return_pct",
                        "performance_shift",
                        "war_impact",
                    ]
                    available_cols = [col for col in show_cols if col in sector_stocks.columns]
                    display_df = sector_stocks[available_cols].rename(
                        columns={
                            "pre_war_cumulative_return_pct": "Pre-War Cumulative Return %",
                            "war_cumulative_return_pct": "War Cumulative Return %",
                            "performance_shift": "Performance Shift %",
                            "war_impact": "Impact",
                        }
                    )
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                else:
                    st.info("ไม่มีหุ้นใน sector นี้")

    st.markdown("---")
    st.subheader("Median Performance Shift by Sector")
    chart_df = df_sectors[["sector", "median_performance_shift"]].set_index("sector")
    st.bar_chart(chart_df)

with tab2:
    if not df_stocks.empty:
        st.subheader("Stock Ranking by War Impact")

        sectors_list = ["All"] + sorted(df_stocks["sector"].dropna().unique().tolist())
        selected_sector = st.selectbox("Filter by Sector", sectors_list, key="rank_sector")
        filtered = df_stocks if selected_sector == "All" else df_stocks[df_stocks["sector"] == selected_sector]

        show_cols = [
            "rank",
            "symbol",
            "full_name",
            "sector",
            "industry",
            "Live Price",
            "Live Volume",
            "pre_war_cumulative_return_pct",
            "war_cumulative_return_pct",
            "performance_shift",
            "war_impact",
        ]
        available_cols = [col for col in show_cols if col in filtered.columns]
        display_df = filtered[available_cols].rename(
            columns={
                "pre_war_cumulative_return_pct": "Pre-War Cumulative Return %",
                "war_cumulative_return_pct": "War Cumulative Return %",
                "performance_shift": "Performance Shift %",
                "war_impact": "Impact",
            }
        )
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.markdown("---")
        col_a, col_b = st.columns(2)

        top_positive = filtered.sort_values("performance_shift", ascending=False).head(20)
        top_negative = filtered.sort_values("performance_shift", ascending=True).head(20)

        # ดึง live price เฉพาะ 40 ตัวนี้เท่านั้น
        top40_symbols = list(set(top_positive["symbol"].tolist() + top_negative["symbol"].tolist()))
        rt_top40 = fetch_realtime_prices(top40_symbols)

        # symbols ที่ไม่มีใน live_trades → fallback ดึงจาก yfinance
        missing = [s for s in top40_symbols if s not in rt_top40 or rt_top40[s].get("price") is None]
        yf_prices = fetch_yfinance_fallback(tuple(sorted(missing))) if missing else {}

        # รวมทั้งสอง source (live_trades ก่อน, yfinance fallback ถ้าไม่มี)
        merged_prices = {}
        for sym in top40_symbols:
            rt = rt_top40.get(sym, {})
            live_val = rt.get("price") if rt else None
            try:
                if live_val is not None and not pd.isna(float(live_val)) and float(live_val) != 0:
                    merged_prices[sym] = float(live_val)
                elif sym in yf_prices:
                    merged_prices[sym] = yf_prices[sym]
            except Exception:
                if sym in yf_prices:
                    merged_prices[sym] = yf_prices[sym]

        def enrich_with_live(df):
            df = df.copy()

            def get_live_price(symbol):
                val = merged_prices.get(symbol)
                if val is None:
                    return None
                try:
                    f = float(val)
                    return f if f != 0 and not pd.isna(f) else None
                except Exception:
                    return None

            df["_live_price"] = df["symbol"].apply(get_live_price)
            df["_intraday_num"] = df.apply(
                lambda row: round(
                    (row["_live_price"] - row["war_latest_close"]) / row["war_latest_close"] * 100, 2
                )
                if (
                    row["_live_price"] is not None
                    and row.get("war_latest_close") is not None
                    and not pd.isna(row.get("war_latest_close", float("nan")))
                    and row["war_latest_close"] != 0
                )
                else None,
                axis=1,
            )
            df["Intraday %"] = df["_intraday_num"].apply(
                lambda x: f"{x:+.2f}%" if x is not None else "-"
            )
            df["Live Price"] = df["_live_price"].apply(
                lambda x: f"${x:.2f}" if x is not None else "-"
            )
            df = df.drop(columns=["_live_price"])
            return df

        top_positive = enrich_with_live(top_positive)
        top_negative = enrich_with_live(top_negative)

        top_cols = [
            "rank", "symbol", "full_name", "sector",
            "Live Price", "Intraday %",
            "war_cumulative_return_pct", "performance_shift", "war_impact",
        ]

        def style_top20(df, show_cols):
            rename_map = {
                "war_cumulative_return_pct": "War Cum Return %",
                "performance_shift": "Shift %/day",
                "war_impact": "Impact",
            }
            display = df[[c for c in show_cols if c in df.columns]].rename(columns=rename_map)
            def color_intraday(row):
                styles = ["" for _ in row.index]
                if "Intraday %" in row.index:
                    idx = list(row.index).index("Intraday %")
                    num_col = "_intraday_num"
                    # ใช้ค่า string เพื่อตรวจ
                    val_str = row["Intraday %"]
                    if isinstance(val_str, str) and val_str not in ("-", ""):
                        try:
                            val = float(val_str.replace("%", "").replace("+", ""))
                            styles[idx] = "color: #22c55e" if val >= 0 else "color: #ef4444"
                        except Exception:
                            pass
                return styles
            return display.style.apply(color_intraday, axis=1)

        with col_a:
            st.subheader("🚀 Top 20 Positive")
            show = [c for c in top_cols if c in top_positive.columns]
            st.dataframe(style_top20(top_positive, show), use_container_width=True, hide_index=True)
        with col_b:
            st.subheader("📉 Top 20 Negative")
            show = [c for c in top_cols if c in top_negative.columns]
            st.dataframe(style_top20(top_negative, show), use_container_width=True, hide_index=True)
    else:
        st.info("ไม่มีข้อมูล stock ranking")

with tab3:
    if not df_weekly.empty:
        st.subheader("Weekly Sector Performance")
        sectors_avail = sorted(df_weekly["sector"].dropna().unique().tolist())
        selected = st.multiselect("Select Sectors", sectors_avail, default=sectors_avail[:5] if sectors_avail else [], key="weekly_sectors")

        if selected:
            wk = df_weekly[df_weekly["sector"].isin(selected)].copy()
            pivot = wk.pivot_table(index="week", columns="sector", values="avg_daily_return_pct", aggfunc="first")
            pivot = pivot.sort_index()
            st.line_chart(pivot)
            st.dataframe(wk.sort_values(["week", "sector"]), use_container_width=True, hide_index=True)
        else:
            st.info("เลือก Sector อย่างน้อย 1 กลุ่ม")
    else:
        st.info("ไม่มีข้อมูล weekly performance")

with tab4:
    if not df_timeline.empty:
        st.subheader("Daily Market Return Timeline")
        df_timeline["date"] = pd.to_datetime(df_timeline["date"], errors="coerce")
        df_timeline = df_timeline.sort_values("date")
        st.line_chart(df_timeline.set_index("date")[["market_avg_return"]])

        display_tl = df_timeline[["date", "market_avg_return", "best_sector", "best_sector_return", "worst_sector", "worst_sector_return"]].rename(
            columns={
                "date": "Date",
                "market_avg_return": "Market Avg Return %",
                "best_sector": "Best Sector",
                "best_sector_return": "Best Sector Return %",
                "worst_sector": "Worst Sector",
                "worst_sector_return": "Worst Sector Return %",
            }
        )
        st.dataframe(display_tl, use_container_width=True, hide_index=True)
    else:
        st.info("ไม่มีข้อมูล war timeline")

with tab5:
    if not df_stocks.empty:
        st.subheader("🔍 Search Individual Stock")
        search = st.text_input("Enter stock symbol (e.g. AAPL, MSFT)", key="stock_search").upper().strip()

        if search:
            match = df_stocks[df_stocks["symbol"] == search]
            if not match.empty:
                s = match.iloc[0]
                emoji = IMPACT_EMOJI.get(s.get("war_impact", "neutral"), "⚪")
                st.markdown(f"### {emoji} {s['symbol']} — {s.get('full_name', '')}")

                realtime_single = fetch_realtime_prices([search])
                current_price_str = "-"
                volume_str = "-"
                if realtime_single and search in realtime_single:
                    rt = realtime_single[search]
                    current_price_str = f"${rt.get('price', 0):.2f}"
                    volume_str = f"{rt.get('volume', 0):,}"

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Live Price", current_price_str)
                m2.metric("Rank", f"#{s['rank']}")
                m3.metric("Sector", s.get("sector", "-"))
                m4.metric("Impact", style_impact_label(s.get("war_impact", "neutral")))

                m5, m6, m7, m8 = st.columns(4)
                m5.metric("Pre-War Cumulative Return", safe_metric(s.get("pre_war_cumulative_return_pct", 0), suffix="%"))
                m6.metric("War Cumulative Return", safe_metric(s.get("war_cumulative_return_pct", 0), suffix="%"))
                m7.metric("Performance Shift", safe_metric(s.get("performance_shift", 0), suffix="%"))
                m8.metric("Live Volume", volume_str)

                hist = list(
                    db["silver_historical_daily"].find(
                        {"symbol": search},
                        {"_id": 0, "date": 1, "close": 1, "daily_return_pct": 1},
                    ).sort("date", 1)
                )
                if hist:
                    hdf = pd.DataFrame(hist)
                    hdf["date"] = pd.to_datetime(hdf["date"], errors="coerce")
                    st.markdown("---")
                    st.subheader(f"Price History — {search}")
                    st.line_chart(hdf.set_index("date")[["close"]])

                    st.subheader(f"Daily Return — {search}")
                    st.line_chart(hdf.set_index("date")[["daily_return_pct"]])
            else:
                st.warning(f"ไม่พบ symbol: {search}")
    else:
        st.info("ไม่มีข้อมูล stock ranking")

st.markdown("---")
st.caption(f"S&P 500: {total_stocks} stocks | {len(df_sectors)} sectors | Gold Layer powered by Apache Airflow + MongoDB")

# Auto-refresh: rerun หน้าตาม interval ที่เลือก
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()