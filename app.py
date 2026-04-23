import streamlit as st
import pandas as pd
from pymongo import MongoClient

st.set_page_config(page_title="S&P 500 — US-Iran War Impact Dashboard", layout="wide")

IMPACT_COLORS = {
    "positive": "#22c55e",
    "neutral": "#fbbf24",
    "negative": "#ef4444",
}

IMPACT_EMOJI = {
    "positive": "🟢",
    "neutral": "🟡",
    "negative": "🔴",
}


@st.cache_resource
def get_db():
    client = MongoClient("mongodb://mongodb:27017/")
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


@st.cache_data(ttl=30)
def fetch_recent_live_trades(limit=8):
    return list(
        db["live_trades"]
        .find({}, {"_id": 0, "symbol": 1, "price": 1, "volume": 1, "timestamp": 1})
        .sort("timestamp", -1)
        .limit(limit)
    )



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

st.markdown("### 🔴 Live Trade Monitor")
live_trades = fetch_recent_live_trades()
if live_trades:
    live_df = pd.DataFrame(live_trades)
    if not live_df.empty:
        if "timestamp" in live_df.columns:
            live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], errors="coerce")
        st.dataframe(live_df, use_container_width=True, hide_index=True)
else:
    st.info("ยังไม่มีข้อมูล live trades")

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
        with col_a:
            st.subheader("🚀 Top 20 Positive")
            top_positive = filtered.sort_values("performance_shift", ascending=False).head(20)
            st.dataframe(top_positive[[col for col in available_cols if col in top_positive.columns]], use_container_width=True, hide_index=True)
        with col_b:
            st.subheader("📉 Top 20 Negative")
            top_negative = filtered.sort_values("performance_shift", ascending=True).head(20)
            st.dataframe(top_negative[[col for col in available_cols if col in top_negative.columns]], use_container_width=True, hide_index=True)
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