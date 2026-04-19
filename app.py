import streamlit as st
import pandas as pd
from pymongo import MongoClient

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

# ──────────────── HEADER ────────────────
st.title("📊 S&P 500 — US-Iran War Impact Dashboard")
st.caption("Medallion Architecture: Bronze → Silver → Gold | WAR_START_DATE = 2026-01-01")

# ──────────────── KPI Row ────────────────
total_stocks = len(df_stocks) if not df_stocks.empty else 0
positive_sectors = len(df_sectors[df_sectors["war_impact_label"].isin(["strong_positive", "positive"])])
negative_sectors = len(df_sectors[df_sectors["war_impact_label"].isin(["strong_negative", "negative"])])
best_sector_row = df_sectors.iloc[0] if not df_sectors.empty else None
worst_sector_row = df_sectors.iloc[-1] if not df_sectors.empty else None

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
                    show_cols = ["rank", "symbol", "full_name", "war_cumulative_return_pct",
                                 "performance_shift", "war_impact"]
                    st.dataframe(sector_stocks[show_cols], use_container_width=True, hide_index=True)
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

    # Bar chart — median shift by sector
    st.subheader("Median Performance Shift by Sector")
    chart_df = df_sectors[["sector", "median_performance_shift"]].set_index("sector")
    st.bar_chart(chart_df, use_container_width=True)

    # Sector volatility comparison
    if "avg_war_volatility" in df_sectors.columns:
        st.subheader("Average War-Period Volatility by Sector")
        vol_df = df_sectors[["sector", "avg_war_volatility"]].set_index("sector")
        st.bar_chart(vol_df, use_container_width=True)

# ==================== TAB 2: Top Winners & Losers ====================
with tab2:
    if not df_stocks.empty:
        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("🚀 Top 20 Winners")
            top_w = df_stocks.head(20)[["rank", "symbol", "full_name", "sector", "war_cumulative_return_pct", "war_impact"]]
            st.dataframe(top_w, use_container_width=True, hide_index=True)

        with col_b:
            st.subheader("📉 Top 20 Losers")
            top_l = df_stocks.tail(20).iloc[::-1][["rank", "symbol", "full_name", "sector", "war_cumulative_return_pct", "war_impact"]]
            st.dataframe(top_l, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("Full Ranking Table")

        # Filter by sector
        sectors_list = ["All"] + sorted(df_stocks["sector"].unique().tolist())
        selected_sector = st.selectbox("Filter by Sector", sectors_list, key="rank_sector")
        filtered = df_stocks if selected_sector == "All" else df_stocks[df_stocks["sector"] == selected_sector]

        display_cols = ["rank", "symbol", "full_name", "sector", "war_impact",
                        "performance_shift", "war_cumulative_return_pct",
                        "pre_war_cumulative_return_pct", "war_volatility"]
        st.dataframe(filtered[display_cols], use_container_width=True, hide_index=True)
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
            # แปลง week format "2025-W30" → วันจันทร์ของสัปดาห์นั้น
            import datetime as _dt
            def week_to_date(w):
                try:
                    parts = w.split("-W")
                    return _dt.datetime.strptime(f"{parts[0]}-W{parts[1]}-1", "%Y-W%W-%w").strftime("%d %b %Y")
                except Exception:
                    return w
            wk["week_label"] = wk["week"].apply(week_to_date)
            pivot = wk.pivot_table(index="week_label", columns="sector", values="avg_daily_return_pct", aggfunc="mean")
            pivot = pivot.sort_index()
            st.line_chart(pivot, use_container_width=True)
        else:
            st.info("เลือก Sector อย่างน้อย 1 กลุ่ม")

        st.markdown("---")
        st.subheader("Pre-War vs War Average Return by Sector")
        period_agg = df_weekly.groupby(["sector", "period"])["avg_daily_return_pct"].mean().unstack("period")
        if "pre_war" in period_agg.columns and "war" in period_agg.columns:
            period_agg = period_agg[["pre_war", "war"]].sort_values("war", ascending=False)
            st.dataframe(period_agg.style.format("{:.4f}"), use_container_width=True)
    else:
        st.info("ไม่มีข้อมูล weekly performance")

# ==================== TAB 4: War Timeline ====================
with tab4:
    if not df_timeline.empty:
        st.subheader("Daily Market Return During War Period")
        tl = df_timeline.copy()
        tl["date"] = pd.to_datetime(tl["date"])
        tl = tl.sort_values("date")

        st.line_chart(tl.set_index("date")["market_avg_return"], use_container_width=True)

        st.markdown("---")
        st.subheader("Best & Worst Sectors Each Day")
        display_tl = tl[["date", "market_avg_return", "best_sector", "best_sector_return",
                          "worst_sector", "worst_sector_return"]].copy()
        display_tl["date"] = display_tl["date"].dt.strftime("%Y-%m-%d")
        st.dataframe(display_tl, use_container_width=True, hide_index=True)

        # Sector heatmap-like table
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
            st.line_chart(hm_pivot, use_container_width=True)
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

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Rank", f"#{s['rank']}")
                m2.metric("Sector", s["sector"])
                m3.metric("War Impact", s["war_impact"])
                m4.metric("Performance Shift", f"{s['performance_shift']:+.2f}%")

                m5, m6, m7, m8 = st.columns(4)
                m5.metric("War Cumulative Return", f"{s.get('war_cumulative_return_pct', 0):.2f}%")
                m6.metric("Pre-War Cumulative Return", f"{s.get('pre_war_cumulative_return_pct', 0):.2f}%")
                m7.metric("War Volatility", f"{s.get('war_volatility', 0):.4f}")
                m8.metric("War Avg Daily Return", f"{s.get('war_avg_daily_return', 0):.4f}%")

                # Historical price chart for this stock
                hist = list(db["silver_historical_daily"].find(
                    {"symbol": search},
                    {"_id": 0, "date": 1, "close": 1, "period": 1}
                ).sort("date", 1))
                if hist:
                    hdf = pd.DataFrame(hist)
                    hdf["date"] = pd.to_datetime(hdf["date"])
                    st.markdown("---")
                    st.subheader(f"Price History — {search}")
                    st.line_chart(hdf.set_index("date")["close"], use_container_width=True)
            else:
                st.warning(f"ไม่พบ symbol: {search}")
    else:
        st.info("ไม่มีข้อมูล stock ranking")

# ──────────────── Footer ────────────────
st.markdown("---")
st.caption(f"S&P 500: {total_stocks} stocks | {len(df_sectors)} sectors | Gold Layer powered by Apache Airflow + MongoDB")