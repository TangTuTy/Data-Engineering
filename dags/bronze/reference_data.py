from pymongo import MongoClient
import pandas as pd
from urllib.request import Request, urlopen
import logging


MONGO_URI = "mongodb://mongodb:27017/"
DB_NAME = "stock_database"
TICKERS_COLLECTION = "sp500_tickers"
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Expected range สำหรับ sanity check
MIN_EXPECTED_TICKERS = 490
MAX_EXPECTED_TICKERS = 520


def load_sp500_tickers_to_bronze():
    """Fetch S&P 500 tickers from Wikipedia and store them in the bronze layer.

    รวม data validation เพื่อป้องกันหุ้นผี (delisted/merged) หลุดเข้า pipeline:
    - ตัด row ที่ไม่มี symbol หรือ sector
    - ตัด row ที่ column ไม่ครบ (NaN)
    - Sanity check จำนวน tickers ที่ได้
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[TICKERS_COLLECTION]

    try:
        req = Request(
            SP500_WIKI_URL,
            headers={"User-Agent": "Mozilla/5.0"},
        )

        with urlopen(req) as response:
            html = response.read()

        tables = pd.read_html(html)

        # Wikipedia page มี 2 tables หลัก
        # [0] = Current constituents  <- อันนี้ที่เราต้องการ
        # [1] = Selected changes to the list (historical add/remove)
        df = tables[0].copy()

        # ── Rename columns ──────────────────────────────────────────────
        df = df.rename(
            columns={
                "Symbol": "symbol",
                "Security": "company_name",
                "GICS Sector": "gics_sector",
                "GICS Sub-Industry": "gics_sub_industry",
                "Headquarters Location": "headquarters",
            }
        )

        keep_cols = [
            "symbol",
            "company_name",
            "gics_sector",
            "gics_sub_industry",
            "headquarters",
        ]

        # ── ตรวจสอบ columns ครบไหม ─────────────────────────────────────
        missing = [c for c in keep_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Wikipedia table structure changed — missing columns: {missing}")

        df = df[keep_cols]

        # ── DATA VALIDATION (จุดใหม่ — ตัดหุ้นผี) ─────────────────────
        original_count = len(df)

        # 1. ตัด row ที่ symbol เป็น NaN/ว่าง
        df = df[df["symbol"].notna()]
        df = df[df["symbol"].astype(str).str.strip() != ""]

        # 2. ตัด row ที่ gics_sector เป็น NaN/ว่าง (หุ้นที่ไม่มี sector = เสี่ยง delisted/merged)
        df = df[df["gics_sector"].notna()]
        df = df[df["gics_sector"].astype(str).str.strip() != ""]

        # 3. ตัด duplicate symbol (เผื่อ Wikipedia มี row ซ้ำ)
        df = df.drop_duplicates(subset=["symbol"], keep="first")

        filtered_count = len(df)
        dropped = original_count - filtered_count
        if dropped > 0:
            print(f"⚠️  Data validation: dropped {dropped} row(s) with missing symbol/sector")

        # ── Normalize symbol (BRK.B → BRK-B สำหรับ yfinance) ─────────
        df["symbol"] = df["symbol"].astype(str).str.replace(".", "-", regex=False).str.strip()

        # ── SANITY CHECK ─────────────────────────────────────────────
        count = len(df)
        if count < MIN_EXPECTED_TICKERS or count > MAX_EXPECTED_TICKERS:
            raise ValueError(
                f"S&P 500 ticker count out of expected range: got {count}, "
                f"expected between {MIN_EXPECTED_TICKERS}-{MAX_EXPECTED_TICKERS}. "
                f"Aborting to prevent corrupting downstream pipeline."
            )

        records = df.to_dict("records")

        # ── Replace collection atomically ────────────────────────────
        collection.delete_many({})
        if records:
            collection.insert_many(records)
            collection.create_index("symbol", unique=True)
            collection.create_index("gics_sector")

        print(f"✅ Loaded {len(records)} S&P 500 tickers into bronze layer.")
        print(f"   Sectors: {df['gics_sector'].nunique()} unique sectors")

    except Exception as e:
        print(f"❌ Error loading S&P 500 tickers: {e}")
        raise

    finally:
        client.close()