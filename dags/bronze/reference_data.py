from pymongo import MongoClient
import pandas as pd
from urllib.request import Request, urlopen


MONGO_URI = "mongodb://mongodb:27017/"
DB_NAME = "stock_database"
TICKERS_COLLECTION = "sp500_tickers"
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def load_sp500_tickers_to_bronze():
    """Fetch S&P 500 tickers from Wikipedia and store them in the bronze layer."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[TICKERS_COLLECTION]

    try:
        req = Request(
            SP500_WIKI_URL,
            headers={
                "User-Agent": "Mozilla/5.0"
            },
        )

        with urlopen(req) as response:
            html = response.read()

        tables = pd.read_html(html)
        df = tables[0].copy()

        df = df.rename(
            columns={
                "Symbol": "symbol",
                "Security": "company_name",
                "GICS Sector": "sector",
                "GICS Sub-Industry": "sub_industry",
                "Headquarters Location": "headquarters",
            }
        )

        keep_cols = [
            "symbol",
            "company_name",
            "sector",
            "sub_industry",
            "headquarters",
        ]
        df = df[keep_cols]

        df["symbol"] = df["symbol"].astype(str).str.replace(".", "-", regex=False)

        records = df.to_dict("records")

        collection.delete_many({})

        if records:
            collection.insert_many(records)

        print(f"Loaded {len(records)} S&P 500 tickers into bronze layer.")

    except Exception as e:
        print(f"Error loading S&P 500 tickers: {e}")
        raise

    finally:
        client.close()