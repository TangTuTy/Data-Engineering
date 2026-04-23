from pymongo import MongoClient
import yfinance as yf
import pandas as pd


MONGO_URI = "mongodb://mongodb:27017/"
DB_NAME = "stock_database"
BRONZE_COLLECTION = "historical_prices"
TICKERS_COLLECTION = "sp500_tickers"


def load_sp500_daily_to_bronze():
    """Load latest daily S&P 500 stock prices into the bronze layer."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[BRONZE_COLLECTION]
    tickers_collection = db[TICKERS_COLLECTION]

    ticker_docs = list(tickers_collection.find({}, {"symbol": 1, "_id": 0}))
    symbols = [doc["symbol"] for doc in ticker_docs if doc.get("symbol")]

    if not symbols:
        print("No symbols found in sp500_tickers collection.")
        client.close()
        return

    for symbol in symbols:
        try:
            df = yf.Ticker(symbol).history(period="5d", interval="1d")

            if df.empty:
                continue

            df = df.reset_index()
            df["symbol"] = symbol
            df["Date"] = pd.to_datetime(df["Date"])
            df["DateString"] = df["Date"].dt.strftime("%Y-%m-%d")
            df["DateTime"] = df["Date"].dt.strftime("%Y-%m-%dT00:00:00Z")

            records = df.to_dict("records")

            for record in records:
                clean_record = {}
                for key, value in record.items():
                    if pd.isna(value):
                        clean_record[key] = None
                    elif isinstance(value, pd.Timestamp):
                        clean_record[key] = value.to_pydatetime()
                    else:
                        clean_record[key] = value

                collection.update_one(
                    {
                        "symbol": clean_record["symbol"],
                        "DateString": clean_record["DateString"],
                    },
                    {"$set": clean_record},
                    upsert=True,
                )

        except Exception as e:
            print(f"Error loading {symbol}: {e}")

    client.close()
