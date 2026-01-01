import pandas as pd
from sqlalchemy import text
from db_connection import get_engine
from datetime import datetime
import pytz

wib = pytz.timezone("Asia/Jakarta")

def ingest_raw():
    engine = get_engine()

    df = pd.read_csv("data/transactions_raw.csv")

    # metadata ingestion
    df["ingestion_time"] = datetime.now(wib)

    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.transactions_raw (
            order_id INT,
            user_id INT,
            product TEXT,
            price NUMERIC,
            quantity INT,
            order_date TIMESTAMP,
            ingestion_time TIMESTAMP  
        );
        """))

    df.to_sql(
        name="transactions_raw",
        con=engine,
        schema="raw",
        if_exists="append",
        index=False
    )

    print("RAW incremental ingestion completed")

if __name__ == "__main__":
    ingest_raw()