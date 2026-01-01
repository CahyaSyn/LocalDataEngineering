from sqlalchemy import text
from db_connection import get_engine

def transform_gold():
    engine = get_engine()

    sql = """
    CREATE TABLE IF NOT EXISTS gold.daily_revenue AS
        SELECT 
            order_date,
            SUM(total_price) AS daily_revenue
        FROM silver.sales
        GROUP BY order_date
        ORDER BY order_date;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("GOLD transform completed")

if __name__ == "__main__":
    transform_gold()