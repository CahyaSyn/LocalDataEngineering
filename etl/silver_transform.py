from sqlalchemy import text
from db_connection import get_engine

def transform_silver():
    engine = get_engine()

    sql = """
    CREATE TABLE IF NOT EXISTS silver.sales (
        order_id INT PRIMARY KEY,
        user_id INT NOT NULL,
        product TEXT,
        total_price NUMERIC NOT NULL,
        order_date DATE NOT NULL
    );

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'chk_total_price_positive'
        ) THEN
            ALTER TABLE silver.sales
            ADD CONSTRAINT chk_total_price_positive CHECK (total_price > 0);
        END IF;
    END $$;

    INSERT INTO silver.sales (
        order_id,
        user_id,
        product,
        total_price,
        order_date
    )
    SELECT
        order_id,
        user_id,
        product,
        COALESCE(price, 0) * COALESCE(quantity, 0) AS total_price,
        DATE(order_date)
    FROM bronze.transactions
    ON CONFLICT (order_id) DO UPDATE
    SET
        user_id     = EXCLUDED.user_id,
        product     = EXCLUDED.product,
        total_price = EXCLUDED.total_price,
        order_date  = EXCLUDED.order_date;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("SILVER transform completed (upsert-safe)")

if __name__ == "__main__":
    transform_silver()
