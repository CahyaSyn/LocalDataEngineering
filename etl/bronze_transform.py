from sqlalchemy import text
from db_connection import get_engine

def transform_bronze():
    engine = get_engine()

    sql = """
    CREATE TABLE IF NOT EXISTS bronze.transactions (
        order_id INT PRIMARY KEY,
        user_id INT NOT NULL,
        product TEXT,
        price NUMERIC NOT NULL,
        quantity INT NOT NULL,
        order_date TIMESTAMP NOT NULL
    );
        
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'chk_price_positive'
        ) THEN
            ALTER TABLE bronze.transactions
            ADD CONSTRAINT chk_price_positive CHECK (price > 0);
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'chk_quantity_positive'
        ) THEN
            ALTER TABLE bronze.transactions
            ADD CONSTRAINT chk_quantity_positive CHECK (quantity > 0);
        END IF;
    END$$;

    INSERT INTO bronze.transactions
    SELECT
        order_id,
        user_id,
        product,
        price,
        quantity,
        order_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id
                   ORDER BY ingestion_time DESC
               ) AS rn
        FROM raw.transactions_raw
    ) t
    WHERE rn = 1
    ON CONFLICT (order_id) DO NOTHING;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    print("BRONZE incremental transform completed")

if __name__ == "__main__":
    transform_bronze()
