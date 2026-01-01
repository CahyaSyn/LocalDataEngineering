from sqlalchemy import text
from db_connection import get_engine

def run_data_quality_checks():
    engine = get_engine()

    checks = {
        "bronze_null_order_id": """
            SELECT COUNT(*) FROM bronze.transactions
            WHERE order_id IS NULL
        """,
        "bronze_negative_price": """
            SELECT COUNT(*) FROM bronze.transactions
            WHERE price <= 0
        """,
        "silver_null_order_date": """
            SELECT COUNT(*) FROM silver.sales
            WHERE order_date IS NULL
        """,
        "silver_negative_total_price": """
            SELECT COUNT(*) FROM silver.sales
            WHERE total_price <= 0
        """
    }

    failed = False

    with engine.connect() as conn:
        for name, sql in checks.items():
            result = conn.execute(text(sql)).scalar()
            if result > 0:
                print(f"❌ Data quality failed: {name} → {result} rows")
                failed = True
            else:
                print(f"✅ Passed: {name}")

    if failed:
        raise Exception("Data quality checks failed")

    print("✅ All data quality checks passed")

if __name__ == "__main__":
    run_data_quality_checks()
