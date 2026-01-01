from db_connection import get_engine
from sqlalchemy import text

engine = get_engine()

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print("Connected: ", result.scalar())
