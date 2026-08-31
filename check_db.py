import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"
)

print("\n--- DIM_PRODUCTS TABLE ---")
print(pd.read_sql("SELECT * FROM dim_products LIMIT 5;", engine))

print("\n--- DIM_USERS TABLE ---")
print(pd.read_sql("SELECT * FROM dim_users LIMIT 5;", engine))