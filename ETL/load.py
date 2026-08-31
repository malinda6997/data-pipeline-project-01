import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_db_engine():
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        raise ValueError("Error: Missing database configuration in .env file!")
    
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    return engine


def load_products(products_df):
    if products_df.empty:
        print("No product data to load.")
        return

    try:
        engine = get_db_engine()
        products_df.to_sql(
            name='dim_products',
            con=engine,
            if_exists='replace',  
            index=False
        )
        print("Successfully loaded products into PostgreSQL!")
    except Exception as e:
        print(f"Error loading products to database: {e}")


def load_users(users_df):
    if users_df.empty:
        print("No user data to load.")
        return

    try:
        engine = get_db_engine()
        users_df.to_sql(
            name='dim_users',
            con=engine,
            if_exists='replace',
            index=False
        )
        print("Successfully loaded users into PostgreSQL!")
    except Exception as e:
        print(f"Error loading users to database: {e}")
