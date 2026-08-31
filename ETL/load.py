import os
import io
import boto3
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# DB Configurations
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# S3 Configurations
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def get_db_engine():
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        raise ValueError("Error: Missing database configuration in .env file!")
    
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    return engine


def upload_to_s3(df, file_name):
    """Dataframe එක S3 Bucket එකට CSV file එකක් ලෙස upload කරයි"""
    if not S3_BUCKET_NAME:
        print("S3_BUCKET_NAME not set in .env file. Skipping S3 upload.")
        return

    try:
        s3_client = boto3.client("s3", region_name=AWS_REGION)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"processed/{file_name}.csv",
            Body=csv_buffer.getvalue()
        )
        print(f"Successfully uploaded {file_name}.csv to S3 Bucket ({S3_BUCKET_NAME})!")
    except Exception as e:
        print(f"Error uploading {file_name} to S3: {e}")


def load_products(products_df):
    if products_df.empty:
        print("No product data to load.")
        return

    # 1. Load to PostgreSQL RDS
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

    # 2. Upload to S3
    upload_to_s3(products_df, "products")


def load_users(users_df):
    if users_df.empty:
        print("No user data to load.")
        return

    # 1. Load to PostgreSQL RDS
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

    # 2. Upload to S3
    upload_to_s3(users_df, "users")