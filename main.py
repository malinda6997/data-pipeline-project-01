import sys
from etl.extract import extract_products, extract_users
from etl.transform import transform_products, transform_users
from etl.load import load_products, load_users


def run_pipeline():
    print("=== Starting Fake Store ETL Pipeline ===")

    # Step 1: Extract Data
    print("\n[1/3] Extracting data from Fake Store API...")
    raw_products_df = extract_products()
    raw_users_df = extract_users()

    if raw_products_df.empty or raw_users_df.empty:
        print("Pipeline aborted: Failed to extract data.")
        sys.exit(1)
 
    # Step 2: Transform Data
    print("\n[2/3] Transforming extracted data...")
    clean_products_df = transform_products(raw_products_df)
    clean_users_df = transform_users(raw_users_df)

    # Step 3: Load Data into AWS RDS PostgreSQL
    print("\n[3/3] Loading clean data into PostgreSQL database...")
    load_products(clean_products_df)
    load_users(clean_users_df)

    print("\n=== ETL Pipeline Completed Successfully! ===")


if __name__ == "__main__":
    run_pipeline()