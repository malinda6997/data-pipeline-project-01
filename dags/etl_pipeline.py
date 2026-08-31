from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default Arguments Configuration
default_args = {
    'owner': 'malinda',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# 1. Extract Step
def extract():
    print("Extracting data from source (API / Database)...")
    data = {"users": 100, "status": "active"}
    return data

# 2. Transform Step
def transform():
    print("Transforming extracted data...")
    transformed_status = "SUCCESS"
    print(f"Transformation status: {transformed_status}")

# 3. Load Step
def load():
    print("Loading transformed data into Data Warehouse / Database...")

# DAG Definition
with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='Automated Data Pipeline with Airflow and GitHub Actions',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['etl', 'data_engineering'],
) as dag:

    # Define Tasks
    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load,
    )

    # Set Task Dependencies (Extract -> Transform -> Load)
    task_extract >> task_transform >> task_load