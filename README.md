# 🚀 Fake Store ETL Pipeline - Data Engineering Project

A fully automated **Extract, Transform, Load (ETL)** pipeline that orchestrates data ingestion from the Fake Store API, applies comprehensive data quality checks, and persists data to AWS RDS PostgreSQL and S3. Powered by Apache Airflow, deployed on AWS EC2, and automated with GitHub Actions CI/CD.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Configuration](#configuration)
- [Usage](#usage)
- [Pipeline Structure](#pipeline-structure)
- [Data Quality Checks](#data-quality-checks)
- [AWS Infrastructure](#aws-infrastructure)
- [GitHub Actions CI/CD](#github-actions-cicd)
- [Database Schema](#database-schema)
- [Monitoring & Verification](#monitoring--verification)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)
- [Contributing](#contributing)

---

## 🎯 Project Overview

This project implements a production-ready ETL pipeline that demonstrates modern data engineering practices including:

- **Data Extraction**: Ingests product and user data from the Fake Store API
- **Data Transformation**: Normalizes, validates, and restructures data for analytics
- **Data Quality Assurance**: Implements comprehensive validation checks before loading
- **Multi-Target Loading**: Persists data to both PostgreSQL (OLAP) and S3 (Data Lake)
- **Orchestration**: Automated scheduling and monitoring via Apache Airflow
- **Cloud Infrastructure**: Leverages AWS services (RDS, S3, EC2)
- **CI/CD Automation**: GitHub Actions for automated deployment to EC2

This project is ideal for **learning data engineering** or as a **template for production pipelines**.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     FAKE STORE ETL PIPELINE                          │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│  Fake Store API  │
│  • Products      │
│  • Users         │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     EXTRACTION LAYER (main.py)                        │
│  • Fetches data from API endpoints                                    │
│  • Handles API errors and timeouts                                    │
│  • Returns DataFrames for downstream processing                       │
└────────┬─────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION LAYER (transform.py)                │
│  Products:                       Users:                               │
│  • Parse nested rating objects   • Extract nested names               │
│  • Rename columns for clarity    • Parse addresses                    │
│  • Type casting & validation     • Standardize phone numbers          │
│  • Select relevant fields        • Format names (capitalization)      │
└────────┬─────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────┐
│               DATA QUALITY CHECKS (quality.py)                        │
│  ✓ Null value validation in primary keys                              │
│  ✓ Uniqueness constraints on primary keys                             │
│  ✓ Price range validation (products must be > 0)                      │
│  ✓ Email validation (users must have email)                           │
│  ✓ DataFrame completeness checks                                      │
└────────┬─────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    LOAD LAYER (load.py)                               │
│                                                                        │
│  ┌────────────────────────┐    ┌─────────────────────────────┐       │
│  │  AWS RDS PostgreSQL    │    │   AWS S3 Data Lake          │       │
│  ├────────────────────────┤    ├─────────────────────────────┤       │
│  │ dim_products           │    │ s3://bucket/processed/      │       │
│  │ dim_users              │    │ • products.csv              │       │
│  │ (OLAP Database)        │    │ • users.csv                 │       │
│  └────────────────────────┘    └─────────────────────────────┘       │
└──────────────────────────────────────────────────────────────────────┘

Orchestration:
┌────────────────────────────────────────────────────────────────────────┐
│  Apache Airflow (EC2) - DAG: etl_pipeline                              │
│  • extract_data task → transform_data task → load_data task           │
│  • Schedule: Daily (@daily)                                            │
│  • Monitoring: Airflow Web UI                                          │
└────────────────────────────────────────────────────────────────────────┘

Deployment:
┌────────────────────────────────────────────────────────────────────────┐
│  GitHub Actions CI/CD Pipeline                                         │
│  Trigger: git push to main branch                                      │
│  Action: Deploy DAGs to EC2 via SCP                                    │
└────────────────────────────────────────────────────────────────────────┘
```

See [architecture_diagram.png](readme_assets/architecture_digram.png) for visual reference.

---

## ✨ Features

- ✅ **Automated Data Pipeline**: End-to-end ETL orchestrated with Apache Airflow
- ✅ **Robust Error Handling**: API timeout handling, database connection retries, validation errors
- ✅ **Data Quality Framework**: Comprehensive validation checks for data integrity
- ✅ **Multi-Destination Loading**: Write to PostgreSQL for OLAP and S3 for data lake
- ✅ **Cloud-Native**: Fully deployed on AWS (EC2, RDS, S3)
- ✅ **CI/CD Automation**: GitHub Actions for seamless deployment
- ✅ **Infrastructure as Code**: Pulumi for AWS resource provisioning
- ✅ **Monitoring**: Database query verification and pipeline status tracking
- ✅ **Scalable Architecture**: Ready for production workloads
- ✅ **Comprehensive Logging**: Detailed console output for debugging

---

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Language** | Python | 3.x |
| **Data Processing** | Pandas | Latest |
| **Orchestration** | Apache Airflow | Latest |
| **Database** | PostgreSQL (AWS RDS) | 12+ |
| **Data Storage** | AWS S3 | - |
| **Compute** | AWS EC2 | - |
| **Containerization** | Docker | Latest |
| **IaC** | Pulumi (Python SDK) | Latest |
| **CI/CD** | GitHub Actions | - |
| **ORM/Connection** | SQLAlchemy | Latest |
| **AWS SDK** | Boto3 | Latest |
| **API Client** | Requests | Latest |
| **DB Driver** | psycopg2 | Latest |

---

## 📋 Prerequisites

Before running this project, ensure you have:

### Local Development Environment
- **Python 3.8+** installed
- **Git** for version control
- **Docker** for containerization
- **Virtual Environment** support (venv)

### AWS Account
- **AWS Account** with appropriate permissions
- **RDS PostgreSQL Instance** (or create one during setup)
- **S3 Bucket** for data storage
- **EC2 Instance** for Airflow deployment
- **IAM User** with programmatic access (Access Key ID & Secret Access Key)

### AWS Permissions Required
```
- rds:*
- s3:*
- ec2:*
- iam:*
```

### GitHub
- **GitHub Repository** with this code
- **GitHub Secrets** configured for AWS credentials and EC2 details

---

## 🚀 Installation & Setup

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/fake-store-etl-pipeline.git
cd fake-store-etl-pipeline
```

### Step 2: Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Create .env File

Create a `.env` file in the project root:

```bash
cp .env.example .env
```

### Step 5: Configure Environment Variables

Edit `.env` with your AWS and database credentials:

```bash
# API Configuration
API_URL=https://fakestoreapi.com

# PostgreSQL Database Configuration
DB_HOST=your-rds-endpoint.amazonaws.com
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=postgres
DB_PASSWORD=your_secure_password

# AWS S3 Configuration
S3_BUCKET_NAME=your-s3-bucket-name
AWS_REGION=us-east-1

# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

---

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `API_URL` | Fake Store API base URL | `https://fakestoreapi.com` |
| `DB_HOST` | PostgreSQL RDS endpoint | `my-db.xxxxx.us-east-1.rds.amazonaws.com` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_NAME` | Database name | `etl_database` |
| `DB_USER` | Database username | `postgres` |
| `DB_PASSWORD` | Database password | `SecurePassword123!` |
| `S3_BUCKET_NAME` | S3 bucket name | `my-etl-bucket` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `AKIAIOSFODNN7EXAMPLE` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |

### Airflow Configuration

The DAG is scheduled to run **daily** starting from January 1, 2026:

```python
schedule_interval='@daily'
start_date=datetime(2026, 1, 1)
catchup=False  # Don't backfill past dates
```

**To modify schedule**: Edit [dags/etl_pipeline.py](dags/etl_pipeline.py)

---

## 💻 Usage

### Option 1: Run Locally (Development)

```bash
# Activate virtual environment
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Run the ETL pipeline
python main.py

# Expected Output:
# === Starting Fake Store ETL Pipeline ===
# [1/4] Extracting data from Fake Store API...
# [2/4] Transforming extracted data...
# [3/4] Performing Data Quality Checks...
# [4/4] Loading clean data into PostgreSQL database...
# === ETL Pipeline Completed Successfully! ===
```

### Option 2: Verify Database Contents

After running the pipeline, verify data in PostgreSQL:

```bash
python check_db.py

# Output:
# --- DIM_PRODUCTS TABLE ---
# (First 5 rows of products)
#
# --- DIM_USERS TABLE ---
# (First 5 rows of users)
```

### Option 3: Run with Apache Airflow (Production)

On your EC2 instance with Airflow installed:

```bash
# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start Airflow Web Server
airflow webserver --port 8080

# Start Airflow Scheduler (in another terminal)
airflow scheduler

# Access Airflow UI at: http://your-ec2-ip:8080
```

---

## 📊 Pipeline Structure

### Main Pipeline Flow

```
main.py (Entry Point)
│
├── 1. EXTRACT PHASE
│   ├── extract_products()  → Fetch from /products endpoint
│   ├── extract_users()     → Fetch from /users endpoint
│   └── Return: Raw DataFrames
│
├── 2. TRANSFORM PHASE
│   ├── transform_products()
│   │   ├── Parse nested rating (rate, count)
│   │   ├── Rename columns (id → product_id, title → product_name, etc.)
│   │   ├── Type casting (int, float)
│   │   └── Select relevant columns
│   │
│   └── transform_users()
│       ├── Extract nested name (firstname, lastname)
│       ├── Parse nested address (street, city)
│       ├── Format names (capitalize)
│       ├── Rename columns (id → user_id, email → user_email, etc.)
│       └── Select relevant columns
│
├── 3. QUALITY CHECK PHASE
│   ├── check_products_quality()
│   │   ├── Check empty DataFrame
│   │   ├── Check product_id nulls
│   │   ├── Check product_id duplicates
│   │   └── Check price > 0
│   │
│   └── check_users_quality()
│       ├── Check empty DataFrame
│       ├── Check user_id nulls
│       ├── Check user_id duplicates
│       └── Check email not null
│
└── 4. LOAD PHASE
    ├── load_products()
    │   ├── Write to PostgreSQL (dim_products table)
    │   └── Upload to S3 (processed/products.csv)
    │
    └── load_users()
        ├── Write to PostgreSQL (dim_users table)
        └── Upload to S3 (processed/users.csv)
```

### File Organization

| File | Purpose |
|------|---------|
| `main.py` | Pipeline orchestration and entry point |
| `check_db.py` | Database verification utility |
| `etl/extract.py` | Data extraction from APIs |
| `etl/transform.py` | Data cleaning and transformation |
| `etl/quality.py` | Data quality validation checks |
| `etl/load.py` | Data loading to PostgreSQL and S3 |
| `dags/etl_pipeline.py` | Apache Airflow DAG definition |
| `.env` | Environment variables (create locally) |
| `requirements.txt` | Python dependencies |

---

## ✅ Data Quality Checks

### Products Table Validation

```python
def check_products_quality(df):
    ✓ DataFrame not empty
    ✓ No null values in product_id (Primary Key)
    ✓ No duplicate product_id values
    ✓ All prices > 0 (logical validation)
```

**Failure Behavior**: Raises `ValueError` if any check fails, preventing pipeline progression.

### Users Table Validation

```python
def check_users_quality(df):
    ✓ DataFrame not empty
    ✓ No null values in user_id (Primary Key)
    ✓ No duplicate user_id values
    ✓ All users have email addresses
```

**Failure Behavior**: Raises `ValueError` if any check fails, preventing pipeline progression.

### Example: Running Quality Checks

```python
# If a check fails:
ValueError: Data Quality Error: Duplicate 'product_id' values found!

# Pipeline stops → No data loaded to database
# Check logs → Fix upstream issues → Retry
```

---

## ☁️ AWS Infrastructure

### AWS RDS PostgreSQL

**Table: `dim_products`**
```sql
CREATE TABLE dim_products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    product_price FLOAT,
    product_category VARCHAR(100),
    product_description TEXT,
    rating_rate FLOAT,
    rating_count INTEGER
);
```

**Table: `dim_users`**
```sql
CREATE TABLE dim_users (
    user_id INTEGER PRIMARY KEY,
    full_name VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    user_email VARCHAR(255) NOT NULL,
    user_username VARCHAR(100),
    user_phone VARCHAR(20),
    street VARCHAR(255),
    city VARCHAR(100)
);
```

### AWS S3 Bucket Structure

```
s3://your-bucket-name/
├── processed/
│   ├── products.csv
│   └── users.csv
```

### AWS EC2 Deployment

**Instance Configuration**:
- **AMI**: Ubuntu 20.04+ or Amazon Linux 2
- **Instance Type**: t2.medium or larger (for Airflow)
- **Storage**: 30GB+ EBS volume
- **Security Group**: Allow inbound on port 8080 (Airflow UI) and SSH (22)

**Software Stack**:
- Python 3.x
- Docker
- Docker Compose
- Apache Airflow
- PostgreSQL Client

---

## 🔄 GitHub Actions CI/CD

### Automated Deployment Pipeline

**Trigger**: `git push` to `main` branch

**Workflow File**: [.github/workflows/deploy_dags.yml](.github/workflows/deploy_dags.yml)

```yaml
Jobs:
1. Checkout Code
2. Deploy DAGs to EC2 via SCP
   - Copies dags/* to ~/airflow/dags
   - Triggers Airflow DAG reload
```

### GitHub Secrets Configuration

Set these secrets in your GitHub repository settings (Settings → Secrets):

```
EC2_HOST          = your-ec2-public-ip.compute.amazonaws.com
EC2_USERNAME      = ec2-user (or ubuntu for Ubuntu AMI)
EC2_SSH_KEY       = (private SSH key in PEM format)
```

### Deploy to EC2

```bash
# GitHub Actions automatically deploys on:
git add .
git commit -m "Update DAGs"
git push origin main

# Watch deployment: GitHub → Actions → deploy_dags workflow
```

---

## 📊 Database Schema

### Products Dimension Table

```
dim_products
├── product_id (INT) - Primary Key
├── product_name (VARCHAR)
├── product_price (FLOAT)
├── product_category (VARCHAR)
├── product_description (TEXT)
├── rating_rate (FLOAT) - Average rating
└── rating_count (INT) - Number of ratings
```

### Users Dimension Table

```
dim_users
├── user_id (INT) - Primary Key
├── full_name (VARCHAR)
├── first_name (VARCHAR)
├── last_name (VARCHAR)
├── user_email (VARCHAR) - NOT NULL
├── user_username (VARCHAR)
├── user_phone (VARCHAR)
├── street (VARCHAR)
└── city (VARCHAR)
```

---

## 🔍 Monitoring & Verification

### Check Pipeline Execution Logs

**Local Execution**:
```bash
# Run pipeline and watch console output
python main.py

# Expected successful output:
# === Starting Fake Store ETL Pipeline ===
# [1/4] Extracting data from Fake Store API...
# Raw products: 20 records, Raw users: 10 records
# [2/4] Transforming extracted data...
# Transformed products: 20 records
# Transformed users: 10 records
# [3/4] Performing Data Quality Checks...
# Products Data Quality Checks Passed!
# Users Data Quality Checks Passed!
# [4/4] Loading clean data into PostgreSQL database...
# Successfully loaded products into PostgreSQL!
# Successfully loaded products.csv to S3 Bucket!
# Successfully loaded users into PostgreSQL!
# Successfully loaded users.csv to S3 Bucket!
# === ETL Pipeline Completed Successfully! ===
```

### Verify Database Contents

**After successful pipeline run**:
```bash
python check_db.py

# Output:
# --- DIM_PRODUCTS TABLE ---
#    product_id      product_name  ...  rating_rate  rating_count
# 0           1     Fjallraven ...      3.9            109
# 1           2  Mens Casual ...      2.6             48
# ...
#
# --- DIM_USERS TABLE ---
#    user_id   full_name  ...  user_email        city
# 0       1   John Doe  ...  john@example.com  kilcoole
# 1       2   Jane Smith ...  jane@example.com  frankfurt
```

### Monitor Airflow Execution

**Access Airflow Web UI**:
```
http://your-ec2-ip:8080
```

Screenshots available:
- [airflow_dag_main_ui.png](readme_assets/airflow_dag_main_ui.png) - DAG overview
- [airflow_dag_ui_graph.png](readme_assets/airflow_dag_ui_graph.png) - Task graph
- [airflow_ui_dag_grid_view.png](readme_assets/airflow_ui_dag_grid_view.png) - Grid view

### Monitor S3 Uploads

**AWS Console**:
```
S3 → your-bucket-name → processed/ → verify CSV files exist
```

Reference: [inside_of_s3_bucket_image_01.png](readme_assets/inside_of_s3_bucket_image_01.png)

### Monitor Database via Python

**Query database directly**:
```python
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Check record counts
products_count = pd.read_sql("SELECT COUNT(*) FROM dim_products;", engine)
users_count = pd.read_sql("SELECT COUNT(*) FROM dim_users;", engine)

print(f"Products: {products_count.iloc[0, 0]} records")
print(f"Users: {users_count.iloc[0, 0]} records")
```

Reference: [database_view_using_python_script.png](readme_assets/database_view_using_python_script.png)

---

## 🔧 Troubleshooting

### Issue 1: API Connection Error

**Error**: `Error extracting products: Connection timeout`

**Solutions**:
```bash
# 1. Check internet connectivity
ping fakestoreapi.com

# 2. Verify API is accessible
curl https://fakestoreapi.com/products

# 3. Check .env API_URL is correct
grep API_URL .env

# 4. Increase timeout in extract.py
# Change: timeout=10 to timeout=30
```

### Issue 2: PostgreSQL Connection Failed

**Error**: `could not connect to server: No such file or directory`

**Solutions**:
```bash
# 1. Verify RDS endpoint is accessible
psql -h your-rds-endpoint -U postgres -d postgres

# 2. Check .env database configuration
cat .env | grep DB_

# 3. Verify security group allows port 5432
# AWS Console → EC2 → Security Groups → Inbound Rules

# 4. Check PostgreSQL server is running
# AWS Console → RDS → Instances → Check Status

# 5. Verify credentials
echo "SELECT version();" | psql -h $DB_HOST -U $DB_USER -d $DB_NAME
```

### Issue 3: S3 Upload Fails

**Error**: `An error occurred (NoCredentialsError) when calling the PutObject operation`

**Solutions**:
```bash
# 1. Verify AWS credentials in .env
grep AWS_ .env

# 2. Check IAM user has S3 permissions
# AWS Console → IAM → Users → Check Policies

# 3. Verify S3 bucket exists
aws s3 ls s3://your-bucket-name/

# 4. Check bucket name is correct
grep S3_BUCKET_NAME .env

# 5. Verify bucket region in .env
grep AWS_REGION .env
```

### Issue 4: Data Quality Check Fails

**Error**: `ValueError: Data Quality Error: Duplicate 'product_id' values found!`

**Solutions**:
```bash
# 1. Check source data for duplicates
curl https://fakestoreapi.com/products | jq '.[] | .id' | sort | uniq -d

# 2. Inspect raw data before transformation
# Add debugging in transform.py:
print("Raw data shape:", products_df.shape)
print("Unique product IDs:", products_df['id'].nunique())

# 3. Check for data corruption during transformation
# Verify column renaming logic in transform.py

# 4. Clear and retry pipeline
python main.py
```

### Issue 5: Airflow DAG Not Appearing

**Error**: `DAG not visible in Airflow UI`

**Solutions**:
```bash
# 1. Verify DAG file location
ls -la ~/airflow/dags/

# 2. Check DAG syntax for errors
python ~/airflow/dags/etl_pipeline.py

# 3. Restart Airflow scheduler
airflow scheduler restart

# 4. Check Airflow logs
tail -f ~/airflow/logs/etl_pipeline/

# 5. Reload DAG from UI
# Click "Refresh" button or restart webserver
```

---

## 📁 Project Structure

```
fake-store-etl-pipeline/
│
├── 📄 main.py                      # Main pipeline orchestrator
├── 📄 check_db.py                  # Database verification utility
├── 📄 requirements.txt              # Python dependencies
├── 📄 .env                          # Environment variables (CREATE THIS)
├── 📄 .gitignore                    # Git ignore rules
├── 📄 README.md                     # This file
│
├── 📁 etl/                          # ETL module
│   ├── __init__.py
│   ├── extract.py                  # Data extraction functions
│   ├── transform.py                # Data transformation logic
│   ├── quality.py                  # Data quality checks
│   └── load.py                     # Data loading functions
│
├── 📁 dags/                         # Apache Airflow DAGs
│   └── etl_pipeline.py             # Main DAG definition
│
├── 📁 .github/                      # GitHub configuration
│   └── workflows/
│       └── deploy_dags.yml         # GitHub Actions CI/CD pipeline
│
└── 📁 readme_assets/               # Documentation assets
    ├── architecture_digram.png
    ├── airflow_dag_main_ui.png
    ├── airflow_dag_ui_graph.png
    ├── airflow_ui_dag_grid_view.png
    ├── aws_postgreSQL_db.png
    ├── database_view_using_python_script.png
    ├── databse_monitoring_01.png
    ├── databse_monitoring_02.png
    ├── databse_monitoring_03.png
    ├── docker_images_on_ec2.png
    ├── ec2_instance.png
    ├── ETL_pipeline_successfull.png
    ├── github_deployment_ec2_github_actions.png
    ├── github_secretes_keys.png
    ├── inside_of_s3_bucket_image_01.png
    ├── inside_of_s3_bucket_image_02.png
    ├── S3_bucket_for_data_store.png
    └── security_group_aws.png
```

---

## 👨‍💻 Code Examples

### Example 1: Running the ETL Pipeline

```python
# main.py
from etl.extract import extract_products, extract_users
from etl.transform import transform_products, transform_users
from etl.quality import check_products_quality, check_users_quality
from etl.load import load_products, load_users

# Extract
raw_products = extract_products()  # Calls API
raw_users = extract_users()

# Transform
clean_products = transform_products(raw_products)
clean_users = transform_users(raw_users)

# Validate
check_products_quality(clean_products)
check_users_quality(clean_users)

# Load
load_products(clean_products)  # PostgreSQL + S3
load_users(clean_users)
```

### Example 2: Querying Results

```python
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Query products
products = pd.read_sql(
    "SELECT * FROM dim_products ORDER BY rating_rate DESC LIMIT 10;",
    engine
)

# Query users by city
users_by_city = pd.read_sql(
    "SELECT city, COUNT(*) as user_count FROM dim_users GROUP BY city;",
    engine
)
```

---

## 🚀 Next Steps & Future Enhancements

- [ ] Add data lineage tracking (with OpenLineage/Marquez)
- [ ] Implement incremental loading (CDC)
- [ ] Add data profiling and statistics
- [ ] Implement schema validation (Great Expectations)
- [ ] Add Slack notifications for failures
- [ ] Create data quality dashboards (Grafana/Tableau)
- [ ] Implement partition by date for S3
- [ ] Add cost optimization (S3 Intelligent-Tiering)
- [ ] Implement Spark jobs for large-scale processing
- [ ] Add API rate limiting and caching

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Coding Standards
- Follow PEP 8 Python style guide
- Add docstrings to all functions
- Include error handling for edge cases
- Test locally before pushing
- Update README for new features

---

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 📞 Support & Questions

For issues, questions, or suggestions:
1. **GitHub Issues**: Open an issue on the repository
2. **Documentation**: Refer to inline code comments
3. **Logs**: Check pipeline logs for debugging

---

## 🎓 Learning Resources

- **Apache Airflow**: [Official Documentation](https://airflow.apache.org/)
- **AWS RDS PostgreSQL**: [AWS RDS Documentation](https://docs.aws.amazon.com/rds/)
- **Pandas**: [Pandas Documentation](https://pandas.pydata.org/)
- **ETL Best Practices**: [Data Engineering Articles](https://www.dataengineer.io/)

---

## 📸 Project Proof & Visual Evidence

This section contains **ALL 19 screenshots and images** demonstrating a fully functional, production-ready ETL pipeline deployment across AWS, Airflow, and GitHub CI/CD infrastructure.

---

### 🏗️ Architecture & Design

#### System Architecture Diagram
Complete end-to-end architecture showing data flow from API to databases and data lake.

![Architecture Diagram](readme_assets/architecture_digram.png)

---

### ✅ Pipeline Execution & Success

#### Successful ETL Pipeline Run
Evidence of successful pipeline execution with all 4 phases completing without errors.

![ETL Pipeline Success](readme_assets/ETL_pipeline_successfull.png)

---

### 🔄 Apache Airflow Orchestration (3 Images)

#### 1. Airflow DAG Main UI
Main dashboard showing the etl_pipeline DAG overview with execution history and status.

![Airflow DAG Main UI](readme_assets/airflow_dag_main_ui.png)

#### 2. Airflow DAG Graph View
Task dependency graph showing the flow: extract_data → transform_data → load_data.

![Airflow DAG Graph](readme_assets/airflow_dag_ui_graph.png)

#### 3. Airflow Grid View
Grid view showing multiple DAG runs with execution timeline and task status for each run.

![Airflow Grid View](readme_assets/airflow_ui_dag_grid_view.png)

---

### ☁️ AWS Infrastructure Deployment (4 Images)

#### 1. EC2 Instance Configuration
AWS EC2 instance running Airflow scheduler and webserver for pipeline orchestration.

![EC2 Instance](readme_assets/ec2_instance.png)

#### 2. AWS RDS PostgreSQL Database
AWS Relational Database Service (RDS) PostgreSQL instance hosting dim_products and dim_users tables.

![AWS RDS PostgreSQL](readme_assets/aws_postgreSQL_db.png)

#### 3. AWS Security Group Configuration
Security group rules allowing inbound traffic on port 8080 (Airflow UI) and 5432 (PostgreSQL).

![Security Group](readme_assets/security_group_aws.png)

#### 4. S3 Bucket for Data Lake
AWS S3 bucket configured as data lake for processed CSV files from ETL pipeline.

![S3 Bucket](readme_assets/S3_bucket_for_data_store.png)

---

### 💾 Data Lake & S3 Storage (2 Images)

#### 1. S3 Bucket Contents - Part 1
Inside view of S3 bucket showing processed/products.csv and processed/users.csv files.

![S3 Bucket Contents 1](readme_assets/inside_of_s3_bucket_image_01.png)

#### 2. S3 Bucket Contents - Part 2
Detailed view of processed directory contents with file sizes and upload timestamps.

![S3 Bucket Contents 2](readme_assets/inside_of_s3_bucket_image_02.png)

---

### 🗄️ Database Verification & Monitoring (4 Images)

#### 1. Python Database Query Results
Output from check_db.py showing DIM_PRODUCTS and DIM_USERS table contents verification.

![Database View via Python](readme_assets/database_view_using_python_script.png)

#### 2. Database Monitoring - Metrics 1
RDS CloudWatch metrics showing CPU utilization and database performance over time.

![Database Monitoring 1](readme_assets/databse_monitoring_01.png)

#### 3. Database Monitoring - Metrics 2
RDS metrics showing network throughput and connection counts during pipeline execution.

![Database Monitoring 2](readme_assets/databse_monitoring_02.png)

#### 4. Database Monitoring - Metrics 3
RDS storage metrics showing disk usage and IOPS performance after data load.

![Database Monitoring 3](readme_assets/databse_monitoring_03.png)

---

### 🐳 Docker & Container Deployment

#### Docker Images on EC2
Docker containers running Airflow webserver and scheduler services on EC2 instance.

![Docker Images on EC2](readme_assets/docker_images_on_ec2.png)

---

### 🚀 GitHub Actions CI/CD Deployment (2 Images)

#### 1. GitHub Actions Deployment Pipeline
GitHub Actions workflow executing deploy_dags.yml to automatically deploy DAGs to EC2 via SCP.

![GitHub Actions Deployment](readme_assets/github_deployment_ec2_github_actions.png)

#### 2. GitHub Secrets Configuration
GitHub repository secrets storing EC2_HOST, EC2_USERNAME, and EC2_SSH_KEY for secure deployment.

![GitHub Secrets](readme_assets/github_secretes_keys.png)

---

### 📊 Evidence Summary

| Component | Image Count | Status |
|-----------|------------|--------|
| Architecture & Design | 1 | ✅ Documented |
| Pipeline Execution | 1 | ✅ Successful |
| Airflow Orchestration | 3 | ✅ Running |
| AWS Infrastructure | 4 | ✅ Deployed |
| Data Lake (S3) | 2 | ✅ Active |
| Database Monitoring | 4 | ✅ Monitored |
| Docker Deployment | 1 | ✅ Running |
| GitHub CI/CD | 2 | ✅ Automated |
| **TOTAL** | **18** | **✅ COMPLETE** |

**All 19 images integrated into README for complete project documentation and proof of implementation.**

---

## 🎉 Congratulations!

You now have a production-ready ETL pipeline. Start by following the [Installation & Setup](#installation--setup) section to get up and running!

**Happy Data Engineering! 🚀**

---

**Last Updated**: September 2026
**Version**: 1.0.0
**Author**: Data Engineering Team

