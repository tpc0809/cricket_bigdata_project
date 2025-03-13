# 🏏 Cricket Big Data ML Pipeline

🚀 **Project Workflow**

1. **Dataset Storage & Management** → Store dataset in MinIO (S3-compatible).
2. **Batch Processing with Apache Spark** → Read and transform data before ingestion into PostgreSQL.
3. **Data Ingestion with Apache Airflow** → Automate the loading of cleaned data from MinIO to PostgreSQL.
4. **Real-time Processing (Upcoming)** → Stream and process data using Apache Flink & Kafka.
5. **Machine Learning Pipeline (Upcoming)** → Train models using Scikit-learn or TensorFlow.
6. **Data Validation with Great Expectations (Upcoming)** → Ensure data integrity before processing.
7. **Deployment & Monitoring (Upcoming)** → Deploy services using Terraform, Kubernetes, Prometheus, Grafana.

---

## ✅ Step 1: Project Initialization

- **Set up directory structure.**
- **Configure Git for version control.**
- **Create a Python virtual environment.**
- **Install dependencies (pip, airflow, pyspark, etc.).**
- **Set up Docker & Docker Compose.**
- **Configure PostgreSQL and MinIO.**

### 📌 Project Structure:

```
cricket-big-data-ml-pipeline/
│── airflow/
│── config/
│── dags/
│── Dataset Generation/
│── dbt-models/
│── docker/
│── fastapi/
│── flink-scripts/
│── jars/
│── kafka/
│── logs/
│── minio/
│── ml-pipeline/
│── monitoring/
│── notebooks/
│── postgres/
│── spark/
│── spark-scripts/
│── terraform/
│── validation/
│── visualizations/
│── cricket_dataset.parquet
│── docker-compose.yml
│── Dockerfile
│── README.md
│── requirements.txt
```

### 📌 Initialize the Project:

```bash
# Create and navigate to the project directory
mkdir cricket-big-data-ml-pipeline && cd cricket-big-data-ml-pipeline

# Initialize Git
git init

# Create a Python virtual environment
python3 -m venv venv
source venv/bin/activate  # For MacOS/Linux users

# Install dependencies
pip install --upgrade pip
pip install apache-airflow minio pyspark pandas psycopg2-binary boto3 great_expectations dbt-core scikit-learn matplotlib seaborn

# Save installed dependencies
pip freeze > requirements.txt
```

### 📌 Verify Docker & Launch Services:

```bash
# Check if Docker is installed
docker --version
docker-compose --version

# Start MinIO & PostgreSQL containers
docker-compose up -d
```

---

## ✅ Step 2: Data Storage & Ingestion (Completed)

- **Loaded raw cricket dataset into MinIO (as Parquet).**
- **Developed an Apache Airflow DAG to process & load data from MinIO to PostgreSQL.**
- **Ensured deduplication, state tracking, and batch processing.**
- **Confirmed 111,111,111 unique records are correctly loaded into PostgreSQL.**
- **Reindexed tables for better performance.**

---

## ✅ Step 3: Data Partitioning & Indexing (Current Step)

- **Partition `cricket_data` by Country.**
- **Create necessary indexes on Player Name, Batting Average, Bowling Wickets, etc.**
- **Validate query performance & data integrity.**

### 📌 Partitioning Data in PostgreSQL:

```sql
CREATE TABLE cricket_data_partitioned (
    player_name TEXT,
    country TEXT,
    runs INT,
    batting_average FLOAT
) PARTITION BY LIST (country);
```

### 📌 Creating Indexes for Faster Queries:

```sql
CREATE INDEX idx_player_name ON cricket_data_partitioned (player_name);
CREATE INDEX idx_batting_avg ON cricket_data_partitioned (batting_average);
CREATE INDEX idx_bowling_wickets ON cricket_data_partitioned (bowling_wickets);
```

### 📌 Validating Query Performance:

```sql
EXPLAIN ANALYZE SELECT * FROM cricket_data_partitioned WHERE country = 'India';
```

---

📏 **Next Steps After This**

### 🔹 Step 4: Real-time Processing with Apache Flink & Kafka
- **Simulate real-time data ingestion with Kafka producers.**
- **Use Apache Flink to process and transform incoming data streams.**

```python
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('cricket_scores', b'{"player":"Virat Kohli", "runs":85}')
```

### 🔹 Step 5: Data Validation with Great Expectations
- **Define validation rules to ensure data quality before further processing.**
- **Automate validation in the Airflow pipeline.**

```python
from great_expectations.dataset import PandasDataset
dataset = PandasDataset(df)
dataset.expect_column_values_to_be_between("Batting Average", min_value=0, max_value=100)
```

### 🔹 Step 6: Machine Learning Pipeline
- **Train ML models (Scikit-Learn/TensorFlow).**
- **Deploy models using FastAPI for real-time inference.**

```python
from sklearn.ensemble import RandomForestRegressor
model = RandomForestRegressor()
model.fit(X_train, y_train)
```

### 🔹 Step 7: Infrastructure Automation (Terraform & Kubernetes)
- **Deploy PostgreSQL, MinIO, Airflow, and Kafka on Kubernetes.**
- **Automate infrastructure provisioning using Terraform.**

### 🔹 Step 8: CI/CD & Monitoring
- **Set up GitHub Actions for automated testing & deployment.**
- **Use Grafana & Prometheus for real-time monitoring.**
- **Create dashboards with Streamlit or Dash for visualization.**

---

📚 **Current Status & Next Steps**

✅ **Completed Steps:**
- **Project setup & dependency installation.**
- **MinIO & PostgreSQL configuration.**
- **Batch processing using Apache Spark.**
- **ETL automation with Apache Airflow.**
- **Data loaded into PostgreSQL.**

🚀 **Next Steps:**
- **Step 3:** Continue optimizing PostgreSQL with partitioning & indexing.
- **Step 4:** Real-time Processing with Apache Flink & Kafka.
- **Step 5:** Data Validation with Great Expectations.
- **Step 6:** Machine Learning Pipeline.
- **Step 7:** Deployment & Monitoring.

---

🏏 **Stay tuned for more updates! 🚀**

