from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.bigquery_to_mysql import BigQueryToMySqlOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from google.cloud import bigquery
import mysql.connector

# Initialize Spark Session
def get_spark_session():
    return SparkSession.builder \
        .appName("BigQuery_ETL") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.26.0") \
        .getOrCreate()

# Function: Transform data in BigQuery using PySpark
def transform_bigquery_data():
    spark = get_spark_session()
    table = "your_project.your_dataset.your_table"
    
    df = spark.read.format("bigquery").option("table", table).load()
    
    # Example Transformation (filter, rename column)
    df_transformed = df.filter(df.col1.isNotNull()).withColumnRenamed("col1", "new_col1")
    
    # Write back to BigQuery
    df_transformed.write.format("bigquery") \
        .option("table", "your_project.your_dataset.transformed_table") \
        .mode("overwrite") \
        .save()
    
    print("Transformation complete!")

# Function: Write BigQuery Data to Cloud SQL
def bigquery_to_cloud_sql():
    client = bigquery.Client()
    query = "SELECT * FROM `your_project.your_dataset.transformed_table`"
    df = client.query(query).to_dataframe()
    
    # Connect to Cloud SQL
    conn = mysql.connector.connect(
        host="your-cloud-sql-ip",
        user="your-user",
        password="your-password",
        database="your-database"
    )
    
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute("INSERT INTO your_table (col1, col2) VALUES (%s, %s)", (row['new_col1'], row['col2']))
    
    conn.commit()
    print("Data written to Cloud SQL!")

# Define Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "bigquery_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# Task 1: Wait for File in Cloud Storage
wait_for_file = GCSObjectExistenceSensor(
    task_id="wait_for_file",
    bucket="your-bucket-name",
    object="your-file.csv",
    google_cloud_conn_id="google_cloud_default",
    dag=dag,
)

# Task 2: Load Cloud Storage data into BigQuery
load_gcs_to_bigquery = GCSToBigQueryOperator(
    task_id="load_gcs_to_bigquery",
    bucket="your-bucket-name",
    source_objects=["your-file.csv"],
    destination_project_dataset_table="your_project.your_dataset.your_table",
    source_format="CSV",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    google_cloud_conn_id="google_cloud_default",
    dag=dag,
)

# Task 3: Transform BigQuery data using PySpark
transform_data = PythonOperator(
    task_id="transform_bigquery_data",
    python_callable=transform_bigquery_data,
    dag=dag,
)

# Task 4: Export BigQuery data to Cloud Storage
export_bigquery_to_gcs = BigQueryToGCSOperator(
    task_id="export_bigquery_to_gcs",
    source_project_dataset_table="your_project.your_dataset.transformed_table",
    destination_cloud_storage_uris=["gs://your-bucket-name/exported_data.csv"],
    export_format="CSV",
    print_header=True,
    google_cloud_conn_id="google_cloud_default",
    dag=dag,
)

# Task 5: Load transformed BigQuery data into Cloud SQL
load_bigquery_to_cloud_sql = PythonOperator(
    task_id="load_bigquery_to_cloud_sql",
    python_callable=bigquery_to_cloud_sql,
    dag=dag,
)

# Define Task Dependencies
wait_for_file >> load_gcs_to_bigquery >> transform_data
transform_data >> [export_bigquery_to_gcs, load_bigquery_to_cloud_sql]


"""
Below is a PySpark-based ETL pipeline integrating BigQuery, Cloud Storage, and Cloud SQL, orchestrated using Apache Airflow. This setup covers:

Extract:
. Cloud Storage → BigQuery
. Cloud SQL → BigQuery

Enter your email
Subscribe
2. Load & Transform:

BigQuery → DataFrame (PySpark Processing) → Write Back to BigQuery
3. Write:

BigQuery → Cloud Storage
BigQuery → Cloud SQL
1. Setup Requirements
Before running the Airflow DAG, ensure:

Airflow is installed (pip install apache-airflow[google])
PySpark is installed (pip install pyspark)
Cloud SDKs are installed (pip install google-cloud-bigquery google-cloud-storage mysql-connector-python)
Airflow Connections:
Set up Google Cloud connection (Google Cloud Connection ID: google_cloud_default)
Set up Cloud SQL connection (MySQL/PostgreSQL Connection ID: cloud_sql_default)

3. How It Works
Task 1: Waits for a new file in Cloud Storage.
Task 2: Loads the CSV file from Cloud Storage → BigQuery.
Task 3: Uses PySpark to process data in BigQuery (filter, rename, etc.).
Task 4: Exports the transformed data from BigQuery → Cloud Storage.
Task 5: Loads the transformed data from BigQuery → Cloud SQL.

Scaling Considerations
Use Google Dataflow instead of Airflow for real-time ETL.
Use Cloud Composer for large DAGs.
Configure Airflow Worker Autoscaling to handle bigger workloads.
"""