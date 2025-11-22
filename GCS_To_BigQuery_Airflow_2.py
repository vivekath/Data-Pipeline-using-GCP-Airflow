from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateDatasetOperator, BigQueryCreateTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'gcs_to_bigquery',
    default_args=default_args,
    description='A simple DAG to load data from GCS to BigQuery',
    schedule_interval='@daily',
)

create_dataset = BigQueryCreateDatasetOperator(
    task_id='create_dataset',
    dataset_id='my_dataset',
    location='US',
    dag=dag,
)

load_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_gcs_to_bq',
    bucket='gcs-bucket',
    source_objects=['path_to_file.csv'],
    destination_project_dataset_table='my_project.my_dataset.my_table',
    schema_fields=[
        {'name': 'ramesh', 'type': 'STRING'}
    ],
    write_disposition='WRITE_TRUNCATE',
    source_format='CSV',
    dag=dag,
)

create_dataset >> load_gcs_to_bq