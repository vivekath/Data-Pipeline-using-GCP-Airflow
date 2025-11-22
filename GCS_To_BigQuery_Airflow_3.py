from datetime import datetime
from airflow import DAG  # Use uppercase DAG

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateDatasetOperator,  # Use correct class name
    BigQueryCreateTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Consistent spelling
    'start_date': datetime(2023, 1, 1),  # Corrected date format
    'retries': 1,
}

with DAG(
    dag_id='gcs_to_bigquery',
    default_args=default_args,
    description='A simple DAG to load data from GCS to BigQuery',
    schedule_interval='daily',
) as dag:

    create_dataset = BigQueryCreateDatasetOperator(
        task_id='create_dataset',
        dataset_id='my_dataset',
        location='us',
    )

    create_table = BigQueryCreateTableOperator(
        task_id='create_table',
        dataset_id='my_dataset',  # Ensure consistency with BigQueryCreateDatasetOperator
        table_id='my_table',
        schema_fields=[
            {'name': 'ramesh', 'type': 'STRING'},  # Use uppercase STRING
        ],
    )

    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket='gcs-bucket',
        source_objects=['path_file.csv'],
        destination_table='my_project.my-dataset.my_table',  # Corrected table reference
        write_disposition='WRITE_TRUNCATE',  # Use uppercase WRITE_TRUNCATE
        source_format='CSV',
    )

    create_dataset >> create_table >> load_gcs_to_bq  # Set up task dependencies