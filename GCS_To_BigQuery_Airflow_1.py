from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateDatasetOperator,
    BigQueryCreateTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Corrected typo: 'depends_on_past' instead of 'deoends_on_past'
    'start_date': datetime(2023, 1, 1),  # Corrected date format (YYYY, MM, DD)
    'retries': 1,
}

with DAG(
    'gcs_to_bigquery',
    default_args=default_args,
    description='A simple DAG to load data from GCS to BigQuery',
    schedule_interval='daily',
) as dag:

    create_dataset = BigQueryCreateDatasetOperator(
        task_id='CREATE_DATASET',
        dataset_id='my_dataset',
        location='us',
    )

    create_table = BigQueryCreateTableOperator(
        task_id='CREATE_TABLE',
        project_id='my_project',  # Added missing project_id
        dataset_id='my_dataset',  # Corrected typo: 'my-dataset' to 'my_dataset'
        table_id='my_table',
        schema_fields=[
            {'name': 'ramesh', 'type': 'STRING'},  # Corrected field type: 'STRING'
        ],
    )

    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='gcs-bucket',
        source_objects=['path/to/file.csv'],  # Corrected path format
        destination_table='my_project.my_dataset.my_table',  # Corrected full table path
        write_disposition='WRITE_TRUNCATE',  # Corrected capitalization and underscore
        source_format='CSV',
    )

    create_dataset >> create_table >> load_gcs_to_bq