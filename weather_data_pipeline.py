import requests
import json

def fetch_weather_data():
    url = "http://api.weatherapi.com/v1/current.json"
    params = {
        "key": "<your_api_key>",
        "q": "Delhi",
        "aqi": "no"
    }
    response = requests.get(url, params=params)
    data = response.json()
    file_path = "/tmp/weather_raw.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
    print("Weather data fetched and saved locally.")

"""
{
  "location": {
    "name": "Delhi",
    "region": "Delhi",
    "country": "India",
    "lat": 28.67,
    "lon": 77.22,
    "tz_id": "Asia/Kolkata",
    "localtime": "2025-05-30 11:00"
  },
  "current": {
    "temp_c": 38.0,
    "condition": {
      "text": "Sunny"
    },
    "wind_kph": 12.2,
    "humidity": 24
  }
}
"""
from google.cloud import storage

def upload_to_gcs():
    client = storage.Client()
    bucket = client.get_bucket("weather-data-lake")
    blob = bucket.blob("weather_raw.json")
    blob.upload_from_filename("/tmp/weather_raw.json")
    print("Uploaded weather data to GCS.")

import pandas as pd

def process_weather_data():
    with open("/tmp/weather_raw.json", "r") as f:
        raw = json.load(f)
    df = pd.json_normalize(raw)
    df = df[["location.name", "location.region", "current.temp_c", "current.humidity", "current.wind_kph"]]
    df.columns = ["city", "region", "temp_c", "humidity", "wind_kph"]
    df.to_csv("/tmp/weather_clean.csv", index=False)
    print("Weather data processed.")

from google.cloud import bigquery

def load_to_bigquery():
    client = bigquery.Client()
    table_id = "<project_id>.weather_dataset.weather_raw"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    with open("/tmp/weather_clean.csv", "rb") as source_file:
        client.load_table_from_file(source_file, table_id, job_config=job_config).result()
    print("Weather data loaded into BigQuery.")

def transform_weather_data():
    client = bigquery.Client()
    query = """
        CREATE OR REPLACE TABLE `weather_dataset.daily_summary` AS
        SELECT
            city,
            AVG(temp_c) AS avg_temp,
            AVG(humidity) AS avg_humidity,
            MAX(wind_kph) AS max_wind
        FROM `weather_dataset.weather_raw`
        GROUP BY city
    """
    client.query(query).result()
    print("Transformed data into analytics table.")

import datetime
from airflow import models
from airflow.operators.python import PythonOperator

default_args = {
    "start_date": datetime.datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
}

# This is old code for Airflow DAG
with models.DAG(
    "weather_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    process_task = PythonOperator(
        task_id="process_weather_data",
        python_callable=process_weather_data,
    )

    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    transform_task = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data,
    )

    fetch_task >> upload_task >> process_task >> load_task >> transform_task