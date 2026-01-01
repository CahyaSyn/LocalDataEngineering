import sys
import os

sys.path.append(os.path.abspath("/home/cahya/Documents/PROJECT/LocalDataEngineer/etl"))

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from ingest_raw import ingest_raw
from bronze_transform import transform_bronze
from silver_transform import transform_silver
from data_quality_check import run_data_quality_checks
from gold_transform import transform_gold

default_args = {
    "owner": "cahya",
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="data_engineer_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-engineering"]
) as dag:

    raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=ingest_raw
    )

    bronze = PythonOperator(
        task_id="transform_bronze",
        python_callable=transform_bronze
    )

    silver = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver
    )

    quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality_checks
    )

    gold = PythonOperator(
        task_id="transform_gold",
        python_callable=transform_gold
    )

    raw >> bronze >> silver >> quality >> gold
