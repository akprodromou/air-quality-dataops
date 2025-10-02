from pathlib import Path
from airflow import DAG
# PythonOperator is the built-in Airflow operator used for running arbitrary Python functions
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Project root inside the container, i.e. air-quality-dataops <- orchestration
project_root = Path(__file__).parent.parent

# Import the visualization function
from visualize_air_quality import generate_visualizations

# Trigger the execution of the ingest_openaq_data.py script (located in another file)
# This fetches raw data from OpenAQ and saves it to ingestion/raw_data
def ingest_openaq():
    subprocess.run(
        ["python3", str(project_root / "ingestion/ingest_openaq_data.py")],
        # prevent the main program from silently continuing after a critical failure in ingestion
        check=True
    )

def ingest_weather_data():
    subprocess.run(
        ["python3", str(project_root / "ingestion/ingest_weather_data.py")],
        # prevent the main program from silently continuing after a critical failure in ingestion
        check=True
    )

# Runs DBT transformations
# stg_ingested_openaq_data: runs first and converts raw JSON to flattened table (i.e. dumps the data in the warehouse)
# stg_openaq_data cleans, standarizes column names, to make it queryable and understandable
# analysis_air_quality (marts layer) chooses attributes to keep and performs deduplication
def run_dbt():
    subprocess.run(
        [
            "dbt", "run",
            # tell dbt where dbt_project.yml is located
            "--project-dir", str(project_root / "transformation/aq_weather_dbt"),
            # tell dbt where profiles.yml is located
            "--profiles-dir", str(project_root / "transformation/aq_weather_dbt")
        ],
        check=True
    )

# DAG definition

# default_args is a dictionary of parameters that define default behavior for all tasks in our DAG
default_args = {
    # Error handling for airflow tasks, as a precaution
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
}

with DAG(
    dag_id="air_quality_pipeline",
    description="Airflow DAG to ingest OpenAQ, Open Weather data, run DBT, and generate visualizations",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    default_args=default_args,
    # only start running for the next scheduled interval and ingore missed past ones
    catchup=False,
    tags=["air_quality", "weather", "dataops"]
) as dag:

    # Task definitions
    ingest_aq_data_task = PythonOperator(
        task_id="ingest_openaq_data",
        python_callable=ingest_openaq,
    )

    ingest_weather_data_task = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=ingest_weather_data,
    )

    dbt_task = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt,
    )

    task_visualizations = PythonOperator(
        task_id='generate_visualizations',
        python_callable=generate_visualizations,
        op_kwargs={"output_dir": "/opt/airflow/visualizations"},
    )

    # Task dependencies
    ingest_aq_data_task >> ingest_weather_data_task >> dbt_task >> task_visualizations