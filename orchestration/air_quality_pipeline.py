from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Define Python functions for tasks

def ingest_openaq():
    """
    Runs the existing ingest_openaq_data.py script.
    This will fetch raw data from OpenAQ and save it to ingestion/raw_data.
    """
    subprocess.run(
        ["python3", "/opt/airflow/ingestion/ingest_openaq_data.py"],
        check=True
    )

def run_dbt():
    subprocess.run(
        [
            "dbt", "run",
            "--project-dir", "/opt/airflow/transformation/air_quality_dbt",
            "--profiles-dir", "/opt/airflow/transformation/air_quality_dbt",
            "--models", "stg_ingested_openaq_data stg_openaq_data analysis_air_quality"
        ],
        check=True
    )


# Define the DAG

with DAG(
    dag_id="air_quality_pipeline",
    description="Airflow DAG to ingest OpenAQ data and run DBT transformations",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["air_quality", "dataops"]
) as dag:

    # Define tasks

    ingest_task = PythonOperator(
        task_id="ingest_openaq_data",
        python_callable=ingest_openaq
    )

    dbt_task = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt
    )

    # Set task dependencies

    ingest_task >> dbt_task