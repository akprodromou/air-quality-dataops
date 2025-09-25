from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import tempfile
import os

# Project root inside the container
project_root = Path(__file__).parent.parent

# Import the visualization function
from visualize_air_quality import generate_visualizations


def ingest_openaq():
    """
    Runs the existing ingest_openaq_data.py script.
    Fetches raw data from OpenAQ and saves it to ingestion/raw_data.
    """
    subprocess.run(
        ["python3", str(project_root / "ingestion/ingest_openaq_data.py")],
        check=True
    )

def run_dbt():
    """
    Runs DBT transformations.
    """
    subprocess.run(
        [
            "dbt", "run",
            "--project-dir", str(project_root / "transformation/air_quality_dbt"),
            "--profiles-dir", str(project_root / "transformation/air_quality_dbt"),
            "--models", "stg_ingested_openaq_data stg_openaq_data analysis_air_quality"
        ],
        check=True
    )

def generate_visualizations_safe():
    """
    Generate visualizations and save them to a directory with proper permissions.
    Returns the path where files were saved.
    """
    # Use /tmp/airflow_visualizations as a persistent temp directory
    output_path = Path("/tmp/airflow_visualizations")
    
    try:
        output_path.mkdir(exist_ok=True, parents=True, mode=0o755)
        
        # Test write permissions
        test_file = output_path / "test.tmp"
        test_file.write_text("test")
        test_file.unlink()
        
        print(f"Using output directory: {output_path}")
        result_path = generate_visualizations(output_dir=output_path)
        
        # Log the files that were created
        html_files = list(output_path.glob("*.html"))
        print(f"✅ Pipeline completed! Created {len(html_files)} visualization files in {output_path}:")
        for file in html_files:
            file_size_mb = file.stat().st_size / (1024 * 1024)
            print(f"  - {file.name} ({file_size_mb:.1f} MB)")
        
        return str(output_path)
        
    except Exception as e:
        print(f"Error in visualization generation: {e}")
        raise

# ----------------------
# DAG definition
# ----------------------

with DAG(
    dag_id="air_quality_pipeline",
    description="Airflow DAG to ingest OpenAQ data, run DBT, and generate visualizations",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["air_quality", "dataops"]
) as dag:

    # Tasks
    ingest_task = PythonOperator(
        task_id="ingest_openaq_data",
        python_callable=ingest_openaq
    )

    dbt_task = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt
    )

    task_visualizations = PythonOperator(
        task_id='generate_visualizations',
        python_callable=generate_visualizations_safe,
        op_kwargs={"output_dir": "/opt/airflow/visualizations"}
    )

    # Task dependencies
    ingest_task >> dbt_task >> task_visualizations