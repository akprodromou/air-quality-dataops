# Air Quality DataOps Project

This project demonstrates a simple DataOps workflow for collecting, storing, and visualizing air quality data from the OpenAQ API for Thessaloniki, Greece. It uses Python, DuckDB, and Airflow for orchestration, and Plotly for visualization.

## Project Structure

```
.
├── .env
├── .venv
├── dags_data
├── data
│   ├── air_quality.duckdb
│   ├── data_ingestion
│   │   └── raw_data
│   │       └── ingest_openaq_data.py
│   └── data_transformation
│       └── air_quality_dbt
├── logs
├── orchestration
│   ├── air_quality_pipeline.py
│   └── example_dag.py
├── docker-compose.yml
├── Dockerfile
├── explore_duckdb.py
├── visualizations
│   └── visualize_air_quality.py
│   └── air_quality_plot.png
├── README.md
└── requirements.txt
```

## Features

### Data Ingestion
- Fetches air quality measurements from the OpenAQ API.
- Saves raw JSON files locally in `data/data_ingestion/raw_data/`.

### Data Storage
- Stores ingested JSON data in DuckDB.
- Converts JSON to structured tables: `ingested_openaq_data` and `stg_openaq_data`.

### Data Orchestration
- Airflow DAG (`air_quality_pipeline.py`) automates ingestion and transformation.
- Schedule defined as daily (`@daily`) in Airflow.

### Data Visualization
- `visualize_air_quality.py` reads from DuckDB and creates charts using Plotly.
- Visualizations include time series of pollutants (CO, NO2, O3, PM10, PM2.5, SO2).

## Requirements

- Python 3.10+
- DuckDB
- Pandas
- Plotly
- Requests
- Airflow (if running DAGs in Docker)
- Docker & Docker Compose (for local orchestration)

Install Python dependencies via:

```bash
pip install -r requirements.txt
```

## Usage

### Run Data Ingestion
```bash
python data/data_ingestion/raw_data/ingest_openaq_data.py
```
This fetches the latest air quality data and saves JSON files in `data/data_ingestion/raw_data/`.

### Populate DuckDB
```bash
python explore_duckdb.py
```
This reads all raw JSON files and populates the `stg_openaq_data` table in `data/air_quality.duckdb`.

### Visualize Data
```bash
python visualizations/visualize_air_quality.py
```
Generates interactive charts for pollutants over time.

### Optional: Run Airflow DAG
```bash
docker-compose up -d
```
- Airflow will run the `air_quality_pipeline` DAG according to the schedule (`@daily`).
- Access the Airflow web UI at [http://localhost:8081](http://localhost:8081).

## Notes

- The project currently fetches data from the "Agia Sofia" station in Thessaloniki.
- The pollutants tracked are CO, NO2, O3, PM10, PM2.5, and SO2 (units: µg/m³).
- For portfolio purposes, the project runs locally or via Docker; no cloud deployment is required.

