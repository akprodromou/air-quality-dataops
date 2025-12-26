# Air Quality DataOps Project

This project demonstrates a simple DataOps workflow for collecting, storing, and visualizing air quality data from the OpenAQ API for Thessaloniki, Greece. It uses Python, DuckDB, and Airflow for orchestration, and Plotly for visualization.

## Project Structure

```
AIR-QUALITY-DATA.../
├── data/                           # Raw, historic, and forecast data
├── ingestion/                      # Scripts to collect OpenAQ & weather data
├── transformation/                 # DBT models and ETL logic
├── AQ_predictive_modeling_files/   # Random Forest model & forecasting scripts
├── visualization/                  # Streamlit app, plotting, and styling
├── orchestration/                  # Pipeline orchestration scripts
└── logs/                           # Logs for ingestion, transformation, and model runs

```

## Features

### Data Ingestion
- Fetches air quality measurements from the OpenAQ API and weather data from external sources.
- Saves raw JSON files locally in `data/ingestion/raw_data/`.

### Data Storage & Transformation
- Stores ingested data in DuckDB.
- Transforms raw JSON into structured tables (`ingested_openaq_data`, `stg_openaq_data`).
- DBT models handle staging, intermediate tables, and final data marts.

### Data Orchestration
- Airflow DAG (`AIR_quality_pipeline.py`) automates ingestion, transformation, and storage.
- Scheduled to run daily (`@daily`) with logging of process outcomes.

### Predictive Modeling
- Random Forest model predicts future pollutant concentrations.
- Model artifacts stored in `AQ_predictive_modeling_files/`.
- Forecasts can be generated programmatically via `rf_forecast.py`.

### Data Visualization
- Interactive Streamlit app (`streamlit_app.py`) reads from DuckDB and displays dynamic charts.
- Visualizations include pollutant time series, historical trends, and forecasted air quality levels.

## Requirements

- Python 3.10+
- DuckDB
- Pandas
- Plotly
- Requests
- Streamlit
- scikit-learn
- joblib
- DBT (e.g., dbt-core, dbt-duckdb)
- Airflow (if running DAGs in Docker)
- Docker
- python-dotenv (for environment variables)

## Usage & Pre-Deployment Instructions to run locally

### 0. Activate Python Environment

Install Python dependencies. For example, if venv is a virtual env:

```bash
source venv/bin/activate
pip install -r requirements.txt
```

### 1. Run Data Ingestion

Fetch the latest data from OpenAQ and Open-Meteo:

```bash
python ingestion/ingest_openaq_data.py
python ingestion/ingest_weather_data.py
```

JSON files will be saved in:

```text
ingestion/raw_data/air_quality/
ingestion/raw_data/weather/
```

Verify the data keys and content to ensure ingestion worked.

### 2. Populate DuckDB via DBT

Navigate to the DBT project:

```bash
cd transformation/aq_weather_dbt
```

Set environment variables for raw data and DuckDB file:

```bash
export RAW_DATA_PATH_AIR_QUALITY=../../ingestion/raw_data/air_quality
export RAW_DATA_PATH_WEATHER=../../ingestion/raw_data/weather
export DUCKDB_PATH=./data/air_quality_weather.duckdb
```

Verify DBT configuration:

```bash
dbt debug --project-dir . --profiles-dir .
```

Run all staging and analysis models:

```bash
dbt run --project-dir . --profiles-dir .
```

Run tests:

```bash
dbt test --project-dir . --profiles-dir .
```

This will check:
- Profiles & database connections
- SQL compilation and execution
- Column-level constraints (nulls, accepted values, unique constraints)

### 3. Run Predictive Model

Generate future pollutant forecasts using the Random Forest model:

```bash
python ../../AQ_predictive_modeling_files/rf_forecast.py
```

Model parameters are stored in `AQ_predictive_modeling_files/`.
Check the output for forecasted pollutant concentrations.

### 5. Visualize Data

Launch the Streamlit app:

```bash
cd ../..
streamlit run visualization/streamlit_app.py
```

## Run via Airflow DAG

Instead of running ingestion, DBT, predictive modeling, and visualization manually, you can launch the full pipeline using Docker and Airflow:

### Open Docker
Run the Docker app on your machine. Then start Airflow services and run the DAG:

```bash
docker-compose up -d
```

- Access the web UI at http://localhost:8081
- DAG `air_quality_pipeline` runs automatically on a daily schedule.

## Notes

- **Current station:** "Agia Sofia", Thessaloniki
- **Pollutants tracked:** NO2, O3, PM10, PM2.5, SO2 (µg/m³)

## Model layers explained

- The `stg_ingested_*` models mirror the API schema. No transformation is applied on it, except for flattening. That way, we can always debug against the original feed.
- The staging model (`stg_openaq_data`) is where we remove obvious junk (null IDs, invalid country codes, etc.) and apply basic transformations:
    - Extracting and flattening nested JSON structures
    - Renaming columns to business-friendly names
    - Light filtering (specific parameters only)
    - Standardizing data types
- Intermediate `int_openaq_deduped` is the layer where we enforce data quality rules like deduplication, keeping the latest record, or normalizing across sources. 
It gives us predictable, clean tables for downstream marts.
- Marts (`mrt_*`) the final, analytics-ready tables that aggregate, summarize, and structure data for specific use cases, such as dashboards, forecasting, or reporting.  
    By keeping this layer separate, downstream applications can query clean, business-friendly tables without worrying about raw or intermediate transformations.

## File Viewing

```bash
DuckDB UI
duckdb -ui
```

## Data Sources

- **Air Quality Data:**  
  Sourced from the **European Environment Agency (EEA)** via the [EEA Data Download Portal](https://eeadmz1-downloads-webapp.azurewebsites.net/).  
  Measurements are reported in micrograms per cubic meter (µg/m³).  

- **Weather Data:**  
  Real-time and forecast weather variables retrieved from the [Open-Meteo API](https://open-meteo.com/).  

- **Historic Climate Data:**  
  Provided by [CLIMPACT](https://data.climpact.gr/en/dataset/497dc26d-45e0-4ad5-b8f3-5f8890f65129), the Greek National Research Network for Climate Change and Its Effects.  

- **Air Quality Index Reference:**  
  Methodological reference from the [Copernicus Atmosphere Monitoring Service (CAMS)](https://ecmwf-projects.github.io/copernicus-training-cams/proc-aq-index.html).

## Streamlit

The Streamlit app was used to create an interactive dashboard for exploring the data, connecting directly to the local DuckDB database. You can access it here: https://air-quality-thessaloniki.streamlit.app/


