# This script is the first step of the Weather Data Pipeline. It's run inside weather_data_pipeline.py.
# It connects to the Open Meteo database and brings in data for the location specified (i.e. Thessaloniki)
# in raw (JSON) format. 

from pathlib import Path
import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

# All required weather variables should be listed here. The below code defaults
# to Thessaloniki, if the variables are not defined in docker-compose.yml

# Variables set-up

CITY_LAT = os.getenv("CITY_LAT")
CITY_LON = os.getenv("CITY_LON")
CITY_NAME = os.getenv("CITY_NAME")
# Forecast horizon (days). Our aim is to make PM2.5 predictions for 7 days,
# hence the respective weather data needed
WEATHER_FORECAST_DAYS = int(os.getenv("WEATHER_FORECAST_DAYS", "7"))
WEATHER_TIMEZONE = os.getenv("OPENMETEO_TIMEZONE", "Europe/Moscow")

API_URL = "https://api.open-meteo.com/v1/forecast"

# os.getenv checks if the RAW_DATA_PATH specified in docker-compose.yml exists (i.e. /opt/airflow/ingestion/air_quality)
# if not, it uses the relative path provided
RAW_DATA_PATH_WEATHER = Path(os.getenv("RAW_DATA_PATH_WEATHER","./ingestion/raw_data/weather"))
RAW_DATA_PATH_WEATHER.mkdir(parents=True, exist_ok=True)

DEFAULT_HOURLY_VARS = [
    "temperature_2m",
    "relativehumidity_2m",
    "windspeed_10m",
    "winddirection_10m",
    "precipitation"
]

# Open-Meteo client set-up

# We will use the Python built-in logging module to capture logs, which provide insights into  
# application flow, errors, and usage patterns.

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ingest_open_meteo")

# This is a helper function that creates a session configured with retry/backoff behavior.
# Sessions are used to persist parameters across requests, so we don't have to create a
# fresh connection for each request
def make_session(retries: int = 3, backoff: float = 0.5, timeout: int = 30) -> requests.Session:
    session = requests.Session()
    # build a Retry object
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        # HTTP status codes that should trigger a retry
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        # allow the session to 
        raise_on_status=False,
    )
    # adapter so that requests going through that session inherit the retry rules
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    # store default timeout on session for convenience
    session.request_timeout = timeout
    return session

# Fetch function 

def fetch_weather_forecast(
    lat: float,
    lon: float,
    hourly_vars: Optional[List[str]] = None,
    forecast_days: int = WEATHER_FORECAST_DAYS,
    timezone: str = WEATHER_TIMEZONE,
    session: Optional[requests.Session] = None,
) -> Optional[Dict[str, Any]]:

    # Call Open-Meteo forecast API and return parsed JSON (or None on error).
    hourly_vars = hourly_vars or DEFAULT_HOURLY_VARS
    session = session or make_session()

    params = {
        "latitude": lat,
        "longitude": lon,
        # Open-Meteo accepts a comma-separated list for hourly params
        "hourly": ",".join(hourly_vars),
        "forecast_days": int(forecast_days),
        "timezone": timezone,
    }

    logger.info("Requesting Open-Meteo: lat=%s lon=%s hourly=%s forecast_days=%s timezone=%s",
                lat, lon, hourly_vars, forecast_days, timezone)

    try:
        # send the GET request
        resp = session.get(API_URL, params=params, timeout=session.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        logger.info("Open-Meteo response OK; keys in response: %s", sorted(list(data.keys())))
        return data
    except requests.exceptions.RequestException as e:
        logger.exception("HTTP error fetching Open-Meteo forecast: %s", e)
    except ValueError as e:
        logger.exception("Error decoding JSON from Open-Meteo: %s", e)
    return None

# Save raw data function
# Our strategy is to write data to a temporary file and when data has been successfully written, 
# rename the file to the correct destination file

def save_weather_raw(data: Dict[str, Any], city: str, outdir: Path = RAW_DATA_PATH_WEATHER) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_city = city.lower().replace(" ", "_")
    final_path = outdir / f"openmeteo_{safe_city}_{ts}.json"
    tmp_path = outdir / (final_path.name + ".tmp")

    # Save the API response with a small metadata wrapper (fetched_at_utc, coords, requested_params)
    wrapper = {
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "open-meteo",
        "city": city,
        "data": data,
    }
    # Uses atomic write (tmp -> replace) so partial files are not left behind
    try:
        with tmp_path.open("w", encoding="utf-8") as fh:
            json.dump(wrapper, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        tmp_path.replace(final_path)
        logger.info("Saved Open-Meteo raw file: %s", final_path)
        return final_path
    except Exception:
        logger.exception("Failed to write raw Open-Meteo file to %s", final_path)
        raise


# Ingest function for Airflow

def ingest_weather(
    lat: float = CITY_LAT,
    lon: float = CITY_LON,
    city: str = CITY_NAME,
    hourly_vars: Optional[List[str]] = None,
    forecast_days: int = WEATHER_FORECAST_DAYS,
    outdir: Path = RAW_DATA_PATH_WEATHER
) -> Optional[Path]:
    # Fetch and save one Open-Meteo forecast snapshot. Returns Path to saved file or None.

    session = make_session()
    data = fetch_weather_forecast(lat, lon, hourly_vars=hourly_vars, forecast_days=forecast_days, timezone=WEATHER_TIMEZONE, session=session)
    if not data:
        logger.error("No data fetched; nothing saved.")
        return None
    return save_weather_raw(data, city, outdir=outdir)


# Command Line Interface entrypoint
if __name__ == "__main__":
    # simple CLI-like behavior: run with default env vars
    path = ingest_weather()
    if path:
        logger.info("Ingestion successful: %s", path)
    else:
        logger.error("Ingestion failed.")