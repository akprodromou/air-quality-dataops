# config.py

from pathlib import Path

# Expected pollutant columns
EXPECTED_POLLUTANTS = ["pm10_value", "pm25_value", "o3_value", "no2_value", "so2_value"]

# Air quality thresholds (µg/m³)
NO2_LIMITS = [0, 40, 90, 120, 230, 340, 1000]
OZONE_LIMITS = [0, 50, 100, 130, 240, 380, 800]
PM25_LIMITS = [0, 10, 20, 25, 50, 75, 800]
PM10_LIMITS = [0, 20, 40, 50, 100, 150, 1200]
SO2_LIMITS = [0, 100, 200, 350, 500, 750, 1250]

# Dictionary mapping display names to limits
LIMITS_DICT = {
    "NO2 (µg/m³)": NO2_LIMITS,
    "O3 (µg/m³)": OZONE_LIMITS,
    "PM10 (µg/m³)": PM10_LIMITS,
    "PM25 (µg/m³)": PM25_LIMITS,
    "SO2 (µg/m³)": SO2_LIMITS,
}

# Color scheme for air quality levels
VERY_GOOD_HEX = "#188c39"
GOOD_HEX = "#7cb324"
MEDIUM_HEX = "#ceb000"
POOR_HEX = "#dc9a00"
VERY_POOR_HEX = "#db6d00"
EXTREMELY_POOR_HEX = "#ca0000"
GREY_HEX = "#d3d3d3"  # light grey for inactive bins

COLORS = [
    VERY_GOOD_HEX,
    GOOD_HEX,
    MEDIUM_HEX,
    POOR_HEX,
    VERY_POOR_HEX,
    EXTREMELY_POOR_HEX,
]

# Verbal labels for air quality levels
VERBAL_LABELS = ["Very Good", "Good", "Medium", "Poor", "Very Poor", "Extremely Poor"]

COLUMN_MAPPING = {
    "forecast_day": "Date",
    "pm10_value": "PM10 (µg/m³)",
    "pm25_value": "PM2.5 (µg/m³)",
    "o3_value": "Ozone (µg/m³)",
    "no2_value": "NO₂ (µg/m³)",
    "so2_value": "SO₂ (µg/m³)",
}

# Define AQI category limits and colors
LIMIT_DICT = {
    "no2_value": [0, 40, 90, 120, 230, 340, 1000],
    "o3_value": [0, 50, 100, 130, 240, 380, 800],
    "pm10_value": [0, 20, 40, 50, 100, 150, 1200],
    "pm25_value": [0, 10, 20, 25, 50, 75, 800],
    "so2_value": [0, 100, 200, 350, 500, 750, 1250],
}

AQI_COLORS = [
    "#188c39",  # Very good
    "#7cb324",  # Good
    "#ceb000",  # Medium
    "#dc9a00",  # Poor
    "#db6d00",  # Very poor
    "#ca0000",  # Extremely poor
]

BASE_DIR = Path(__file__).resolve().parent.parent  # project root
DB_PATH = BASE_DIR / "data" / "air_quality_weather.duckdb"  # duckdb file
FORECAST_CSV = BASE_DIR / "data" / "forecasts" / "forecast_results.csv"