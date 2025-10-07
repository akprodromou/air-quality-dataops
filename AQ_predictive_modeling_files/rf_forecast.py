from pathlib import Path
import pandas as pd
import duckdb
import joblib

rf_model = joblib.load("random_forest_air_quality.pkl")
predictors_final = joblib.load("rf_feature_columns.pkl")

# Base path (rf_forecast.py is in AQ_predictive_modeling_files/)
base_dir = Path(__file__).parent.parent  # adjust to project root

# Path to DuckDB file
db_path = base_dir / "data/air_quality_weather.duckdb"

# Connect to DuckDB
con = duckdb.connect(database=str(db_path), read_only=False)

# Fetch 7-day weather forecast
# Aggregate hourly weather to daily
weather_query = """
SELECT 
    DATE(reading_date) AS forecast_day,
    AVG(temperature_2m) AS t_mean,
    AVG(relativehumidity_2m) AS rh_mean,
    AVG(windspeed_10m) AS ws_mean,
    SUM(precipitation) AS dwd
FROM stg_weather_data
WHERE reading_date >= CURRENT_DATE
GROUP BY forecast_day
ORDER BY forecast_day
LIMIT 7
"""
weather_forecast = con.execute(weather_query).fetchdf()

print(weather_forecast[['forecast_day']].drop_duplicates())


latest_aq_query = """
SELECT *
FROM stg_openaq_data
WHERE reading_date = (SELECT MAX(reading_date) FROM stg_openaq_data)
"""
latest_aq = con.execute(latest_aq_query).fetchdf()


targets = ['pm10_value', 'pm25_value', 'o3_value', 'no2_value', 'so2_value']

prev_pollutants = {
    'pm10_value_prev': latest_aq.loc[latest_aq['parameter']=='pm10', 'value'].values[0],
    'pm25_value_prev': latest_aq.loc[latest_aq['parameter']=='pm25', 'value'].values[0],
    'o3_value_prev': latest_aq.loc[latest_aq['parameter']=='o3', 'value'].values[0],
    'no2_value_prev': latest_aq.loc[latest_aq['parameter']=='no2', 'value'].values[0],
    'so2_value_prev': latest_aq.loc[latest_aq['parameter']=='so2', 'value'].values[0],
}

prev_day_values = prev_pollutants.copy()

# Initialize a DataFrame to store predictions
forecast_results = pd.DataFrame(columns=['forecast_day'] + targets)


for idx, day in weather_forecast.iterrows():
    # 1. Day date
    day_date = pd.to_datetime(day['forecast_day']).date()

    # 2. Feature row
    day_features = {
        't_mean': day['t_mean'],
        'rh_mean': day['rh_mean'],
        'ac_r': 0,  # placeholder if not available
        'ws_mean': day['ws_mean'],
        'dwd': day['dwd'],
    }

    # Month dummies
    month_num = day_date.month
    for m in range(1, 13):
        day_features[f'month_{m}'] = 1 if m == month_num else 0

    # Previous day pollutants
    day_features.update(prev_day_values)

    # Convert to DataFrame with same columns as training
    X_day = pd.DataFrame([day_features])[predictors_final]

    # 3. Predict pollutants
    y_pred_day = rf_model.predict(X_day)[0]

    # 4. Store predictions
    forecast_results = pd.concat([
        forecast_results,
        pd.DataFrame([[day_date] + list(y_pred_day)], columns=forecast_results.columns)
    ], ignore_index=True)

    # 5. Update prev_day_values for next iteration
    prev_day_values = dict(zip([f"{p}_prev" for p in targets], y_pred_day))



print(forecast_results)
