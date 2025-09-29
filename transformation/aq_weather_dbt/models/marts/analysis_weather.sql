-- analysis_weather.sql

{{ config(
    materialized='incremental',
    unique_key=['city', 'time_raw']
) }}

WITH base AS (
    SELECT
        fetched_at_utc,
        city,
        latitude,
        longitude,
        time_raw,
        temperature_2m,
        relativehumidity_2m,
        windspeed_10m,
        winddirection_10m,
        precipitation,
        -- PM2.5 heuristic
        (0.5 * temperature_2m - 0.3 * windspeed_10m + 0.2 * relativehumidity_2m) AS predicted_pm25
    FROM {{ ref('stg_weather_data') }}
)

SELECT *
FROM base
{% if is_incremental() %}
WHERE (city, time_raw) NOT IN (SELECT city, time_raw FROM {{ this }})
{% endif %}
