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
        (- 0.577802 * temperature_2m + 0.122864 * relativehumidity_2m 
        + 0.397465 * precipitation - 0.620459 * windspeed_10m + 0.005832 * winddirection_10m) AS predicted_pm10
    FROM {{ ref('stg_weather_data') }}
)

SELECT *
FROM base
{% if is_incremental() %}
WHERE (city, time_raw) NOT IN (SELECT city, time_raw FROM {{ this }})
{% endif %}
