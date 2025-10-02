-- analysis_weather.sql

{{ config(
    materialized='incremental',
    unique_key=['city']
) }}

WITH base AS (
    SELECT
        city,
        latitude,
        longitude,
        reading_date,
        reading_time,
        temperature_2m,
        relativehumidity_2m,
        windspeed_10m,
        winddirection_10m,
        precipitation,
        ROUND(
            (29.787054 - 0.755534 * temperature_2m + 0.094849 * relativehumidity_2m 
            + 0.391230 * precipitation - 0.689948 * windspeed_10m 	-0.004429 * winddirection_10m), 
            2
        ) AS predicted_pm25
    FROM {{ ref('stg_weather_data') }}
)

SELECT *
FROM base
{% if is_incremental() %}
WHERE (city) NOT IN (SELECT city FROM {{ this }})
{% endif %}
