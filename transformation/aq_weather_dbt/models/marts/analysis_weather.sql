-- analysis_weather.sql

{{ config(
    materialized='table',
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
    FROM {{ ref('stg_weather_data') }}
)

SELECT *
FROM base
{% if is_incremental() %}
WHERE (city) NOT IN (SELECT city FROM {{ this }})
{% endif %}
