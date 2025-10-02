-- We materialize as view here, because it’s just column renaming + unnesting.
-- Views are cheap here, and always reflect whatever’s in the ingested table.

{{ config(
    materialized='view'
) }}

WITH unnested_weather AS (
    SELECT 
        fetched_at_utc::TIMESTAMP AS fetched_at_utc,
        source,
        city,
        data.latitude AS latitude,
        data.longitude AS longitude,
        data.utc_offset_seconds AS utc_offset_seconds,
        data.timezone AS timezone,
        data.timezone_abbreviation AS timezone_abbreviation,
        data.elevation AS elevation,
        UNNEST(data.hourly.time) AS time_raw,
        UNNEST(data.hourly.temperature_2m) AS temperature_2m,
        UNNEST(data.hourly.relativehumidity_2m) AS relativehumidity_2m,
        UNNEST(data.hourly.windspeed_10m) AS windspeed_10m,
        UNNEST(data.hourly.winddirection_10m) AS winddirection_10m,
        UNNEST(data.hourly.precipitation) AS precipitation
    FROM {{ ref('stg_ingested_weather_data') }}
)

SELECT
    *
FROM unnested_weather

