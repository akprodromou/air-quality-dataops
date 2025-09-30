{{ config(
    materialized='view'  
) }}

SELECT DISTINCT ON (location_id, sensor_id, parameter)
    CAST(location_id AS VARCHAR) AS location_id,
    location_name,
    locality,
    country_code,
    country,
    latitude,
    longitude,
    timezone,
    provider_name,
    last_updated,
    sensor_id,
    parameter,
    unit,
    parameter_display_name
FROM {{ ref('stg_openaq_data') }}
ORDER BY location_id, sensor_id, parameter, last_updated DESC