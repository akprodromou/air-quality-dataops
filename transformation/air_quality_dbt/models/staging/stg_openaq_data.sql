-- A staging model is a layer that transforms this raw data into more structured, clean, and
-- queryable "building block" tables with proper column names and data types

{{ config(
    materialized='view'
) }}

WITH expanded AS (
    SELECT
        result_item.locationsId AS location_id,
        'Thessaloniki Agia Sofia' AS location_name,
        'Thessaloniki' AS city,
        'GR' AS country,
        result_item.coordinates.latitude AS latitude,
        result_item.coordinates.longitude AS longitude,
        result_item.sensorsId AS sensor_id,    
        CASE result_item.sensorsId
            WHEN 12079197 THEN 'CO'
            WHEN 8622634  THEN 'NO2'
            WHEN 8622640  THEN 'O3'
            WHEN 8622643  THEN 'PM10'
            WHEN 8622646  THEN 'PM2.5'
            WHEN 8622649  THEN 'SO2'
        END AS parameter,
        result_item.value AS value,
        'µg/m³' AS unit,
        result_item.datetime.utc AS last_updated
    FROM {{ ref('stg_ingested_openaq_data') }},
         UNNEST(results) AS t(result_item)
)

SELECT *
FROM expanded

