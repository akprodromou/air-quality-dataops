-- A staging model is a layer that transforms this raw data into more structured, clean, and
-- queryable "building block" tables with proper column names and data types.

-- However, staging should reflect raw data structure (warts and all). That way, you can always 
-- debug against the original feed.

{{ config(
    materialized='table'
) }}

WITH expanded AS (
    SELECT
        result_item.sensorsId AS sensor_id,
        result_item.value AS value,
        result_item.locationsId AS location_id,
        CAST(result_item.datetime.utc AS DATE) AS reading_date
    FROM {{ ref('stg_ingested_openaq_data') }},
         UNNEST(results) AS t(result_item)
),

sensors_mapping AS (
    SELECT
        unnest(['co', 'no', 'no2', 'o3', 'pm10', 'pm25', 'so2']) AS parameter,
        unnest([sensors_dict.co, sensors_dict."no", sensors_dict.no2, 
                sensors_dict.o3, sensors_dict.pm10, sensors_dict.pm25, 
                sensors_dict.so2]) AS sensor_id
    FROM {{ ref('stg_ingested_openaq_data') }}
)

SELECT 
        e.sensor_id,
        e.value,
        s.parameter,
        e.reading_date,
        e.location_id,
FROM expanded e
LEFT JOIN sensors_mapping s
       ON e.sensor_id = s.sensor_id
ORDER BY e.location_id, s.parameter