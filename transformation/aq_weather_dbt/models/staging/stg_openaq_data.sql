-- A staging model is a layer that transforms this raw data into more structured, clean, and
-- queryable "building block" tables with proper column names and data types.

-- However, staging should reflect raw data structure (warts and all). That way, you can always 
-- debug against the original feed.

-- We materialize as view here, because it's just column renaming + unnesting.
-- Views are cheap here, and always reflect whatever's in the ingested table.
{{ config(
    materialized='view'
) }}

WITH expanded AS (
    SELECT
        CAST(result_item.id AS VARCHAR) AS location_id,
        result_item.name AS location_name,
        result_item.locality AS locality,
        result_item.country.code AS country_code,
        result_item.country.name AS country,
        result_item.coordinates.latitude AS latitude,
        result_item.coordinates.longitude AS longitude,
        result_item.timezone AS timezone,
        result_item.provider.name AS provider_name,
        result_item.datetimeLast->>'utc' AS last_updated,
        sensor_item.id AS sensor_id,
        LOWER(TRIM(sensor_item.parameter.name)) AS parameter,
        sensor_item.parameter.units AS unit,
        sensor_item.parameter.displayName AS parameter_display_name
    -- dependency defined and read by DAG
    FROM {{ ref('stg_ingested_openaq_data') }},
         UNNEST(results) AS t(result_item),
         UNNEST(result_item.sensors) AS s(sensor_item)
    WHERE LOWER(TRIM(sensor_item.parameter.name)) IN ('pm25','pm10','o3','no2','so2','co')
    ORDER BY location_id, parameter
)

SELECT *
FROM expanded
ORDER BY location_id, parameter
