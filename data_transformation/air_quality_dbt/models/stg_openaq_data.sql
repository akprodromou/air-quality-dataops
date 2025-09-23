-- models/stg_openaq_data.sql

{{ config(materialized='view') }}

-- Step 1: Read the raw JSON file and unnest the top-level 'results' array.
-- This creates a row for each item in the 'results' array,
-- with the entire JSON object for that result in the 'location_json' column.
WITH raw AS (
    SELECT
        value AS location_json -- 'value' is the default column name when unnesting with json_each
    FROM 
        read_json_auto('/opt/airflow/data_ingestion/raw_data/openaq_data_thessaloniki_agia_sofia_*.json', filename=true)
        -- Use json_each to flatten the 'results' array at the top level
        CROSS JOIN json_each(results)
),

-- Step 2: Flatten the nested 'measurements' array and extract relevant fields.
-- We use LEFT JOIN json_each to handle cases where 'measurements' might be missing or empty.
-- TRY_CAST is used for robust type conversion, returning NULL if conversion fails.
-- TRIM(BOTH '"' FROM ...) is used to clean up string values extracted from JSON,
-- as json_extract often returns values with quotes.
flattened AS (
    SELECT
        TRY_CAST(json_extract(location_json, '$.id') AS INTEGER) AS location_id,
        trim(both '"' from json_extract(location_json, '$.location')) AS location_name,
        trim(both '"' from json_extract(location_json, '$.city')) AS city,
        trim(both '"' from json_extract(location_json, '$.country')) AS country,
        TRY_CAST(json_extract(location_json, '$.coordinates.latitude') AS DOUBLE) AS latitude,
        TRY_CAST(json_extract(location_json, '$.coordinates.longitude') AS DOUBLE) AS longitude,
        -- Note: 'sensorType' might not be present in OpenAQ v3 'latest' endpoint results directly.
        -- If this column is consistently NULL, you might remove it or find it from another source.
        trim(both '"' from json_extract(location_json, '$.sensorType')) AS sensor_type,
        measurement.value AS measurement_json -- 'value' from json_each is the measurement object
    FROM
        raw
        -- Flatten the 'measurements' array within each location_json
        LEFT JOIN json_each(json_extract(location_json, '$.measurements')) AS measurement
        ON TRUE -- This is effectively a LATERAL JOIN equivalent in DuckDB for json_each
)

-- Final selection and type casting for the staged data model.
SELECT
    location_id,
    location_name,
    city,
    country,
    latitude,
    longitude,
    sensor_type,
    trim(both '"' from json_extract(measurement_json, '$.parameter')) AS parameter,
    TRY_CAST(json_extract(measurement_json, '$.value') AS DOUBLE) AS value,
    trim(both '"' from json_extract(measurement_json, '$.unit')) AS unit,
    -- Convert last_updated to TIMESTAMP. OpenAQ's lastUpdated is usually UTC.
    TRY_CAST(trim(both '"' from json_extract(measurement_json, '$.lastUpdated')) AS TIMESTAMP) AS last_updated
FROM
    flattened
-- Basic data quality filtering: ensure key fields are not null
WHERE
    location_id IS NOT NULL
    AND parameter IS NOT NULL
    AND value IS NOT NULL
    AND last_updated IS NOT NULL

