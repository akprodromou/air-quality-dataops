-- Materialized as an incremental table, i.e. updated by processing only new or changed data since the last run
{{ config(
    materialized='incremental'
) }}

WITH parameter_metadata AS (
    SELECT *
    FROM (VALUES
        ('pm10', 'µg/m³', 'Particulate Matter 10µm', 'PM'),
        ('pm25', 'µg/m³', 'Particulate Matter 2.5µm', 'PM'),
        ('co', 'µg/m³', 'CO mass', 'Gas'),
        ('no', 'µg/m³', 'NO mass', 'Gas'),
        ('no2', 'µg/m³', 'NO2 mass', 'Gas'),
        ('o3', 'µg/m³', 'O3 mass', 'Gas'),
        ('so2', 'µg/m³', 'SO2 mass', 'Gas')
    ) AS t(parameter, unit, display_name, category)
),

cleaned_data AS (
    SELECT
        s.sensor_id,
        CAST(s.value AS DOUBLE) AS value,
        ROUND(CAST(s.value AS DOUBLE), 1) AS value_rounded,
        s.parameter,
        s.reading_date,
        s.location_id,
        m.unit,
        m.display_name,
        m.category
    FROM {{ ref('int_openaq_deduped') }} s
    LEFT JOIN parameter_metadata m
           ON s.parameter = m.parameter
)

SELECT *
FROM cleaned_data
ORDER BY reading_date DESC, parameter

