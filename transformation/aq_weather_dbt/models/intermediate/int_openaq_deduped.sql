-- models/intermediate/int_openaq_deduped.sql

{{ config(
    materialized='ephemeral'  
) }}

SELECT DISTINCT
    sensor_id,
    value,
    parameter,
    reading_date,
    location_id
FROM {{ ref('stg_openaq_data') }}


