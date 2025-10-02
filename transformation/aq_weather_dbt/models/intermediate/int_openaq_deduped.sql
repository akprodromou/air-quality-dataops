-- models/intermediate/int_openaq_deduped.sql

{{ config(
    materialized='view'  
) }}

SELECT DISTINCT
    sensor_id,
    value,
    location_id,
    parameter
FROM {{ ref('stg_openaq_data') }}


