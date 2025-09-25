{{ config(
    materialized='incremental',
    unique_key=['location_id', 'parameter', 'last_updated']
) }}

WITH base AS (
    SELECT
        current_date AS run_date,
        location_id,
        location_name,
        city,
        country,
        parameter,
        value,
        unit,
        last_updated
    FROM {{ ref('stg_openaq_data') }}
)

SELECT *
FROM base
{% if is_incremental() %}
WHERE (location_id, parameter, last_updated) NOT IN (SELECT location_id, parameter, last_updated FROM {{ this }})
{% endif %}
