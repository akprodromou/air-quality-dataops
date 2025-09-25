-- This model converts raw JSON to flattened table

{{ config(
    materialized='table'
) }}

SELECT *
FROM read_json_auto('{{ env_var("RAW_DATA_PATH") }}/*.json')

