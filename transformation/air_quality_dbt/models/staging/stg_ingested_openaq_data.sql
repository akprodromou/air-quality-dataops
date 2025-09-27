-- This model reads the raw JSON files into the warehouse as-is

-- Turn it into a table, rather than a view (query) because reading JSON files repeatedly is expensive.
-- We want a persisted snapshot of the raw data to build on.

{{ config(
    materialized='table'
) }}

SELECT *
FROM read_json_auto('{{ env_var("RAW_DATA_PATH") }}/*.json')

