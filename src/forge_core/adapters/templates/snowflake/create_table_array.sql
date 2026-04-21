{{
    config(
        materialized='incremental',
        unique_key=['ingestion_hash','idx'],
        on_schema_change='append_new_columns',
        cluster_by=['ingestion_timestamp']
    )
}}

SELECT 
    CONCAT(idx, '_', CAST(ROW_NUMBER() OVER(PARTITION BY ingestion_hash, idx ORDER BY "~JSON_FIELD~") AS VARCHAR)) AS idx
    ,ingestion_hash
    ,ingestion_timestamp
    ,CONCAT(table_path, '__', '~JSON_FIELD~') AS table_path
    , ~DBT_SELECT~
FROM ~TABLE_NAME~,
LATERAL FLATTEN(input => PARSE_JSON("~JSON_FIELD~")) AS "~JSON_FIELD~"
WHERE "~JSON_FIELD~" IS NOT NULL
{% if is_incremental() %}
AND
(
        ingestion_hash NOT IN (SELECT ingestion_hash FROM {{this}})
    AND ingestion_timestamp >= (SELECT MAX(ingestion_timestamp) FROM {{this}})
)
{% endif %}
