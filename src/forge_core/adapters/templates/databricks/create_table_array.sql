{{
    config(
        materialized='incremental',
        unique_key=['ingestion_hash','idx'],
        on_schema_change='append_new_columns',
        file_format='delta'
    )
}}

SELECT 
    CONCAT(idx, '_', CAST(ROW_NUMBER() OVER(PARTITION BY ingestion_hash, idx ORDER BY to_json(exploded_value)) AS STRING)) AS idx,
    ingestion_hash,
    ingestion_timestamp,
    CONCAT(table_path, '__', '~JSON_FIELD~') AS table_path,
    ~DBT_SELECT~
FROM ~TABLE_NAME~
LATERAL VIEW EXPLODE(from_json(`~JSON_FIELD~`, 'array<string>')) exploded AS exploded_value
WHERE exploded_value IS NOT NULL
{% if is_incremental() %}
AND (
    ingestion_hash NOT IN (SELECT ingestion_hash FROM {{this}})
    AND ingestion_timestamp >= (SELECT MAX(ingestion_timestamp) FROM {{this}})
)
{% endif %}
