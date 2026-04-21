{{
    config(
        materialized='incremental',
        unique_key=['ingestion_hash','idx'],
        on_schema_change='append_new_columns',
        file_format='delta'
    )
}}

SELECT * FROM 
(
    SELECT 
        CAST(ROW_NUMBER() OVER(PARTITION BY md5(CAST(`~JSON_FIELD~` AS STRING)) ORDER BY CAST(`~JSON_FIELD~` AS STRING)) AS STRING) AS idx,
        CONCAT('[', CAST(`~JSON_FIELD~` AS STRING), ']') AS root,
        md5(CAST(`~JSON_FIELD~` AS STRING)) AS ingestion_hash,
        CURRENT_TIMESTAMP() AS ingestion_timestamp,
        'frg__root' AS table_path
    FROM ~TABLE_NAME~
    WHERE `~JSON_FIELD~` IS NOT NULL
)
{% if is_incremental() %}
    WHERE ingestion_hash NOT IN (SELECT ingestion_hash FROM {{this}})
{% endif %}
