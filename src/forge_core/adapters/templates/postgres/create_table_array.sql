{{
    config(
        materialized='incremental',
        unique_key=['ingestion_hash','idx']
    )
}}

SELECT
    _src.idx || '_' || cast(ROW_NUMBER() over(partition by _src.ingestion_hash, _src.idx order by _src."~JSON_FIELD~"::text) as varchar) AS idx
    ,_src.ingestion_hash
    ,_src.ingestion_timestamp
    ,_src.table_path || '__' || '~JSON_FIELD~' AS table_path
    ,~DBT_SELECT~
FROM (
    SELECT t.idx, t.ingestion_hash, t.ingestion_timestamp, t.table_path,
           elem.value AS "~JSON_FIELD~"
    FROM ~TABLE_NAME~ t,
    LATERAL jsonb_array_elements(t."~JSON_FIELD~") AS elem
) _src
WHERE _src."~JSON_FIELD~" IS NOT NULL
{% if is_incremental() %}
AND (
        _src.ingestion_hash NOT IN (SELECT ingestion_hash FROM {{this}})
    AND _src.ingestion_timestamp >= (SELECT max(ingestion_timestamp) FROM {{this}})
)
{% endif %}
