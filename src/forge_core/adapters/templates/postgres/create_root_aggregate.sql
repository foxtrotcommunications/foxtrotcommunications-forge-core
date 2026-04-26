{{
    config(
        materialized='incremental',
        unique_key=['ingestion_hash','idx']
    )
}}

SELECT
    cast(ROW_NUMBER() over(partition by md5("root"::text) order by "root"::text) as varchar) AS idx
    ,jsonb_build_array("root") AS "root"
    ,md5("root"::text) AS ingestion_hash
    ,CURRENT_TIMESTAMP AS ingestion_timestamp
    ,'frg' AS table_path
FROM (
~SQL_SELECTS~
) j
{% if is_incremental() %}
WHERE md5("root"::text) NOT IN (SELECT ingestion_hash FROM {{this}})
{% endif %}
