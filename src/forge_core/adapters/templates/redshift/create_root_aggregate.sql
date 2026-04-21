{{
    config(
        materialized='incremental',
        unique_key=['ingestion_hash','idx'],
        sort=['ingestion_timestamp']
    )
}}

select 
    cast(ROW_NUMBER() over(partition by MD5(JSON_SERIALIZE("root")) order by JSON_SERIALIZE("root")) as varchar) as idx
    ,'[' || JSON_SERIALIZE(j."root") || ']' as "root"
    ,MD5(JSON_SERIALIZE("root")) as ingestion_hash
    ,CURRENT_TIMESTAMP as ingestion_timestamp 
    ,'frg' as table_path
from 
(
~SQL_SELECTS~
) j
{% if is_incremental() %}
where MD5(JSON_SERIALIZE("root")) not in (select ingestion_hash from {{this}})
{% endif %}
