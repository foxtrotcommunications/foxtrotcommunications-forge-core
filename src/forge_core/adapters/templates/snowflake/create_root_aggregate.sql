{{
    config(
        materialized='incremental',
        unique_key=['ingestion_hash','idx'],
        on_schema_change='append_new_columns'
    )
}}

select 
    cast(ROW_NUMBER() over(partition by MD5(TO_JSON("root")::VARCHAR) order by TO_JSON("root")::VARCHAR) as string) as idx
    ,'[' || TO_JSON(j."root")::VARCHAR || ']' as "root"
    ,MD5(TO_JSON("root")::VARCHAR) as ingestion_hash
    ,CURRENT_TIMESTAMP() as ingestion_timestamp 
    ,'FRG' as table_path
from 
(
~SQL_SELECTS~
) j
{% if is_incremental() %}
where MD5(TO_JSON("root")::VARCHAR) not in (select ingestion_hash from {{this}})
{% endif %}
