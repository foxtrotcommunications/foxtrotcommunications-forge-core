{{
    config(
        materialized='incremental',
        unique_key=['ingestion_hash','idx'],
        on_schema_change='append_new_columns',
        partition_by={
            "field": "ingestion_timestamp",
            "data_type": "timestamp",
            "granularity": "month"
        },
        time_ingestion_column=true,
        cluster_by=["ingestion_timestamp"]~LABELS_CONFIG~
    )
}}

select 
    cast(ROW_NUMBER() over(partition by to_hex(md5(to_json_string(`root`))) order by to_json_string(`root`)) as string) idx
    ,"[" || to_json_string(j.`root`) || "]" `root`
    ,to_hex(md5(to_json_string(`root`))) ingestion_hash
    ,CURRENT_TIMESTAMP() ingestion_timestamp 
    ,'frg' as table_path
from 
(
~SQL_SELECTS~
) j
{% if is_incremental() %}
where to_hex(md5(to_json_string(`root`))) not in (select ingestion_hash from {{this}})
{% endif %}