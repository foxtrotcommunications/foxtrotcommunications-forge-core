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
        cluster_by=["ingestion_timestamp"]
    )
}}

SELECT 
    concat(idx, '_', cast(ROW_NUMBER() over(partition by ingestion_hash, idx order by to_json_string(`~JSON_FIELD~`)) as string)) idx
    ,ingestion_hash
    ,ingestion_timestamp
    ,CONCAT(table_path, '__', '~JSON_FIELD~') as table_path
    ,~DBT_SELECT~
FROM ~TABLE_NAME~
where `~JSON_FIELD~` is not null
{% if is_incremental() %}
and
(
        ingestion_hash not in (select ingestion_hash from {{this}})
    and ingestion_timestamp >= (select max(ingestion_timestamp) from {{this}})
)
{% endif %}
