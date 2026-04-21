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

select * from 
(
    select 
        cast(ROW_NUMBER() over(partition by to_hex(md5(to_json_string(`~JSON_FIELD~`))) order by to_json_string(`~JSON_FIELD~`)) as string) idx
        ,parse_json(`~JSON_FIELD~`) as `~JSON_FIELD~`
        ,to_hex(md5(to_json_string(`~JSON_FIELD~`))) ingestion_hash
        ,CURRENT_TIMESTAMP() ingestion_timestamp 
        ,'frg__root' as table_path
    from ~TABLE_NAME~
    where SAFE.parse_json(`~JSON_FIELD~`) is not null
)
{% if is_incremental() %}
    where ingestion_hash not in (select ingestion_hash from {{this}})
{% endif %}