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

SELECT 
    concat(src_tbl.idx, '_', cast(ROW_NUMBER() over(partition by src_tbl.ingestion_hash, src_tbl.idx order by to_json_string(`~JSON_FIELD~`)) as string)) idx
    ,src_tbl.ingestion_hash
    ,src_tbl.ingestion_timestamp
    ,CONCAT(src_tbl.table_path, '__', '~JSON_FIELD~') as table_path
    ,~DBT_SELECT~
FROM ~TABLE_NAME~ src_tbl,
UNNEST(JSON_EXTRACT_ARRAY(src_tbl.`~JSON_FIELD~`)) `~JSON_FIELD~`
where `~JSON_FIELD~` is not null
{% if is_incremental() %}
and
(
        ingestion_hash not in (select ingestion_hash from {{this}})
    and ingestion_timestamp >= (select max(ingestion_timestamp) from {{this}})
)
{% endif %}