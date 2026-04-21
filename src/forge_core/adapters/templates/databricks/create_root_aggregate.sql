{{
    config(
        materialized='incremental',
        unique_key=['ingestion_hash','idx'],
        file_format='delta',
        partition_by='ingestion_timestamp'
    )
}}

select 
    cast(row_number() over(partition by md5(cast(to_json(`root`) as string)) order by cast(to_json(`root`) as string)) as string) as idx
    ,concat('[', cast(to_json(j.`root`) as string), ']') as `root`
    ,md5(cast(to_json(`root`) as string)) as ingestion_hash
    ,current_timestamp() as ingestion_timestamp 
    ,'frg' as table_path
from 
(
~SQL_SELECTS~
) j
{% if is_incremental() %}
where md5(cast(to_json(`root`) as string)) not in (select ingestion_hash from {{this}})
{% endif %}
