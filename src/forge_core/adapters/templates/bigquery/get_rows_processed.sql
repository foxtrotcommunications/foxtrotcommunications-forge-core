SELECT 
    count(1) rows_processed
FROM `~PROJECT~.~DATASET~.~TABLE_NAME~`
where ingestion_timestamp >= '~BEGINNING_TS~'
