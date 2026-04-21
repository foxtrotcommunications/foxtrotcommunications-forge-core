SELECT 
    COUNT(1) AS rows_processed
FROM `~CATALOG~`.`~SCHEMA~`.`~TABLE_NAME~`
WHERE ingestion_timestamp >= '~BEGINNING_TS~'
