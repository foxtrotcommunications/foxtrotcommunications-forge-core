SELECT
    count(1) AS rows_processed
FROM "~SCHEMA~"."~TABLE_NAME~"
WHERE ingestion_timestamp >= '~BEGINNING_TS~'
