select array_agg(distinct keys) keys from
(
    SELECT keys
    from
    (
        SELECT DISTINCT JSON_KEYS(safe.parse_json(`~JSON_FIELD~`), 1) AS keys_
        FROM ~TABLE_NAME~ src_tbl,
        UNNEST(JSON_QUERY_ARRAY(src_tbl.`~JSON_FIELD~`)) `~JSON_FIELD~`
        where safe.parse_json(`~JSON_FIELD~`) is not null
    ),unnest(keys_) keys
)