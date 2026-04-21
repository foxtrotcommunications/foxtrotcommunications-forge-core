select array_agg(distinct keys) keys from
(
    SELECT keys
    from
    (
        SELECT DISTINCT JSON_KEYS(safe.parse_json(`~JSON_FIELD~`), 1) AS keys_
        FROM ~TABLE_NAME~,
        UNNEST(JSON_QUERY_ARRAY(`~JSON_FIELD~`)) `~JSON_FIELD~`
        where safe.parse_json(`~JSON_FIELD~`) is not null
    ),unnest(keys_) keys
)