select array_agg(distinct keys) keys from
(
    select keys
    from
    (
        SELECT DISTINCT JSON_KEYS(safe.parse_json(`~JSON_FIELD~`), 1) AS keys_
        FROM ~TABLE_NAME~
        WHERE safe.parse_json(`~JSON_FIELD~`) is not null
    ),unnest(keys_) keys
)