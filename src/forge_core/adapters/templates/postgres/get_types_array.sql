SELECT
    '~KEY~' AS field,
    CASE min_type
        WHEN 0 THEN 'object'
        WHEN 1 THEN 'array'
        ELSE 'string'
    END AS type
FROM (
    SELECT '~KEY~' AS field,
        min(
            CASE jsonb_typeof(jsonb_extract_path(elem.value, '~KEY~'))
                WHEN 'object' THEN 0
                WHEN 'array' THEN 1
                ELSE 2
            END
        ) AS min_type
    FROM ~TABLE_NAME~,
    LATERAL jsonb_array_elements("~JSON_FIELD~") AS elem
) j
