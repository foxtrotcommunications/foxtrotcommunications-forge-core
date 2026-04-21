SELECT 
    '~KEY~' AS "field",
    CASE type_code
        WHEN 0 THEN 'object'
        WHEN 1 THEN 'array'
        ELSE 'string'
    END AS "type"
FROM (
    SELECT 
        MIN(
            CASE 
                WHEN IS_OBJECT(GET(array_flat.VALUE, '~KEY~')) OR IS_OBJECT(TRY_PARSE_JSON(GET(array_flat.VALUE, '~KEY~')::VARCHAR)) THEN 0
                WHEN IS_ARRAY(GET(array_flat.VALUE, '~KEY~')) OR IS_ARRAY(TRY_PARSE_JSON(GET(array_flat.VALUE, '~KEY~')::VARCHAR)) THEN 1
                ELSE 2
            END
        ) AS type_code
    FROM ~TABLE_NAME~,
    LATERAL FLATTEN(input => TRY_PARSE_JSON("~JSON_FIELD~")) array_flat
    WHERE GET(array_flat.VALUE, '~KEY~') IS NOT NULL
)
