SELECT array_agg(DISTINCT k) AS keys FROM (
    SELECT json_object_keys(elem.value) AS k
    FROM ~TABLE_NAME~,
    LATERAL json_array_elements("~JSON_FIELD~"::json) AS elem
    WHERE "~JSON_FIELD~" IS NOT NULL
    AND json_typeof(elem.value) = 'object'
) subq
