SELECT array_agg(DISTINCT k) AS keys FROM (
    SELECT jsonb_object_keys(elem.value) AS k
    FROM ~TABLE_NAME~,
    LATERAL jsonb_array_elements("~JSON_FIELD~") AS elem
    WHERE "~JSON_FIELD~" IS NOT NULL
    AND jsonb_typeof(elem.value) = 'object'
) subq
