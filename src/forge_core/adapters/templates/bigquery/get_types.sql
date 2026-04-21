SELECT 
	'~KEY~' field
	,case type
		when 0 then 'object'
		when 1 then 'array'
		else 'string'
	end type

FROM
(
	SELECT '~KEY~' field
	, min(
			case 
			when json_type(SAFE.JSON_QUERY(SAFE.PARSE_JSON(`~JSON_FIELD~`), '$."~KEY~"')) = 'object' then 0
			when json_type(SAFE.JSON_QUERY(SAFE.PARSE_JSON(`~JSON_FIELD~`), '$."~KEY~"')) = 'array' then 1
			else 2
		end
	) type
	FROM ~TABLE_NAME~
) j
