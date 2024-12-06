SELECT
    description,
    replace(code, ' ', '') AS code
FROM
    {{ source('raw_zone', 'codes_operations_traitements') }}
