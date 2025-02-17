SELECT
    description,
    replace(code, ' ', '') AS code
FROM
    {{ source('raw_zone_referentials', 'codes_operations_traitements') }}
