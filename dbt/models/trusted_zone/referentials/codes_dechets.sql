SELECT
    code,
    description
FROM {{ ref('stg_codes_dechets') }}
