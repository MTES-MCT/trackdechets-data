SELECT 
    code, 
    description
FROM {{ ref('codes_dechets') }}