{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":8
    }
    )
}}


WITH etabs AS (
    SELECT
        se.*,
        cgc.code_commune,
        cgc.code_departement,
        cgc.code_region
    FROM
        {{ ref('stock_etablissement') }}
            AS se
    LEFT JOIN
        {{ ref('code_geo_communes') }}
            AS cgc
        ON
            se.code_commune_etablissement = cgc.code_commune
            AND cgc.type_commune != 'COMD'
),

coords AS (
    SELECT
        code_commune_insee,
        avg(latitude)  AS latitude,
        avg(longitude) AS longitude
    FROM
        {{ ref('base_codes_postaux') }}
    GROUP BY
        code_commune_insee
)

SELECT
    etabs.*,
    coords.latitude,
    coords.longitude
FROM etabs
LEFT JOIN coords ON etabs.code_commune = coords.code_commune_insee
