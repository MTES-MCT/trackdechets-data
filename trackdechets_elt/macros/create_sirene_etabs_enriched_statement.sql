{% macro create_sirene_etabs_enriched_statement() -%}
SELECT
    cgc.*,
    se.siret,
    se.activite_principale_etablissement
FROM
    {{ ref('stock_etablissement') }}
    AS se
LEFT JOIN
    {{ ref('code_geo_communes') }}
    AS cgc
    ON se.code_commune_etablissement = cgc.code_commune and cgc.type_commune != 'COMD'
{%- endmacro %}