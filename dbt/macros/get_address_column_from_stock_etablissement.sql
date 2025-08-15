{% macro get_address_column_from_stock_etablissement() -%}
    coalesce(numero_voie_etablissement || ' ', '')
    || coalesce(type_voie_etablissement || ' ', '')
    || coalesce(libelle_voie_etablissement || ' ', '')
    || coalesce(code_postal_etablissement || ' ', '')
    || coalesce(libelle_commune_etablissement, '')
{%- endmacro %}

