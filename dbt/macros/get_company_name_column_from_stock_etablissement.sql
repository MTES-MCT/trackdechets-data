{% macro get_company_name_column_from_stock_etablissement() -%}
    coalesce(
        denomination_usuelle_etablissement,
        enseigne_1_etablissement,
        enseigne_2_etablissement,
        enseigne_3_etablissement
    )
{%- endmacro %}

