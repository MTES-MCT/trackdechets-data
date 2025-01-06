{% macro create_laposte_enriched_statement() -%}
select
    code_commune_insee,
    avg(latitude) as latitude,
    avg(longitude) as longitude
from
    {{ ref('base_codes_postaux') }} bcp
group by
    code_commune_insee
{%- endmacro %}