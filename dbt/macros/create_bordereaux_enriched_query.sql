{% macro create_bordereaux_enriched_query(
        model_name,
        use_recipient
    ) -%}

{% if use_recipient %}
    {% set destination_prefix = 'recipient' %}
{% else %}
    {% set destination_prefix = 'destination' %}
{% endif %}


with filtered_form as (
    select * from {{ ref(model_name) }} b
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where b.updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),
first_join as (
    SELECT
    b.*,
    etabs.code_commune                      AS "emitter_commune",
    etabs.code_departement                  AS "emitter_departement",
    etabs.code_region                       AS "emitter_region",
    etabs.latitude                       AS "emitter_latitude",
    etabs.longitude                       AS "emitter_longitude",
    etabs.activite_principale_etablissement AS "emitter_naf"
    FROM
        filtered_form
        AS b
    LEFT JOIN {{ ref('stock_etablissement_enriched') }} etabs
        ON b.emitter_company_siret = etabs.siret
)
SELECT
    b.*,
    etabs.code_commune                      AS "{{destination_prefix}}_commune",
    etabs.code_departement                  AS "{{destination_prefix}}_departement",
    etabs.code_region                       AS "{{destination_prefix}}_region",
    etabs.latitude                       AS "{{destination_prefix}}_latitude",
    etabs.longitude                       AS "{{destination_prefix}}_longitude",
    etabs.activite_principale_etablissement AS "{{destination_prefix}}_naf"
FROM
    first_join
    AS b
LEFT JOIN {{ ref('stock_etablissement_enriched') }} etabs
    ON b.{{destination_prefix}}_company_siret = etabs.siret
{%- endmacro %}
