{{
  config(
    materialized = 'table',
    order_by = 'siret'
    ) }}

select
    assumeNotNull(coalesce(b.siret, st.siret, ps.siret)) as siret,
    {% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('bordereaux_counts_by_siret'), except=["siret"]) %}
    {% for column_name in column_names %}
        {% if 'num' in column_name or 'quantity' in column_name %}
            coalesce({{ column_name }}, 0) as {{ column_name }},
        {% else %}
            {{ column_name }},
        {% endif %}
    {% endfor %}
    {% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('statements_counts_by_siret'), except=["siret"]) %}
    {% for column_name in column_names %}
        {% if 'num' in column_name or 'quantity' in column_name or 'volume' in column_name %}
            coalesce({{ column_name }}, 0) as {{ column_name }},
        {% else %}
            {{ column_name }},
        {% endif %}
    {% endfor %}
    coalesce(st.num_statements, 0)
    + coalesce(b.num_bordereaux, 0)
        as total_bordereaux_statements_references,
    ps.num_statements_as_emitter
        as num_pnttd_statements_as_emitter,
    ps.quantity_as_emitter
        as quantity_pnttd_as_emitter,
    ps.num_statements_as_destination
        as num_pnttd_statements_as_destination,
    ps.quantity_as_destination
        as quantity_pnttd_as_destination
from
    {{ ref('bordereaux_counts_by_siret') }} as b
full outer join
    {{ ref('statements_counts_by_siret') }} as st
    on b.siret = st.siret
full outer join
    {{ ref('pnttd_statements_counts_by_siret') }} as ps
    on coalesce(b.siret, st.siret) = ps.siret
where not empty(siret) and siret is not null
