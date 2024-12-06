{{
  config(
    materialized = 'table',
    indexes = [ {'columns': ['siret'],
    'unique': True },],
    )
}}

select
    coalesce(b.siret, st.siret)      as siret,
    {% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('bordereaux_counts_by_siret'), except=["siret"]) %}
    {% for column_name in column_names %}
        {{ column_name }},
    {% endfor %}
    {% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('statements_counts_by_siret'), except=["siret"]) %}
    {% for column_name in column_names %}
        {{ column_name }},
    {% endfor %}
    ps.num_statements_as_emitter     as num_pnttd_statements_as_emitter,
    ps.quantity_as_emitter           as quantity_pnttd_as_emitter,
    ps.num_statements_as_destination as num_pnttd_statements_as_destination,
    ps.quantity_as_destination       as quantity_pnttd_as_destination
from
    {{ ref('bordereaux_counts_by_siret') }} as b
full outer join
    {{ ref('statements_counts_by_siret') }} as st
    on b.siret = st.siret
full outer join
    {{ ref('pnttd_statements_counts_by_siret') }} as ps
    on coalesce(b.siret, st.siret) = ps.siret
