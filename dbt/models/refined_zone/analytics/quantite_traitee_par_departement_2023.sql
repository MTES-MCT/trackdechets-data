{{
  config(
    materialized = 'table',
    )
}}

select
    destination_departement as code_departement,
    sum(quantity_received)  as quantite_traitee
from
    {{ ref('bordereaux_enriched') }}
where
    not is_draft
    and (processed_at between '2023-01-01' and '2023-12-31')
    and (
        {{ dangerous_waste_filter('bordereaux_enriched') }}
    )
    and processed_at is not null
group by 1
order by quantite_traitee desc nulls last
