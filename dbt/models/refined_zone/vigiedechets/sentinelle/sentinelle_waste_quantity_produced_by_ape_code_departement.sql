{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":16
    },
    order_by = '(code_departement, ape_code, waste_code)'
	)
}}


with quantities_by_ape as (
    select
        q.company_code_departement as code_departement,
        q.company_ape_code         as ape_code,
        q.waste_code,
        sum(q.waste_quantity)      as waste_quantity
    from
        {{ ref('sentinelle_waste_quantity_produced_by_siret') }} as q
    where q.company_ape_code is not null
    group by 1, 2, 3
    having not empty(code_departement)
),

data_with_quantity_share as (
    select
        q.waste_quantity,
        assumeNotNull(
            q.code_departement
        ) as code_departement,
        assumeNotNull(
            q.ape_code
        ) as ape_code,
        assumeNotNull(
            q.waste_code
        ) as waste_code,
        q.waste_quantity,
        q.waste_quantity
        / (
            sum(q.waste_quantity)
                over (partition by q.code_departement, q.ape_code)
        )
            as waste_quantity_share
    from quantities_by_ape as q
)

select
    code_departement,
    ape_code,
    waste_code,
    waste_quantity,
    waste_quantity_share
from data_with_quantity_share
