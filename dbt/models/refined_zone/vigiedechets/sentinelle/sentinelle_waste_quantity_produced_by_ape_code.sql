{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":16
    }
	)
}}


with quantities_by_ape as (
    select
        q.siret_ape_code      as ape_code,
        q.waste_code,
        sum(q.waste_quantity) as waste_quantity
    from
        {{ ref('sentinelle_waste_quantity_produced_by_siret_ape_code') }} as q
    where ape_code is not null
    group by 1, 2
)

select
    q.ape_code,
    q.waste_code,
    q.waste_quantity,
    q.waste_quantity
    / (sum(q.waste_quantity) over (partition by q.ape_code))
        as waste_quantity_share
from quantities_by_ape as q
