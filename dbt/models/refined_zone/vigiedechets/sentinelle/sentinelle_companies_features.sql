{{
  config(
    materialized = 'table',
    )
}}

with companies as (
    select
        siret,
        max(siret_ape_code)              as siret_ape_code,
        max(first_activity_datetime_min) as first_activity_datetime_min,
        min(total_events_count)          as total_events_count
    from
        {{ ref('sentinelle_waste_quantity_produced_by_siret_ape_code') }}
    group by
        1
    having
        siret_ape_code is not null
),

full_grid as (
    select
        c.siret,
        c.first_activity_datetime_min as first_activity_datetime,
        c.total_events_count,
        wqpbac.ape_code,
        wqpbac.waste_code,
        wqpbac.waste_quantity_share   as waste_quantity_share_ref
    from
        {{ ref('sentinelle_waste_quantity_produced_by_ape_code') }} as wqpbac
    inner join companies as c
        on
            wqpbac.ape_code = c.siret_ape_code
)

select
    fg.*,
    cOALESCE(pq.waste_quantity_share, 0) as waste_quantity_share
from
    full_grid as fg
left join
    {{ ref('sentinelle_waste_quantity_produced_by_siret_ape_code') }} as pq
    on
        fg.siret = pq.siret
        and fg.waste_code = pq.waste_code
