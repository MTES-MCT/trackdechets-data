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
        {{ ref('producers_produced_wastes_quantities') }}
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
        {{ ref('waste_quantity_produced_by_ape_code') }} as wqpbac
    inner join companies as c
        on
            wqpbac.ape_code = c.siret_ape_code
),

full_grid_with_company_data as (
    select
        fg.*,
        cOALESCE(pq.waste_quantity_share, 0) as waste_quantity_share
    from
        full_grid as fg
    left join {{ ref('producers_produced_wastes_quantities') }} as pq
        on
            fg.siret = pq.siret
            and fg.waste_code = pq.waste_code
),

scores as (
    select
        fgd.siret,
        max(fgd.first_activity_datetime) as first_activity_datetime,
        max(fgd.total_events_count)      as total_events_count,
        max(
            fgd.ape_code
        )                                as ape_code,
        groupArray(
            '{\'waste_code\': \''
            || fgd.waste_code
            || '\', '
            || '\'quantity_share\':'
            || toDecimalString(round(fgd.waste_quantity_share, 6), 8)
            || ', \'quantity_share_ref\':'
            || toDecimalString(round(fgd.waste_quantity_share_ref, 6), 8)
            || '}')
            as score_details,
        sum(
            abs(fgd.waste_quantity_share - fgd.waste_quantity_share_ref)
            * fgd.waste_quantity_share_ref
        )                                as scores
    from
        full_grid_with_company_data as fgd
    group by
        1
)

select *
from
    scores
