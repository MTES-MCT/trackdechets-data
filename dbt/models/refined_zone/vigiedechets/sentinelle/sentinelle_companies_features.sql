{{
  config(
    materialized = 'table',
    order_by = '(company_siret,waste_code)'
    )
}}

with companies as (
    select
        company_siret,
        max(company_ape_code)         as company_ape_code,
        max(company_name)             as company_name,
        max(first_activity_datetime)  as first_activity_datetime,
        max(total_events_count)       as total_events_count,
        max(company_name)             as company_name,
        max(company_address)          as company_address,
        max(company_code_postal)      as company_code_postal,
        max(company_libelle_commune)  as company_libelle_commune,
        max(company_code_commune)     as company_code_commune,
        max(company_code_departement) as company_code_departement,
        max(company_code_region)      as company_code_region
    from
        {{ ref('sentinelle_waste_quantity_produced_by_siret') }}
    group by
        1
    having
        company_ape_code is not null
),

full_grid as (
    select
        c.company_siret,
        c.first_activity_datetime,
        c.total_events_count,
        c.company_name,
        c.company_address,
        c.company_code_postal,
        c.company_libelle_commune,
        c.company_code_commune,
        c.company_code_departement,
        c.company_code_region,
        c.company_ape_code,
        wqpbac.waste_code,
        wqpbac.waste_quantity_share as waste_quantity_share_ref
    from
        {{ ref('sentinelle_waste_quantity_produced_by_ape_code') }} as wqpbac
    inner join companies as c
        on
            wqpbac.ape_code = c.company_ape_code
)

select
    fg.*,
    coalesce(pq.waste_quantity_share, 0) as waste_quantity_share
from
    full_grid as fg
left join
    {{ ref('sentinelle_waste_quantity_produced_by_siret') }} as pq
    on
        fg.company_siret = pq.company_siret
        and fg.waste_code = pq.waste_code
