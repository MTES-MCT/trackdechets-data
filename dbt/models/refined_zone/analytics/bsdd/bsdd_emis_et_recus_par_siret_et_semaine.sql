{{
  config(
    materialized = 'table',
    )
}}
with bsdd as (
    select
        id,
        emitter_company_siret,
        recipient_company_siret,
        sent_at,
        received_at
    from
        {{ ref('bsdd') }}
    where
        not is_deleted
        and status != 'DRAFT'
        and (
            {{ dangerous_waste_filter('bsdd') }}
        )

)

select
    emitter_company_siret as siret,
    'emis'                as flux,
    date_trunc(
        'week',
        sent_at
    )                     as semaine,
    count(id)             as nombre_bordereaux
from bsdd
group by
    emitter_company_siret,
    date_trunc(
        'week',
        sent_at
    )
union all
select
    recipient_company_siret as siret,
    'reçus'                as flux,
    date_trunc(
        'week',
        received_at
    )                       as semaine,
    count(*)                as nombre_bordereaux
from
    bsdd
group by
    recipient_company_siret,
    date_trunc(
        'week',
        received_at
    )
