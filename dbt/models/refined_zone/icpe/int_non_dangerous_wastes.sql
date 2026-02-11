{{
  config(
    materialized = 'view',
    )
}}

with installations as (
    select * from {{ ref('int_non_dangerous_installations') }}
),

dnd_wastes as (
    select
        report_for_company_siret as siret,
        reception_date           as day_of_processing,
        operation_code           as code_traitement,
        sum(weight_value)        as quantite
    from {{ ref('registry_incoming_waste') }}
    where
        reception_date >= '2022-01-01'
        and report_for_company_siret in (
            select siret
            from
                installations
        )
    group by 1, 2, 3
),

texs_wastes as (
    select
        report_for_company_siret as siret,
        reception_date           as day_of_processing,
        operation_code           as code_traitement,
        sum(weight_value)        as quantite
    from {{ ref('latest_registry_incoming_texs') }}
    where
        -- filter out cancelled and previous versions of registry
        not is_cancelled
        and is_latest

        and reception_date >= '2022-01-01'
        and not ({{ dangerous_waste_filter('latest_registry_incoming_texs') }})
        and report_for_company_siret in (
            select siret
            from
                installations
        )
    group by 1, 2, 3
),

bsda_wastes as (
    select
        destination_company_siret         as siret,
        destination_reception_date        as day_of_processing,
        destination_operation_code        as code_traitement,
        sum(destination_reception_weight) as quantite
    from {{ ref('bsda') }}
    where
        destination_reception_date >= '2022-01-01'
        and destination_company_siret in (
            select siret
            from
                installations
        )
        and destination_operation_code in ('D5')
        -- Actuellement le seul code final pour les BSDA. 
    group by 1, 2, 3
),

plaster_wastes as (
    select
        recipient_company_siret   as siret,
        toDate(received_at)       as day_of_processing,
        processing_operation_done as code_traitement,
        sum(quantity_received)    as quantite
    from {{ ref('bsdd') }}
    where
        received_at >= '2022-01-01'
        and recipient_company_siret in (
            select siret
            from
                installations
        )
        and waste_details_code = '17 08 01*'
    group by 1, 2, 3

),

wastes as (
    select
        siret,
        day_of_processing,
        code_traitement,
        quantite
    from dnd_wastes
    union all
    select
        siret,
        day_of_processing,
        code_traitement,
        quantite
    from texs_wastes
    union all
    select
        siret,
        day_of_processing,
        code_traitement,
        toFloat64(quantite) as quantite
    from bsda_wastes
    union all
    select
        siret,
        day_of_processing,
        code_traitement,
        toFloat64(quantite) as quantite
    from plaster_wastes
)

select 
    siret,
    day_of_processing,
    code_traitement,
    quantite
from wastes