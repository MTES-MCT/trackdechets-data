{{
  config(
    materialized = 'table',
    )
}}

with preprocessed_data as (
    select
        be.emitter_departement     as departement_origine,
        be.destination_departement as departement_destination,
        date_trunc(
            'week', be.processed_at
        )                          as semaine,
        coalesce(
            be.quantity_received, be.accepted_quantity_packagings
        )                          as quantite_traitee
    from {{ ref('bordereaux_enriched') }} as be
    where {{ dangerous_waste_filter('bordereaux_enriched') }}
    /* Pas de bouillons */
    and be.status != 'DRAFT'
    /* Uniquement codes opérations finales */
    and be.processing_operation not in (
        'D9',
        'D13',
        'D14',
        'D15',
        'R12',
        'R13'
    )
    /* Uniquement les données jusqu'à la dernière semaine complète */
    and be.processed_at < date_trunc(
        'week',
        now()
    )
    and be.emitter_departement is not null
    and be.destination_departement is not null
    and be.quantity_received is not null
)

select
    departement_origine,
    departement_destination,
    semaine,
    sum(
        case
            when quantite_traitee > 60 then quantite_traitee / 1000
            else quantite_traitee
        end
    ) as quantite_traitee
from preprocessed_data
group by 1, 2, 3
