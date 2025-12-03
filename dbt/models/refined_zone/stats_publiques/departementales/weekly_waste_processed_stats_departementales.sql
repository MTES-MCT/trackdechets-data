{{
  config(
    materialized = 'table',
    )}}

with preprocessed_data as (
    select
        be.destination_departement as departement,
        be.processing_operation    as code_operation,
        toStartOfWeek(be.processed_at,1,'Europe/Paris')                          as semaine,
        multiIf(
            be.processing_operation like 'R%','Déchet valorisé',
            be.processing_operation like 'D%','Déchet éliminé',
            'Autre'
            ) as type_operation,
        coalesce(
            be.quantity_received, toDecimal256(be.accepted_quantity_packagings,30)
        )                          as quantite_traitee
    from {{ ref('bordereaux_enriched') }} as be
    where
        /* Uniquement les déchets dangereux */
        {{ dangerous_waste_filter('bordereaux_enriched') }}
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
        and be.processed_at < toStartOfWeek(
            now('Europe/Paris'),1,'Europe/Paris'
        )
        and be.emitter_departement is not null
        and be.destination_departement is not null
        and be.quantity_received is not null
)

select
    departement,
    code_operation,
    semaine,
    max(type_operation) as type_operation,
    sum(
        if(quantite_traitee > 60,quantite_traitee / 1000,quantite_traitee)
    )                   as quantite_traitee
from preprocessed_data
group by 1, 2, 3
