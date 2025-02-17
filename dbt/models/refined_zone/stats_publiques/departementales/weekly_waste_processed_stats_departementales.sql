{{
  config(
    materialized = 'table',
    indexes = [{"columns":["departement","code_operation","semaine"],"unique":true}]
    )
}}

with preprocessed_data as (
    select
        be.destination_departement as departement,
        be.processing_operation    as code_operation,
        date_trunc(
            'week', be.processed_at
        )                          as semaine,
        case
            when be.processing_operation like 'R%' then 'Déchet valorisé'
            when be.processing_operation like 'D%' then 'Déchet éliminé'
            else 'Autre'
        end                        as type_operation,
        coalesce(
            be.quantity_received, be.accepted_quantity_packagings
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
        and be.processed_at < date_trunc(
            'week',
            now()
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
        case
            when quantite_traitee > 60 then quantite_traitee / 1000
            else quantite_traitee
        end
    )                   as quantite_traitee
from preprocessed_data
group by 1, 2, 3
