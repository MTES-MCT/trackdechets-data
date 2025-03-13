{{
  config(
    materialized = 'table',
    )}}

with bs_data as (
    select
        _bs_type             as type_bordereau,
        processing_operation as code_operation,
        toStartOfWeek(
            processed_at,1,'Europe/Paris'
        )                    as semaine,
        multiIf(
            processing_operation like 'R%','Déchet valorisé',
            processing_operation like 'D%','Déchet éliminé',
            'Autre'
        ) as type_operation,
        sum(
            if(
                quantity_received > 60,quantity_received / 1000,quantity_received
            )
           )   as quantite_traitee
    from
        {{ ref('bordereaux_enriched') }}
    where
    /* Uniquement déchets dangereux */
        (
            {{ dangerous_waste_filter('bordereaux_enriched') }}
        )
        /* Pas de bouillons */
        and status != 'DRAFT'
        /* Uniquement codes opérations finales */
        and processing_operation not in (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
        /* Uniquement les données jusqu'à la dernière semaine complète */
        and processed_at < toStartOfWeek(
            now('Europe/Paris'),1,'Europe/Paris'
        )
        and _bs_type != 'BSFF'
    group by
        3,
        1,
        2
),

bsff_data as (
    select
        'BSFF'         as type_bordereau,
        operation_code as code_operation,
        toStartOfWeek(
            operation_date,1,'Europe/Paris'
        )              as semaine,
        multiIf(
            operation_code like 'R%','Déchet valorisé',
            operation_code like 'D%','Déchet éliminé',
            'Autre'
        )  as type_operation,
        toDecimal256(sum(
            if(
                acceptation_weight > 60,acceptation_weight / 1000,acceptation_weight
            )
        ),30)              as quantite_traitee
    from
        {{ ref('bsff_packaging') }}
    where
    /* Uniquement déchets dangereux */
        match(acceptation_waste_code,'.*\*$')
        /* Uniquement codes opérations finales */
        and operation_code not in (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
        /* Uniquement les données jusqu'à la dernière semaine complète */
        and operation_date < toStartOfWeek(
            now('Europe/Paris'),1,'Europe/Paris'
        )
    group by
        3,2
),

merged_data as (
    select *
    from
        bs_data
    union all
    select * from bsff_data
)

select
    toDate(semaine) as "semaine",
    type_bordereau,
    code_operation,
    type_operation,
    quantite_traitee
from merged_data
where semaine >= '2020-01-01'
order by semaine desc, type_bordereau asc, code_operation asc
