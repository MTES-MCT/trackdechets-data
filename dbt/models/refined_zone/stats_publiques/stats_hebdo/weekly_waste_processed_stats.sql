{{
  config(
    materialized = 'table',
    indexes = [{"columns":["semaine","type_bordereau","code_operation"],"unique":true}]
    )
}}

with bs_data as (
    select
        _bs_type             as type_bordereau,
        processing_operation as code_operation,
        date_trunc(
            'week',
            processed_at
        )                    as semaine,
        case
            when processing_operation like 'R%' then 'Déchet valorisé'
            when processing_operation like 'D%' then 'Déchet éliminé'
            else 'Autre'
        end                  as type_operation,
        sum(
            case
                when quantity_received > 60 then quantity_received / 1000
                else quantity_received
            end
        )                    as quantite_traitee
    from
        {{ ref('bordereaux_enriched') }}
    where
    /* Uniquement déchets dangereux */
        (
            waste_code ~* '.*\*$'
            or coalesce(
                waste_pop,
                false
            )
            or coalesce(
                waste_is_dangerous,
                false
            )
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
        and processed_at < date_trunc(
            'week',
            now()
        )
        and _bs_type != 'BSFF'
    group by
        date_trunc(
            'week',
            processed_at
        ),
        _bs_type,
        processing_operation
),

bsff_data as (
    select
        'BSFF'         as type_bordereau,
        operation_code as code_operation,
        date_trunc(
            'week',
            operation_date
        )              as semaine,
        case
            when operation_code like 'R%' then 'Déchet valorisé'
            when operation_code like 'D%' then 'Déchet éliminé'
            else 'Autre'
        end            as type_operation,
        sum(
            case
                when acceptation_weight > 60 then acceptation_weight / 1000
                else acceptation_weight
            end
        )              as quantite_traitee
    from
        {{ ref('bsff_packaging') }}
    where
    /* Uniquement déchets dangereux */
        acceptation_waste_code ~* '.*\*$'
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
        and operation_date < date_trunc(
            'week',
            now()
        )
    group by
        date_trunc(
            'week',
            operation_date
        ),
        operation_code
),

merged_data as (
    select *
    from
        bs_data
    union all
    select * from bsff_data
)

select
    semaine::date,
    type_bordereau,
    code_operation,
    type_operation,
    quantite_traitee
from merged_data
where semaine >= '2020-01-01'
order by semaine desc, type_bordereau asc, code_operation asc
