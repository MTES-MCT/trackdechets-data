{{
  config(
    materialized = 'table',
    )
}}

with agg_data as (
    select
        destination_region   as code_region_insee,
        _bs_type             as type_bordereau,
        processing_operation as code_operation,
        date_part(
            'year',
            processed_at
        )                    as annee,
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
        and not is_draft
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
    group by
        date_part(
            'year',
            processed_at
        ),
        destination_region,
        _bs_type,
        processing_operation
)

select
    annee,
    code_region_insee,
    type_bordereau,
    code_operation,
    type_operation,
    quantite_traitee
from agg_data
order by
    annee desc, code_region_insee asc, type_bordereau asc, code_operation asc
