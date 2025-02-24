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
        toYear(processed_at)                    as annee,
        multiIf(processing_operation like 'R%','Déchet valorisé',processing_operation like 'D%','Déchet éliminé','Autre')          as type_operation,
        sum(
            if(quantity_received > 60,quantity_received / 1000,quantity_received)
        )                    as quantite_traitee
    from
        {{ ref('bordereaux_enriched') }}
    where
    /* Uniquement déchets dangereux */
        ({{ dangerous_waste_filter('bordereaux_enriched') }}
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
        and processed_at < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
    group by
        4,
        1,
        2,
        3
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
