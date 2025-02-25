{{
  config(
    materialized = 'table',
    )
}}

select
    toStartOfWeek(
        date_reception,1
    )                                  as semaine,
    code_traitement                    as code_operation,
    multiIf(
            code_traitement like 'R%','Déchet valorisé',
            code_traitement like 'D%','Déchet éliminé',
            'Autre'
    )                     as type_operation,
    sumIf(
        if(
            quantite > 60,
            quantite / 1000,
            quantite
        ),code_unite = 'T'
        
    ) as quantite_traitee,
    sumIf(
        if(
             quantite > 60,
            quantite / 1000,
            quantite
        ),
        code_unite = 'M3'
    ) as volume_traite
from {{ ref('dnd_entrant') }}
group by 1, 2
order by 1 desc
