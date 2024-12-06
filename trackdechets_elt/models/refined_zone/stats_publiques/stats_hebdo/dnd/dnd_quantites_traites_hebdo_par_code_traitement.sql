{{
  config(
    materialized = 'table',
    )
}}

select
    date_trunc(
        'week',
        date_reception
    )                                  as semaine,
    code_traitement                    as code_operation,
    case
        when code_traitement like 'R%' then 'Déchet valorisé'
        when code_traitement like 'D%' then 'Déchet éliminé'
        else 'Autre'
    end                                as type_operation,
    sum(
        case
            when quantite > 60
                then quantite / 1000
            else quantite
        end
    ) filter (where code_unite = 'T')  as quantite_traitee,
    sum(
        case
            when quantite > 60
                then quantite / 1000
            else quantite
        end
    ) filter (where code_unite = 'M3') as volume_traite
from {{ ref('dnd_entrant') }}
group by 1, 2
order by 1 desc
