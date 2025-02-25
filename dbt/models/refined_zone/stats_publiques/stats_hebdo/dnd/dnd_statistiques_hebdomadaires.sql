{{
  config(
    materialized = 'table',
    )
}}

with entrants as (
    select
        toStartOfWeek(toDateTime(date_reception),1,'Europe/Paris')                                    as semaine,
        count(
            distinct id
        )                                    as nombre_declarations_dnd_entrant,
        sumIf(if(quantite > 60, quantite / 1000,quantite), code_unite = 'T') as quantite_dnd_entrant,
        sumIf(quantite,  code_unite = 'M3')                                   as volume_dnd_entrant
    from {{ ref("dnd_entrant") }}
    where toStartOfWeek(toDateTime(date_reception),1,'Europe/Paris') < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
    group by 1
),

sortants as (
    select
        toStartOfWeek(toDateTime(date_expedition),1,'Europe/Paris')             as semaine,
        count(
            distinct id
        )                                    as nombre_declarations_dnd_sortant,
        sumIf(if(quantite > 60, quantite / 1000,quantite), code_unite = 'T') as quantite_dnd_sortant,
        sumIf(quantite,code_unite = 'M3')                              as volume_dnd_sortant
    from {{ ref("dnd_sortant") }}
    where toStartOfWeek(toDateTime(date_expedition),1,'Europe/Paris') < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
    group by 1
)

select
    coalesce(entrants.semaine, sortants.semaine) as semaine,
    coalesce(
        entrants.nombre_declarations_dnd_entrant, 0
    )
        as nombre_declarations_dnd_entrant,
    coalesce(
        sortants.nombre_declarations_dnd_sortant, 0
    )
        as nombre_declarations_dnd_sortant,
    coalesce(
        entrants.quantite_dnd_entrant, 0
    )                                            as quantite_dnd_entrant,
    coalesce(entrants.volume_dnd_entrant, 0)     as volume_dnd_entrant,
    coalesce(
        sortants.quantite_dnd_sortant, 0
    )                                            as quantite_dnd_sortant,
    coalesce(sortants.volume_dnd_sortant)        as volume_dnd_sortant
from entrants
full outer join sortants on entrants.semaine = sortants.semaine
order by 1 desc
