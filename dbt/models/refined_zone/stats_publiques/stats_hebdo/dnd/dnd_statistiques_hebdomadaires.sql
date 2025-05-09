{{
  config(
    materialized = 'table',
    )
}}

with entrants as (
    select
        toStartOfWeek(toDateTime(reception_date),1,'Europe/Paris')                                    as semaine,
        count(
            distinct id
        )                                    as nombre_declarations_dnd_entrant,
        sum(if(weight_value > 60, weight_value / 1000,weight_value)) as quantite_dnd_entrant,
        sum(volume)                                       as volume_dnd_entrant
    from {{ ref("registry_incoming_waste") }}
    where toStartOfWeek(toDateTime(reception_date),1,'Europe/Paris') < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
    group by 1
),

sortants as (
    select
        toStartOfWeek(toDateTime(dispatch_date),1,'Europe/Paris')             as semaine,
        count(
            distinct id
        )                                    as nombre_declarations_dnd_sortant,
        sum(if(weight_value > 60, weight_value / 1000,weight_value)) as quantite_dnd_sortant,
        sum(volume)                              as volume_dnd_sortant
    from {{ ref("registry_outgoing_waste") }}
    where toStartOfWeek(toDateTime(dispatch_date),1,'Europe/Paris') < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
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
