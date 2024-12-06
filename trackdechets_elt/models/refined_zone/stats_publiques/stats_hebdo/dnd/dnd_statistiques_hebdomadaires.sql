{{
  config(
    materialized = 'table',
    )
}}

with entrants as (
    select
        date_trunc(
            'week', date_reception
        )                                    as semaine,
        count(
            distinct id
        )                                    as nombre_declarations_dnd_entrant,
        sum(case
            when quantite > 60
                then quantite / 1000
            else quantite
        end) filter (where code_unite = 'T') as quantite_dnd_entrant,
        sum(quantite) filter (
            where code_unite = 'M3'
        )                                    as volume_dnd_entrant
    from {{ ref("dnd_entrant") }}
    where date_trunc('week', date_reception) < date_trunc('week', now())
    group by 1
),

sortants as (
    select
        date_trunc(
            'week', date_expedition
        )                                    as semaine,
        count(
            distinct id
        )                                    as nombre_declarations_dnd_sortant,
        sum(case
            when quantite > 60
                then quantite / 1000
            else quantite
        end) filter (where code_unite = 'T') as quantite_dnd_sortant,
        sum(quantite) filter (
            where code_unite = 'M3'
        )                                    as volume_dnd_sortant
    from {{ ref("dnd_sortant") }}
    where date_trunc('week', date_expedition) < date_trunc('week', now())
    group by 1
)

select
    coalesce(entrants.semaine, sortants.semaine) as semaine,
    coalesce(
        entrants.nombre_declarations_dnd_entrant, 0
    )                                            as nombre_declarations_dnd_entrant,
    coalesce(
        sortants.nombre_declarations_dnd_sortant, 0
    )                                            as nombre_declarations_dnd_sortant,
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
