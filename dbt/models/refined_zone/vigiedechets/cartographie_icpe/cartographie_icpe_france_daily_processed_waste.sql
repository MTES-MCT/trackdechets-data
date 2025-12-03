{{
  config(
    materialized = 'table',
    )
}}

with stats as (
    select
        if(not match(rubrique, '^2791.*'), substring(rubrique, 1, 6), '2791')
            as rubrique,
        count(distinct code_aiot)
            as nombre_installations,
        sum(quantite_autorisee)
            as quantite_autorisee
    from
        {{ ref('cartographie_icpe_installations') }}
    group by
        1
),

waste_processed_grouped as (
    select
        rubrique,
        day_of_processing,
        sum(quantite_traitee) as quantite_traitee
    from
        {{ ref('cartographie_icpe_departements_daily_processed_waste') }}
    group by
        rubrique,
        day_of_processing
),

waste_stats as (
    select
        w.*,
        s.nombre_installations,
        s.quantite_autorisee
    from
        waste_processed_grouped as w
    right join stats as s
        on
            w.rubrique = s.rubrique
    order by rubrique, day_of_processing
)

select
    w.rubrique,
    w.day_of_processing,
    w.nombre_installations,
    w.quantite_traitee,
    w.quantite_autorisee
from waste_stats as w
