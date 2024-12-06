{{
  config(
    materialized = 'table',
    indexes=[{"columns":["code_region_insee"]},{"columns":["rubrique"]}]
    )
}}

with stats as (
    select
        code_region_insee,
        case
            when rubrique !~* '^2791.*' then substring(rubrique for 6)
            else '2791' -- take into account the 'alineas' of 2791
        end                       as rubrique,
        count(distinct code_aiot) as nombre_installations,
        sum(quantite_autorisee)   as quantite_autorisee
    from
        {{ ref('installations_icpe_2024') }}
    group by
        1,
        2
),

waste_processed_grouped as (
    select
        code_region_insee,
        rubrique,
        day_of_processing,
        sum(quantite_traitee) as quantite_traitee
    from
        {{ ref('icpe_departements_daily_processed_waste') }}
    group by
        code_region_insee,
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
            w.code_region_insee = s.code_region_insee
            and w.rubrique = s.rubrique
    order by code_region_insee, rubrique, day_of_processing
)

select
    w.code_region_insee,
    cg.libelle as nom_region,
    w.rubrique,
    w.day_of_processing::date,
    w.nombre_installations,
    w.quantite_traitee,
    w.quantite_autorisee
from waste_stats as w
left join
    {{ ref('code_geo_regions') }} as cg
    on w.code_region_insee = cg.code_region
