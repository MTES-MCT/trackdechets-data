{{
  config(
    materialized = 'table',
    )
}}

with installations_data as (
    select
        i.code_aiot,
        if(not match(rubrique,'^2791.*'),substring(rubrique,1,6),'2791')                           as rubrique,
        max(i.raison_sociale)         as raison_sociale,
        max(i.siret)                  as siret,
        max(i.adresse1)               as adresse1,
        max(i.adresse2)               as adresse2,
        max(i.code_postal)            as code_postal,
        max(i.commune)                as commune,
        max(i.code_commune_insee)     as code_commune_insee,
        max(i.code_departement_insee) as code_departement_insee,
        max(i.code_region_insee)      as code_region_insee,
        max(i.latitude)               as latitude,
        max(i.longitude)              as longitude,
        sum(i.quantite_autorisee)     as quantite_autorisee,
        max(i.unite)                  as unite
    from {{ ref('installations_icpe_2024') }} as i
    group by
        1,
        2
)

select
    ii.*,
    toNullable(toDate(idpw.day_of_processing)) as "day_of_processing",
    idpw.quantite_traitee
from installations_data as ii
left join {{ ref('daily_processed_waste_by_rubrique') }} as idpw
    on ii.siret = idpw.siret and ii.rubrique = idpw.rubrique
