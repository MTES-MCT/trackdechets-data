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
    ii.code_aiot,
    ii.rubrique as "rubrique",
    ii.raison_sociale,
    ii.siret as "siret",
    ii.adresse1,
    ii.adresse2,
    ii.code_postal,
    ii.commune as "commune",
    ii.code_commune_insee,
    ii.code_departement_insee,
    ii.code_region_insee,
    ii.latitude,
    ii.longitude,
    ii.quantite_autorisee,
    ii.unite,
    toNullable(toDate(idpw.day_of_processing)) as "day_of_processing",
    idpw.quantite_traitee,
    toNullable(c.capacite_50pct) as quantite_objectif
from installations_data as ii
left join {{ ref('daily_processed_waste_by_rubrique') }} as idpw
    on ii.siret = idpw.siret and ii.rubrique = idpw.rubrique
left join {{ ref('isdnd_capacites_limites_50pct') }} c 
    on ii.siret=c.siret and match(ii.rubrique,'^2760\-2.*')