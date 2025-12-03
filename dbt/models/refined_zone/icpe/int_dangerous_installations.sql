{{
  config(
    materialized = 'view',
    )
}}

with installations as (
    select
        siret,
        substring(rubrique, 1, 6)     as rubrique,
        max(raison_sociale)           as raison_sociale,
        groupArray(distinct code_aiot) as codes_aiot,
        sum(quantite_totale)          as quantite_autorisee
    from
        {{ ref('installations_rubriques_2025') }}
    where
        siret is not null
        and rubrique in ('2770', '2790', '2760-1')
        and etat_technique_rubrique = 'Exploité'
        and etat_administratif_rubrique = 'En vigueur'
        and libelle_etat_site = 'Avec titre'
    group by
        1,
        2
)

select * from installations