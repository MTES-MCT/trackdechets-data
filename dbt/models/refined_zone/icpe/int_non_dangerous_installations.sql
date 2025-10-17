{{
  config(
    materialized = 'view',
    )
}}

with installations as (
    select
        i.siret,
        -- take into account the 'alineas'
        if(match(rubrique, '^2791.*'), '2791', substring(rubrique, 1, 6))
            as rubrique,
        max(raison_sociale)
            as raison_sociale,
        groupArray(distinct code_aiot)
            as codes_aiot,
        sum(quantite_totale)
            as quantite_autorisee,
        max(c.capacite_50pct)          as objectif_quantite_traitee
    from
        {{ ref('installations_rubriques_2025') }} i
    left join {{ ref('isdnd_capacites_limites_50pct') }} c on i.siret=c.siret and match(rubrique,'^2760\-2.*')        
    where
        siret is not null
        and match(rubrique, '^2771.*|^2791.*|^2760\-2.*')
        and etat_technique_rubrique = 'Exploité'
        and etat_administratif_rubrique = 'En vigueur'
        and libelle_etat_site = 'Avec titre'
    group by
        1,
        2
)

select * from installations