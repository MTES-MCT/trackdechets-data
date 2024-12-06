{{
  config(
    materialized = 'table',
    indexes=[{"columns":["siret","date_reception","rubrique"], "unique":True}]
    )
}}

with installations as (
    select
        siret,
        case
            when rubrique !~* '^2791.*' then substring(rubrique for 6)
            else '2791' -- take into account the 'alineas'
        end                           as rubrique,
        max(raison_sociale)           as raison_sociale,
        array_agg(distinct code_aiot) as codes_aiot,
        sum(quantite_totale)          as quantite_autorisee
    from
        {{ ref('installations_rubriques_2024') }}
    where
        siret is not null
        and rubrique ~* '^2771.*|^2791.*|^2760\-2.*'
        and etat_technique_rubrique = 'Exploité'
        and etat_administratif_rubrique = 'En vigueur'
        and libelle_etat_site = 'Avec titre'
    group by
        1,
        2
),

dnd_wastes as (
    select
        etablissement_numero_identification as siret,
        date_reception,
        code_traitement,
        sum(quantite)                       as quantite
    from {{ ref('dnd_entrant') }}
    where
        code_unite = 'T'
        and date_reception >= '2022-01-01'
        and etablissement_numero_identification in (
            select siret
            from
                installations
        )
    group by 1, 2, 3
),

texs_wastes as (
    select
        etablissement_numero_identification as siret,
        date_reception,
        code_traitement,
        sum(quantite)                       as quantite
    from {{ ref('texs_entrant') }}
    where
        code_unite = 'T'
        and date_reception >= '2022-01-01'
        and etablissement_numero_identification in (
            select siret
            from
                installations
        )
    group by 1, 2, 3
),

wastes as (
    select
        siret,
        date_reception,
        code_traitement,
        quantite
    from dnd_wastes
    union all
    select
        siret,
        date_reception,
        code_traitement,
        quantite
    from texs_wastes
),

wastes_rubriques as (
    select
        wastes.siret,
        wastes.date_reception,
        mrco.rubrique,
        sum(quantite) as quantite
    from
        wastes
    inner join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            wastes.code_traitement = mrco.code_operation
            and mrco.rubrique ~* '^2771.*|^2791.*|^2760\-2.*'
    group by
        wastes.siret,
        wastes.date_reception,
        mrco.rubrique

)

select *
from wastes_rubriques
