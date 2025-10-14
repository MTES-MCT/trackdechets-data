{{
  config(
    materialized = 'table',
    tags =  ["fiche-etablissements"]
    )
}}

with 
    
    installations as (select * from {{ ref('int_non_dangerous_installations') }}),
    
    wastes as (select * from {{ ref('int_non_dangerous_wastes') }}),

wastes_rubriques as (
    select
        wastes.siret,
        wastes.date_reception as day_of_processing,
        mrco.rubrique,
        sum(quantite)         as quantite_traitee
    from
        wastes
    left join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            wastes.code_traitement = mrco.code_operation
            and (
                match(rubrique,'^2771.*|^2791.*|^2760\-2.*')
            )
    group by
        wastes.siret,
        wastes.date_reception,
        mrco.rubrique

)

select
    i.siret,
    i.rubrique,
    i.raison_sociale,
    i.codes_aiot,
    i.quantite_autorisee,
    i.objectif_quantite_traitee,
    wr.day_of_processing,
    wr.quantite_traitee
from
    installations i
left join wastes_rubriques as wr on
    installations.siret = wr.siret and installations.rubrique = wr.rubrique
