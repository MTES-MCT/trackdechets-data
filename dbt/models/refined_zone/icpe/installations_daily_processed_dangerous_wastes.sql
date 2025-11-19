{{
  config(
    materialized = 'table',
    tags =  ["fiche-etablissements"]
    )
}}

with installations as (
    select * from {{ ref('int_dangerous_installations') }}
),

wastes as (select * from {{ ref('int_dangerous_wastes') }}),

wastes_rubriques as (
    select
        wastes.siret,
        wastes.day_of_processing,
        mrco.rubrique,
        sum(quantite_traitee) as quantite_traitee
    from
        wastes
    left join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            wastes.processing_operation = mrco.code_operation
            and (
                rubrique in ('2770', '2790', '2760-1')
            )
    group by
        wastes.siret,
        wastes.day_of_processing,
        mrco.rubrique
)

select
    i.siret,
    i.rubrique,
    i.raison_sociale,
    i.codes_aiot,
    i.quantite_autorisee,
    wr.day_of_processing,
    wr.quantite_traitee
from
    installations as i
left join wastes_rubriques as wr on
    installations.siret = wr.siret and installations.rubrique = wr.rubrique
