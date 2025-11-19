{{
  config(
    materialized = 'table',
    tags = ['maps']
    )
}}

with installations as (
    select
        siret,
        raison_sociale,
        codes_aiot,
        quantite_autorisee,
        substring(rubrique, 1, 6) as rubrique
    from {{ ref('int_dangerous_installations') }}
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
    inner join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            wastes.processing_operation = mrco.code_operation
            and mrco.rubrique in ('2770', '2790', '2760-1')
    group by
        1, 2, 3
)

select *
from wastes_rubriques
