{{
  config(
    materialized = 'table',
    tags = ['maps']
    )
}}

with installations as (select * from {{ ref('int_non_dangerous_installations') }}),
wastes as (
    select * from {{ ref('int_non_dangerous_wastes') }}
),

wastes_rubriques as (
    select
        wastes.siret,
        wastes.day_of_processing,
        mrco.rubrique,
        sum(quantite) as quantite_traitee
    from
        wastes
    inner join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            wastes.code_traitement = mrco.code_operation
            and match(mrco.rubrique, '^2771.*|^2791.*|^2760\-2.*')
    group by
        wastes.siret,
        wastes.day_of_processing,
        mrco.rubrique

)

select 
    siret,
    day_of_processing,
    rubrique,
    quantite_traitee
from wastes_rubriques
    

