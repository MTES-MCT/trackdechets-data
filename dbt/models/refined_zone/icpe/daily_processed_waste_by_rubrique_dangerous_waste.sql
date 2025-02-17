{{
  config(
    materialized = 'table',
    indexes=[{"columns":["siret","day_of_processing","rubrique"]}]
    )
}}

with installations as (
    select
        siret,
        substring(rubrique for 6)     as rubrique,
        max(raison_sociale)           as raison_sociale,
        array_agg(distinct code_aiot) as codes_aiot,
        sum(quantite_totale)          as quantite_autorisee
    from
        {{ ref('installations_rubriques_2024') }}
    where
        siret is not null
        and rubrique in ('2770', '2790', '2760-1')
        and etat_technique_rubrique = 'Exploité'
        and etat_administratif_rubrique = 'En vigueur'
        and libelle_etat_site = 'Avec titre'
    group by
        1,
        2
),

wastes as (
    select
        b.destination_company_siret as siret,
        b.processing_operation,
        date_trunc(
            'day',
            b.processed_at
        )                           as day_of_processing,
        sum(b.quantity_received)    as quantite_traitee
    from
        {{ ref('bordereaux_enriched') }} as b
    where
        b.destination_company_siret in (
            select siret
            from
                installations
        )
        and b.processed_at >= '2022-01-01'
        and (
            b.waste_code ~* '.*\*$'
            or coalesce(b.waste_pop, false)
            or coalesce(b.waste_is_dangerous, false)
        )
    group by
        b.destination_company_siret,
        date_trunc(
            'day',
            b.processed_at
        ),
        b.processing_operation
),

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
        wastes.siret,
        wastes.day_of_processing,
        mrco.rubrique
)

select *
from wastes_rubriques
