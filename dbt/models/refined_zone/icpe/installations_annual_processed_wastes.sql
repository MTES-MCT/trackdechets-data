{{
  config(
    materialized = 'table',
    indexes=[{"columns":["siret"]},{"columns":["rubrique"]}]
    )
}}

with installations as (
    select
        siret,
        rubrique || coalesce('-' || alinea, '') as rubrique,
        max(raison_sociale)                     as raison_sociale,
        avg(longitude)                          as longitude,
        avg(latitude)                           as latitude,
        max(code_postal)                        as code_postal,
        max(code_insee)                         as code_commune_insee,
        max(etat_activite)                      as etat_activite,
        array_agg(distinct code_aiot)           as codes_aiot,
        sum(quantite_totale)                    as quantite_autorisee
    from
        {{ ref('installations_rubriques') }}
    where
        siret is not null
        and (
            rubrique in ('2770', '2790')
            or (
                rubrique = '2760'
                and alinea = '1'
            )
        )
    group by
        siret, rubrique || coalesce('-' || alinea, '')
),

wastes as (
    select
        b.destination_company_siret as siret,
        b.processing_operation,
        extract(
            year
            from
            b.processed_at
        )                           as "year",
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
            waste_code ~* '.*\*$'
            or coalesce(waste_pop, false)
            or coalesce(waste_is_dangerous, false)
        )
    group by
        b.destination_company_siret,
        extract(
            year
            from
            b.processed_at
        ),
        b.processing_operation
),

wastes_rubriques as (
    select
        wastes.siret,
        wastes.year,
        mrco.rubrique,
        sum(quantite_traitee)       as quantite_traitee,
        sum(quantite_traitee) / 365 as quantite_moyenne_j
    from
        wastes
    left join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            wastes.processing_operation = mrco.code_operation
    group by wastes.siret, wastes.year, mrco.rubrique
)

select
    installations.*,
    wr."year",
    wr.quantite_traitee,
    wr.quantite_moyenne_j
from
    installations
left join wastes_rubriques as wr on
    installations.siret = wr.siret and installations.rubrique = wr.rubrique
