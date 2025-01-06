{{
  config(
    materialized = 'table',
    indexes = [{"columns":["code_departement","rubrique"], "unique": True}]
    )
}}

with rubriques_data as (
    select
        ir.code_aiot,
        ir.siret,
        ir.quantite_totale,
        ir.etat_activite,
        cgc.code_departement,
        cgc.code_region,
        ir.rubrique || coalesce(
            '-' || ir.alinea,
            ''
        ) as rubrique,
        coalesce(
            ir.code_insee,
            se.code_commune_etablissement
        ) as "code_commune_insee"
    from
        {{ ref('installations_rubriques') }} as ir
    left join {{ ref('stock_etablissement') }} as se
        on
            ir.siret = se.siret
    left join {{ ref('code_geo_communes') }} as cgc
        on
            coalesce(
                code_insee,
                se.code_commune_etablissement
            ) = cgc.code_commune
    where
        (
            rubrique = '2770'
            or (
                rubrique = '2760'
                and alinea = '1'
            )
        )
        and ir.siret is not null
        and coalesce(
            code_insee,
            se.code_commune_etablissement
        ) is not null
),

wastes_data as (
    select
        b.destination_company_siret as siret,
        mrco.rubrique,
        sum(b.quantity_received)    as quantite_traitee
    from
        {{ ref('bordereaux_enriched') }} as b
    left join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            b.processing_operation = mrco.code_operation
    where
        b.destination_company_siret in (
            select siret
            from
                rubriques_data
        )
        and date_part(
            'year',
            b.processed_at
        ) = 2023
        and (
            b.waste_code ~* '.*\*$'
            or coalesce(
                b.waste_pop,
                false
            )
            or coalesce(
                b.waste_is_dangerous,
                false
            )
        )
        and mrco.rubrique in ('2760-1', '2770', '2790')
    group by
        b.destination_company_siret,
        mrco.rubrique
)

select
    r.code_departement,
    r.rubrique,
    max(r.code_region)          as code_region,
    count(distinct r.siret)     as num_etablissements,
    count(distinct r.code_aiot) as num_installations,
    sum(r.quantite_totale)      as quantite_totale_autorisee,
    sum(w.quantite_traitee)     as quantite_totale_traitee
from
    rubriques_data as r
left join wastes_data as w
    on
        r.siret = w.siret
        and r.rubrique = w.rubrique
group by r.code_departement, r.rubrique
