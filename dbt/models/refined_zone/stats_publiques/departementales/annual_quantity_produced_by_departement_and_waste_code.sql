{{
  config(
    materialized = 'table',
    )
}}

with ttr_list as (
    select distinct destination_company_siret as siret
    from
        {{ ref('bordereaux_enriched') }}
    where
        processing_operation in (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
        and processed_at is not null
),

grouped_data as (
    select
        TOYEAR(
            be.taken_over_at
        )                      as annee,
        be.emitter_departement
            as code_departement_insee,
        be.waste_code
            as code_dechet,
        MAX(
            be.emitter_region
        )                      as code_region_insee,
        SUM(
            IF(
                be.quantity_received > 60,
                be.quantity_received / 1000,
                be.quantity_received
            )
        )                      as quantite_produite
    from
        {{ ref('bordereaux_enriched') }} as be
    where
    /* Uniquement déchets dangereux */
        (
            MATCH(be.waste_details_code, '(?i).*\*$')
            or COALESCE(
                be.waste_pop,
                false
            )
            or COALESCE(
                be.waste_is_dangerous,
                false
            )
        )
        /* Pas de bouillons */
        and not be.is_draft
        /* Uniquement les non TTRs */
        and be.emitter_company_siret not in (select siret from ttr_list)
        /* Uniquement les données jusqu'à la dernière semaine complète */
        and be.taken_over_at between '2020-01-01' and TOSTARTOFWEEK(
            NOW('Europe/Paris'), 1, 'Europe/Paris'
        )
        and be._bs_type != 'BSFF'
    group by 1, 2, 3
),

bsff_data as (
    select
        TOYEAR(
            beff.transporter_transport_taken_over_at
        )                        as annee,
        beff.emitter_departement
            as code_departement_insee,
        beff.waste_code
            as code_dechet,
        MAX(
            beff.emitter_region
        )                        as code_region_insee,
        SUM(
            IF(
                acceptation_weight > 60,
                acceptation_weight / 1000,
                acceptation_weight
            )
        )                        as quantite_produite
    from
        {{ ref('bsff_packaging') }} as bp
    left join {{ ref('bsff_enriched') }} as beff
        on
            bp.bsff_id = beff.id
    where
    /* Uniquement déchets dangereux */
        MATCH(acceptation_waste_code, '(?i).*\*$')
        /* Uniquement les non TTRs */
        and beff.emitter_company_siret not in (select siret from ttr_list)
        /* Uniquement les données jusqu'à la dernière semaine complète */
        and beff.transporter_transport_taken_over_at between '2020-01-01' and TOSTARTOFWEEK(
            NOW('Europe/Paris'), 1, 'Europe/Paris'
        )
    group by
        1, 2, 3
),

merged_data as (
    select
        COALESCE(a.annee, b.annee) as annee,
        COALESCE(
            a.code_departement_insee, b.code_departement_insee
        )                          as code_departement_insee,
        COALESCE(
            a.code_region_insee, b.code_region_insee
        )                          as code_region_insee,
        COALESCE(
            a.code_dechet, b.code_dechet
        )                          as code_dechet,
        COALESCE(a.quantite_produite, 0)
        + COALESCE(
            b.quantite_produite, 0
        )                          as quantite_produite
    from grouped_data as a
    full outer join bsff_data as b on
        a.annee = b.annee
        and a.code_departement_insee = b.code_departement_insee
        and a.code_dechet = b.code_dechet
)

select
    m.annee,
    m.code_departement_insee,
    cd.libelle as libelle_departement,
    m.code_region_insee,
    cr.libelle as libelle_region,
    m.code_dechet,
    m.quantite_produite
from merged_data as m
left join {{ ref('code_geo_departements') }} as cd
    on m.code_departement_insee = cd.code_departement
left join {{ ref('code_geo_regions') }} as cr
    on m.code_region_insee = cr.code_region
where m.code_departement_insee is not null
order by m.annee desc, m.code_departement_insee asc, m.code_dechet asc
