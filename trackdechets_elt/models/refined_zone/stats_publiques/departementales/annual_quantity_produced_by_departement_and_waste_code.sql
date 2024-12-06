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
        extract(
            'year'
            from
            date_trunc(
                'year',
                taken_over_at
            )
        )::int                 as annee,
        be.emitter_departement as code_departement_insee,
        be.waste_code          as code_dechet,
        max(be.emitter_region) as code_region_insee,
        sum(
            case
                when quantity_received > 60 then quantity_received / 1000
                else quantity_received
            end
        )                      as quantite_produite
    from
        {{ ref('bordereaux_enriched') }} as be
    where
    /* Uniquement déchets dangereux */
        (
            waste_code ~* '.*\*$'
            or coalesce(
                waste_pop,
                false
            )
            or coalesce(
                waste_is_dangerous,
                false
            )
        )
        /* Pas de bouillons */
        and not is_draft
        /* Uniquement les non TTRs */
        and emitter_company_siret not in (select siret from ttr_list)
        /* Uniquement les données jusqu'à la dernière semaine complète */
        and be.taken_over_at between '2020-01-01' and date_trunc(
            'week',
            now()
        )
        and be._bs_type != 'BSFF'
    group by extract(
        'year'
        from
        date_trunc(
            'year',
            taken_over_at
        )
    ), be.emitter_departement, be.waste_code
),

bsff_data as (
    select
        extract(
            'year'
            from
            date_trunc(
                'year',
                beff.transporter_transport_taken_over_at
            )
        )::int                   as annee,
        beff.emitter_departement as code_departement_insee,
        beff.waste_code          as code_dechet,
        max(beff.emitter_region) as code_region_insee,
        sum(
            case
                when acceptation_weight > 60 then acceptation_weight / 1000
                else acceptation_weight
            end
        )                        as quantite_produite
    from
        {{ ref('bsff_packaging') }} as bp
    left join {{ ref('bsff_enriched') }} as beff
        on
            bp.bsff_id = beff.id
    where
    /* Uniquement déchets dangereux */
        acceptation_waste_code ~* '.*\*$'
        /* Uniquement les non TTRs */
        and beff.emitter_company_siret not in (select siret from ttr_list)
        /* Uniquement les données jusqu'à la dernière semaine complète */
        and beff.transporter_transport_taken_over_at between '2020-01-01' and date_trunc(
            'week',
            now()
        )
    group by
        extract(
            'year'
            from
            date_trunc(
                'year',
                beff.transporter_transport_taken_over_at
            )
        ), beff.emitter_departement, beff.waste_code
),

merged_data as (
    select
        coalesce(a.annee, b.annee) as annee,
        coalesce(
            a.code_departement_insee, b.code_departement_insee
        )                          as code_departement_insee,
        coalesce(
            a.code_region_insee, b.code_region_insee
        )                          as code_region_insee,
        coalesce(
            a.code_dechet, b.code_dechet
        )                          as code_dechet,
        coalesce(a.quantite_produite, 0)
        + coalesce(
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
