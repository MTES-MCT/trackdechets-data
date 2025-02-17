{{
  config(
    materialized = 'table',
    indexes = [{"columns":["annee","code_sous_classe"], "unique":true}]
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
                be.taken_over_at
            )
        )::int         as annee,
        be.emitter_naf as naf,
        sum(
            case
                when be.quantity_received > 60 then be.quantity_received / 1000
                else be.quantity_received
            end
        )              as quantite_traitee
    from
        {{ ref('bordereaux_enriched') }} as be
    where
    /* Uniquement déchets dangereux */
        (
            be.waste_code ~* '.*\*$'
            or coalesce(
                be.waste_pop,
                false
            )
            or coalesce(
                be.waste_is_dangerous,
                false
            )
        )
        /* Pas de bouillons */
        and not be.is_draft
        /* Uniquement les non TTRs */
        and be.emitter_company_siret not in (select siret from ttr_list)
        /* Uniquement les données jusqu'à la dernière semaine complète */
        and be.taken_over_at between '2020-01-01' and date_trunc(
            'week',
            now()
        )
        and be._bs_type != 'BSFF'
    group by date_trunc('year', be.taken_over_at), be.emitter_naf
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
        )::int           as annee,
        beff.emitter_naf as naf,
        sum(
            case
                when acceptation_weight > 60 then acceptation_weight / 1000
                else acceptation_weight
            end
        )                as quantite_traitee
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
        date_trunc(
            'year',
            beff.transporter_transport_taken_over_at
        ), beff.emitter_naf
),

merged_data as (
    select
        coalesce(a.annee, b.annee)        as annee,
        coalesce(a.naf, b.naf)            as naf,
        coalesce(a.quantite_traitee, 0)
        + coalesce(b.quantite_traitee, 0) as quantite_produite
    from grouped_data as a
    full outer join bsff_data as b on a.annee = b.annee and a.naf = b.naf
)

select
    naf.*,
    annee,
    quantite_produite
from merged_data
left join trusted_zone_insee.nomenclature_activites_francaises as naf
    on merged_data.naf = naf.code_sous_classe
where not ((naf.code_sous_classe is null) and (merged_data.naf is not null))
order by annee desc, code_sous_classe asc
