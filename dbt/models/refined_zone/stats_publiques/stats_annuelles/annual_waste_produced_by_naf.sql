{{
  config(
    materialized = 'table',
)}}

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
        toYear(be.taken_over_at)        as annee,
        be.emitter_naf as naf,
        sum(if(be.quantity_received > 60,be.quantity_received / 1000,be.quantity_received))              as quantite_traitee
    from
        {{ ref('bordereaux_enriched') }} as be
    where
    /* Uniquement déchets dangereux */
        (
            {{ dangerous_waste_filter('bordereaux_enriched') }}
        )
        /* Pas de bouillons */
        and not be.is_draft
        /* Uniquement les non TTRs */
        and be.emitter_company_siret not in (select siret from ttr_list)
        /* Uniquement les données jusqu'à la dernière semaine complète */
        and be.taken_over_at between '2020-01-01' and toStartOfWeek(now(),1,'Europe/Paris')
        and be._bs_type != 'BSFF'
    group by 1,2
),

bsff_data as (
    select
        toYear(beff.transporter_transport_signature_date)   as annee,
        beff.emitter_naf as naf,
        sum(if(acceptation_weight > 60,acceptation_weight / 1000,acceptation_weight))                as quantite_traitee
    from
        {{ ref('bsff_packaging') }} as bp
    left join {{ ref('bsff_enriched') }} as beff
        on
            bp.bsff_id = beff.id
    where
    /* Uniquement déchets dangereux */
        match(acceptation_waste_code,'(?i).*\*$')
        /* Uniquement les non TTRs */
        and beff.emitter_company_siret not in (select siret from ttr_list)
        /* Uniquement les données jusqu'à la dernière semaine complète */
        and beff.transporter_transport_signature_date between '2020-01-01' and toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
    group by
        1,2
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
    naf.code_section,
    naf.libelle_section,
    naf.code_division,
    naf.libelle_division,
    naf.code_groupe,
    naf.libelle_groupe,
    naf.code_classe,
    naf.libelle_classe,
    naf.code_sous_classe,
    naf.libelle_sous_classe,
    annee,
    quantite_produite
from merged_data
left join {{ ref('nomenclature_activites_francaises') }} as naf
    on merged_data.naf = naf.code_sous_classe
where not ((naf.code_sous_classe is null) and (merged_data.naf is not null))
order by annee desc, code_sous_classe asc
