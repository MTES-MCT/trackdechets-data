{{
  config(
    materialized = 'table',
    indexes = [{'columns':['siret'],'unique':true}]
    )
}}

with bordereaux_data as (
    select
        destination_company_siret,
        max(destination_company_name) as "name",
        sum(quantity_received) filter (
            where
            "_bs_type" = 'BSDD'
        )                             as quantity_bsdd,
        sum(quantity_received) filter (
            where
            "_bs_type" = 'BSDA'
        )                             as quantity_bsda,
        array_agg(id),
        array_agg(readable_id)
    from {{ ref('bordereaux_enriched') }}
    where
        processing_operation = 'D5'
        and (
            waste_code ~* '.*\*$'
            or waste_pop
            or waste_is_dangerous
        )
        and "_bs_type" in ('BSDD', 'BSDA')
    group by
        destination_company_siret
),

agg_data as (
    select
        destination_company_siret as siret,
        max("name")               as "nom_etablissement",
        max(b.quantity_bsdd)      as quantite_bsdd_traitee_en_d5,
        max(b.quantity_bsda)      as quantite_bsda_traitee_en_d5,
        case
            when count(ir.siret) = 0 then 'Pas de données ICPE'
            when '2760-1' = any(array_agg(ir.rubrique || coalesce(
                '-' || ir.alinea,
                ''
            ))) then 'Rubrique 2760-1'
            else 'Données ICPE mais pas de rubrique 2760-1'
        end                       as statut_icpe
    from
        bordereaux_data as b
    left join {{ ref('installations_rubriques') }} as ir
        on
            ir.siret = b.destination_company_siret
    group by
        destination_company_siret
)

select *
from
    agg_data
where
    statut_icpe != 'Rubrique 2760-1'
