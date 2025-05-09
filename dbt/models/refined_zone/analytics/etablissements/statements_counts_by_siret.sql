{{
  config(
    materialized = 'table',
    )}}

with dnd_entrant_stats as (
    select
        report_for_company_siret as siret,
        count(
            distinct id
        )
            as num_dnd_statements_as_destination,
        sum(weight_value)
            as quantity_dnd_statements_as_destination,
        sum(volume)   as volume_dnd_statements_as_destination,
        array_agg(
            distinct operation_code
        ) filter (where operation_code is not null)
            as dnd_processing_operations_as_destination,
        array_agg(distinct waste_code)     as dnd_waste_codes_as_destination
    from {{ ref('registry_incoming_waste') }}
    group by 1
),

dnd_sortant_stats as (
    select
        report_for_company_siret as siret,
        count(
            distinct id
        )                                as num_dnd_statements_as_emitter,
        sum(weight_value)                                as quantity_dnd_statements_as_emitter,
        sum(volume)                                as volume_dnd_statements_as_emitter
    from {{ ref('registry_outgoing_waste') }}
    group by 1
),

dnd_transporteur_stats as (
    select 
        transporteur_siret as siret,
        count(distinct id) as num_dnd_statements_as_transporteur,
        sum(weight_value)  as quantity_dnd_statements_as_transporteur,
        sum(volume)        as volume_dnd_statements_as_transporteur
    from
        {{ ref('registry_incoming_waste') }}riw 
    array join array(transporter1_company_org_id,
        transporter2_company_org_id,
        transporter3_company_org_id,
        transporter4_company_org_id,
        transporter5_company_org_id) as transporteur_siret
    where
        transporteur_siret is not null
    group by 1
),

texs_entrant_stats as (
    select
        report_for_company_siret as siret,
        count(
            distinct id
        )
            as num_texs_statements_as_destination,
        sum(weight_value)
            as quantity_texs_statements_as_destination,
        sum(volume)
            as volume_texs_statements_as_destination,
        array_agg(
            distinct operation_code
        )
            as texs_processing_operations_as_destination,
        array_agg(distinct waste_code)     as texs_waste_codes_as_destination
    from {{ ref('registry_incoming_texs') }}
    group by 1
),

texs_sortant_stats as (
    select
        report_for_company_siret as siret,
        count(
            distinct id
        )                                as num_texs_statements_as_emitter,
        sum(weight_value)                                as quantity_texs_statements_as_emitter,
        sum(volume)                                as volume_texs_statements_as_emitter
    from {{ ref('registry_outgoing_texs') }}
    group by 1
),

texs_transporteur_stats as (
    select 
        transporteur_siret as siret,
        count(distinct id) as num_texs_statements_as_transporteur,
        sum(weight_value)  as quantity_texs_statements_as_transporteur,
        sum(volume)        as volume_texs_statements_as_transporteur
    from
        {{ ref('registry_incoming_texs') }}riw 
    array join array(transporter1_company_org_id,
        transporter2_company_org_id,
        transporter3_company_org_id,
        transporter4_company_org_id,
        transporter5_company_org_id) as transporteur_siret
    where
        transporteur_siret is not null
    group by 1
),

ssd_stats as (
    select
        report_for_company_siret as siret,
        count(
            distinct id
        )
            as num_ssd_statements_as_emitter,
        sum(weight_value)
            as quantity_ssd_statements_as_emitter,
        sum(volume)
            as volume_ssd_statements_as_emitter
    from {{ ref("registry_ssd") }}
    group by 1
)

select
    dnd_processing_operations_as_destination,
    texs_processing_operations_as_destination,
    dnd_waste_codes_as_destination,
    texs_waste_codes_as_destination,
    coalesce(
        dnd1.siret,
        dnd2.siret,
        dnd3.siret,
        texs1.siret,
        texs2.siret,
        texs3.siret,
        ssd.siret
    ) as siret,
    coalesce(
        num_dnd_statements_as_destination, 0
    ) as num_dnd_statements_as_destination,
    coalesce(
        quantity_dnd_statements_as_destination, 0
    ) as quantity_dnd_statements_as_destination,
    coalesce(
        volume_dnd_statements_as_destination, 0
    ) as volume_dnd_statements_as_destination,
    coalesce(
        num_dnd_statements_as_emitter, 0
    ) as num_dnd_statements_as_emitter,
    coalesce(
        quantity_dnd_statements_as_emitter, 0
    ) as quantity_dnd_statements_as_emitter,
    coalesce(
        volume_dnd_statements_as_emitter, 0
    ) as volume_dnd_statements_as_emitter,
    coalesce(
        num_dnd_statements_as_transporteur, 0
    ) as num_dnd_statements_as_transporteur,
    coalesce(
        quantity_dnd_statements_as_transporteur, 0
    ) as quantity_dnd_statements_as_transporteur,
    coalesce(
        volume_dnd_statements_as_transporteur, 0
    ) as volume_dnd_statements_as_transporteur,
    coalesce(
        num_texs_statements_as_destination, 0
    ) as num_texs_statements_as_destination,
    coalesce(
        quantity_texs_statements_as_destination, 0
    ) as quantity_texs_statements_as_destination,
    coalesce(
        volume_texs_statements_as_destination, 0
    ) as volume_texs_statements_as_destination,
    coalesce(
        num_texs_statements_as_emitter, 0
    ) as num_texs_statements_as_emitter,
    coalesce(
        quantity_texs_statements_as_emitter, 0
    ) as quantity_texs_statements_as_emitter,
    coalesce(
        volume_texs_statements_as_emitter, 0
    ) as volume_texs_statements_as_emitter,
    coalesce(
        num_texs_statements_as_transporteur, 0
    ) as num_texs_statements_as_transporteur,
    coalesce(
        quantity_texs_statements_as_transporteur, 0
    ) as quantity_texs_statements_as_transporteur,
    coalesce(
        volume_texs_statements_as_transporteur, 0
    ) as volume_texs_statements_as_transporteur,
    coalesce(
        num_ssd_statements_as_emitter, 0
    ) as num_ssd_statements_as_emitter,
    coalesce(
        quantity_ssd_statements_as_emitter, 0
    ) as quantity_ssd_statements_as_emitter,
    coalesce(
        volume_ssd_statements_as_emitter, 0
    ) as volume_ssd_statements_as_emitter
from dnd_entrant_stats as dnd1
full outer join dnd_sortant_stats as dnd2 on dnd1.siret = dnd2.siret
full outer join
    dnd_transporteur_stats as dnd3
    on coalesce(dnd1.siret, dnd2.siret) = dnd3.siret
full outer join
    texs_entrant_stats as texs1
    on coalesce(dnd1.siret, dnd2.siret, dnd3.siret) = texs1.siret
full outer join
    texs_sortant_stats as texs2
    on coalesce(dnd1.siret, dnd2.siret, dnd3.siret, texs1.siret) = texs2.siret
full outer join
    texs_transporteur_stats as texs3
    on
        coalesce(dnd1.siret, dnd2.siret, dnd3.siret, texs1.siret, texs2.siret)
        = texs3.siret
full outer join
    ssd_stats as ssd
    on
        coalesce(
            dnd1.siret,
            dnd2.siret,
            dnd3.siret,
            texs1.siret,
            texs2.siret,
            texs3.siret
        )
        = ssd.siret
