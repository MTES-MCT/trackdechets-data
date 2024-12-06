{{
  config(
    materialized = 'table',
    indexes = [
        {
            "columns":["siret"],
            "unique":true
        }
    ]
    )
}}

with emitter_stats as (
    select
        emitter_company_siret,
        count(distinct id)     as num_texs_dd_as_emitter,
        sum(quantity_received) as quantity_texs_dd_as_emitter
    from {{ ref('bordereaux_enriched') }}
    where waste_code in ('17 05 03*', '17 05 05*')
    group by 1
),

transporter_stats as (

    select
        transporter_company_siret,
        count(distinct bs_id)  as num_texs_dd_as_transporter,
        sum(quantity_received) as quantity_texs_dd_as_transporter
    from (
        select
            form_id as bs_id,
            transporter_company_siret,
            quantity_received
        from
            {{ ref('bsdd_transporter_enriched') }}
        where waste_details_code in ('17 05 03*', '17 05 05*')
        union all
        select
            bsda_id                      as bs_id,
            transporter_company_siret,
            destination_reception_weight as quantity_received
        from
            {{ ref('bsda_transporter_enriched') }}
        where waste_code in ('17 05 03*', '17 05 05*')
    ) as t
    group by 1
),

destination_stats as (
    select
        destination_company_siret,
        count(distinct id)     as num_texs_dd_as_destination,
        sum(quantity_received) as quantity_texs_dd_as_destination
    from {{ ref('bordereaux_enriched') }}
    where waste_code in ('17 05 03*', '17 05 05*')
    group by 1
)

select
    coalesce(
        emitter_company_siret,
        transporter_company_siret,
        destination_company_siret
    ) as siret,
    coalesce(
        num_texs_dd_as_emitter, 0
    ) as num_texs_dd_as_emitter,
    coalesce(
        quantity_texs_dd_as_emitter, 0
    ) as quantity_texs_dd_as_emitter,
    coalesce(
        num_texs_dd_as_transporter, 0
    ) as num_texs_dd_as_transporter,
    coalesce(
        quantity_texs_dd_as_transporter, 0
    ) as quantity_texs_dd_as_transporter,
    coalesce(
        num_texs_dd_as_destination, 0
    ) as num_texs_dd_as_destination,
    coalesce(
        quantity_texs_dd_as_destination, 0
    ) as quantity_texs_dd_as_destination
from emitter_stats as es
full outer join
    transporter_stats as ts
    on es.emitter_company_siret = ts.transporter_company_siret
full outer join
    destination_stats as ds
    on
        coalesce(es.emitter_company_siret, ts.transporter_company_siret)
        = ds.destination_company_siret
