{{
  config(
    materialized = 'table',
    )
}}

with bordereaux_ttr as (
    select distinct be.destination_company_siret as siret
    from
        {{ ref('bordereaux_enriched') }} as be
    where
        be.processing_operation in (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
        and be.processed_at is not null
        and be.status != 'CANCELED'
),

bordereaux_quantities as (
    select
        be.emitter_company_siret as siret,
        be.waste_code,
        sum(
            if(
                be._bs_type = 'BSFF',
                be.accepted_quantity_packagings,
                toFloat64(be.quantity_received)
            )
            - coalesce(be.quantity_refused, 0)
        )                        as quantity
    from
        {{ ref('bordereaux_enriched') }} as be
    where
        {{ dangerous_waste_filter('bordereaux_enriched') }}
        and be.emitter_company_siret not in (
            select siret
            from
                bordereaux_ttr
        )
        -- old data can have a quantity that is not reliable
        and be.processed_at >= '2023-07-01'
    group by
        1,
        2
    having
        quantity is not null
),

dnd_registry_ttr as (
    select distinct riw.report_for_company_siret as siret
    from
        {{ ref('registry_incoming_waste') }} as riw
    where
        riw.operation_code in (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
        and riw.reception_date is not null
        and not riw.is_cancelled
),

dnd_quantities as (
    select
        riw.emitter_company_org_id as siret,
        riw.waste_code,
        sum(
            riw.weight_value
        )                          as quantity
    from
        {{ ref('registry_incoming_waste') }} as riw
    where
        not{{ dangerous_waste_filter('registry') }}
        and riw.emitter_company_org_id not in (
            select siret
            from
                dnd_registry_ttr
        )
        -- old data can have a quantity that is not reliable
        and riw.reception_date >= '2024-01-01'
        and riw.emitter_company_country_code = 'FR'
    group by
        1,
        2
    having
        quantity is not null
),

texs_registry_ttr as (
    select distinct rtexs.report_for_company_siret as siret
    from
        {{ ref('registry_incoming_texs') }} as rtexs
    where
        rtexs.operation_code in (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
        and rtexs.reception_date is not null
        and not rtexs.is_cancelled
),

texs_quantities as (
    select
        rtexs.emitter_company_org_id as siret,
        rtexs.waste_code,
        sum(
            rtexs.weight_value
        )                            as quantity
    from
        {{ ref('registry_incoming_texs') }} as rtexs
    where
        not {{ dangerous_waste_filter('registry') }}
        and rtexs.emitter_company_org_id not in (
            select siret
            from
                texs_registry_ttr
        )
        -- old data can have a quantity that is not reliable
        and rtexs.reception_date >= '2024-01-01'
        and rtexs.emitter_company_country_code = 'FR'
    group by
        1,
        2
    having
        quantity is not null
),

all_quantities_data as (
    select
        siret,
        waste_code,
        quantity
    from bordereaux_quantities
    union all
    select
        siret,
        waste_code,
        quantity
    from dnd_quantities
    union all
    select
        siret,
        waste_code,
        quantity
    from texs_quantities
)

select
    d.siret                                   as siret,
    max(se.activite_principale_etablissement) as siret_ape_code,
    d.waste_code,
    sum(d.quantity)                           as waste_quantity
from all_quantities_data as d
left join {{ ref('stock_etablissement') }} as se
    on
        d.siret = se.siret
group by 1, 3
