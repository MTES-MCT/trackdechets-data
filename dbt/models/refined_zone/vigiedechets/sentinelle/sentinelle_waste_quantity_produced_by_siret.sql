{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":16
    }
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
        be.emitter_company_siret     as siret,
        be.waste_code,
        max(be.emitter_company_name) as company_name,
        count(distinct be.id)        as processed_bordereaux_count,
        min(be.processed_at)         as processed_at_min,
        sum(
            if(
                be._bs_type = 'BSFF',
                be.accepted_quantity_packagings,
                toFloat64(be.quantity_received)
            )
            - coalesce(be.quantity_refused, 0)
        )                            as quantity
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
        and not empty(be.waste_code)
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
        riw.emitter_company_org_id            as siret,
        riw.waste_code,
        max(riw.initial_emitter_company_name) as company_name,
        count(distinct riw.id)                as statements_count,
        min(riw.reception_date)               as received_at_min,
        sum(
            riw.weight_value
        )                                     as quantity
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
        and not empty(riw.waste_code)
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
        rtexs.emitter_company_org_id            as siret,
        rtexs.waste_code,
        max(rtexs.initial_emitter_company_name) as company_name,
        count(distinct rtexs.id)                as statements_count,
        min(rtexs.reception_date)               as received_at_min,
        sum(
            rtexs.weight_value
        )                                       as quantity
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
        and not empty(rtexs.waste_code)
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
        company_name,
        processed_bordereaux_count as events_count,
        processed_at_min           as first_activity_datetime,
        quantity
    from bordereaux_quantities
    union all
    select
        siret,
        waste_code,
        company_name,
        statements_count as events_count,
        received_at_min  as first_activity_datetime,
        quantity
    from dnd_quantities
    union all
    select
        siret,
        waste_code,
        company_name,
        statements_count as events_count,
        received_at_min  as first_activity_datetime,
        quantity
    from texs_quantities
),

summed as (
    select
        d.siret                                     as company_siret,
        d.waste_code,
        max(se.activite_principale_etablissement)   as company_ape_code,
        max({{ get_address_column_from_stock_etablissement() }})
            as adresse_etablissement,
        max(d.company_name)                         as company_name,
        min(first_activity_datetime)                as first_activity_datetime,
        sum(events_count)                           as events_count,
        sum(d.quantity)                             as waste_quantity,
        max(se.code_commune_etablissement)
            as code_commune_etablissement
    from all_quantities_data as d
    left join {{ ref('stock_etablissement') }} as se
        on
            d.siret = se.siret
    group by 1, 2
),

data_with_cog as (
    select
        s.company_name,
        adresse_etablissement
            as company_address,
        toLowCardinality(
            coalesce(cog.code_commune, cog_om.code_zonage_outre_mer)
        )
            as company_code_commune,
        toLowCardinality(
            coalesce(cog.code_departement, cog_om.code_collectivite_outre_mer)
        )
            as company_code_departement,
        toLowCardinality(
            coalesce(cog.code_region, cog_om.code_collectivite_outre_mer)
        )
            as company_code_region,
        assumeNotNull(s.company_siret)
            as company_siret,
        toLowCardinality(assumeNotNull(company_ape_code))
            as company_ape_code,
        toLowCardinality(assumeNotNull(waste_code))
            as waste_code,
        assumeNotNull(first_activity_datetime)
            as waste_first_activity_datetime,
        assumeNotNull(events_count)
            as waste_events_count,
        assumeNotNull(waste_quantity)
            as waste_quantity,
        min(s.first_activity_datetime)
            over (partition by s.company_siret)
            as first_activity_datetime,
        sum(s.events_count)
            over (partition by s.company_siret)
            as total_events_count,
        s.waste_quantity
        / (sum(s.waste_quantity) over (partition by s.company_siret))
            as waste_quantity_share
    from summed as s
    left join {{ ref('code_geo_communes') }} as cog
        on
            s.code_commune_etablissement = cog.code_commune
            and cog.type_commune != 'COMD'
    left join {{ ref('code_geo_territoires_outre_mer') }} as cog_om
        on s.code_commune_etablissement = cog_om.code_zonage_outre_mer
    where
        not empty(s.company_siret)
        and waste_quantity > 0
)

select
    company_siret,
    waste_code,
    waste_quantity,
    waste_quantity_share,
    waste_first_activity_datetime,
    waste_events_count,
    first_activity_datetime,
    total_events_count,
    company_name,
    company_address,
    company_code_commune,
    company_code_departement,
    company_code_region,
    company_ape_code
from data_with_cog
