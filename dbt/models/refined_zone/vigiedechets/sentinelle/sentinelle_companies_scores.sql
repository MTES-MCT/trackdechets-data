{{
  config(
    materialized = 'table',
    order_by = 'siret',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":128
    }
    )
}}


with scores as (
    select
        fgd.siret,
        max(
            fgd.ape_code
        )                                   as ape_code,
        max(fgd.first_activity_datetime)    as first_activity_datetime,
        max(fgd.total_events_count)         as total_events_count,
        max(fgd.company_name)               as company_name,
        max(fgd.code_commune_etablissement) as code_commune_etablissement,
        groupArray(
            '{\'waste_code\': \''
            || fgd.waste_code
            || '\', '
            || '\'quantity_share\':'
            || toDecimalString(round(fgd.waste_quantity_share, 6), 8)
            || ', \'quantity_share_ref\':'
            || toDecimalString(round(fgd.waste_quantity_share_ref, 6), 8)
            || '}')
            as score_details, -- constructs an array of json objects with score details for each waste code
        sum(
            abs(fgd.waste_quantity_share - fgd.waste_quantity_share_ref)
            * fgd.waste_quantity_share_ref
        )                                   as scores
    from
        {{ ref('sentinelle_companies_features') }} as fgd
    group by
        1
)

select
    s.siret                                                            as siret,
    coalesce(
        s.company_name, {{ get_company_name_column_from_stock_etablissement() }}
    )
        as company_name,
    coalesce(cog.code_commune, cog_om.code_zonage_outre_mer)
        as code_commune,
    coalesce(cog.code_departement, cog_om.code_collectivite_outre_mer)
        as code_departement,
    coalesce(cog.code_region, cog_om.code_collectivite_outre_mer)
        as code_region,
    {{ get_address_column_from_stock_etablissement() }}
        as adresse_etablissement,
    s.ape_code
        as company_ape_code,
    s.first_activity_datetime,
    s.total_events_count,
    s.score_details,
    s.scores
from
    scores as s
left join {{ ref('stock_etablissement') }} as se
    on s.siret = se.siret
left join {{ ref('code_geo_communes') }} as cog
    on
        se.code_commune_etablissement = cog.code_commune
        and cog.type_commune != 'COMD'
left join {{ ref('code_geo_territoires_outre_mer') }} as cog_om
    on se.code_commune_etablissement = cog_om.code_zonage_outre_mer
