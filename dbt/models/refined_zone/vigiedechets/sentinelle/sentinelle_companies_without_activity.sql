{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":16
    },
    settings={"allow_nullable_key":1},
    order_by = '(company_ape_code,company_siret)'
    )
}}

with dataset as (
    select
        assumeNotNull(c.siret)
            as company_siret,
        coalesce(se.activite_principale_etablissement, '')
            as company_ape_code,
        coalesce(
            c.name, {{ get_company_name_column_from_stock_etablissement() }}
        )
            as company_name,
        coalesce(cog.code_commune, cog_om.code_zonage_outre_mer)
            as company_code_commune,
        coalesce(cog.code_departement, cog_om.code_collectivite_outre_mer)
            as company_code_departement,
        coalesce(cog.code_region, cog_om.code_collectivite_outre_mer)
            as company_code_region,
        {{ get_address_column_from_stock_etablissement() }}
            as company_address,
        se.code_postal_etablissement
            as company_code_postal
    from {{ ref('company') }} as c
    left anti join
        {{ ref('sentinelle_waste_quantity_produced_by_siret') }} as sw
        on c.siret = sw.company_siret
    left join {{ ref('statistics_by_siret') }} as sbs on c.siret = sbs.siret
    left join {{ ref('stock_etablissement') }} as se on c.siret = se.siret
    left join {{ ref('code_geo_communes') }} as cog
        on
            se.code_commune_etablissement = cog.code_commune
            and cog.type_commune != 'COMD'
    left join {{ ref('code_geo_territoires_outre_mer') }} as cog_om
        on se.code_commune_etablissement = cog_om.code_zonage_outre_mer
    where
        not empty(c.siret)
        and se.etat_administratif_etablissement = 'A'
        and coalesce(sbs.total_bordereaux_statements_references, 0) = 0
)

select
    company_siret,
    company_name,
    company_ape_code,
    company_address,
    company_code_postal,
    company_code_commune,
    company_code_departement,
    company_code_region
from dataset
