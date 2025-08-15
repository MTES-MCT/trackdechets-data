{{
  config(
    materialized = 'table',
    order_by = 'siret'
    )
}}

select
    se.siret                                                           as siret,
    se.activite_principale_etablissement
        as ape_code,
    {{ get_company_name_column_from_stock_etablissement() }}
        as nom_etablissement,
    coalesce(cog.code_commune, cog_om.code_zonage_outre_mer)
        as code_commune,
    coalesce(cog.code_departement, cog_om.code_collectivite_outre_mer)
        as code_departement,
    coalesce(cog.code_region, cog_om.code_collectivite_outre_mer)
        as code_region,
    {{ get_address_column_from_stock_etablissement() }}
        as adresse_etablissement
from {{ ref('stock_etablissement') }} as se
left anti join {{ ref('company') }} as c on se.siret = c.siret
left join {{ ref('code_geo_communes') }} as cog
    on
        se.code_commune_etablissement = cog.code_commune
        and cog.type_commune != 'COMD'
left join {{ ref('code_geo_territoires_outre_mer') }} as cog_om
    on se.code_commune_etablissement = cog_om.code_zonage_outre_mer
where se.etat_administratif_etablissement = 'A'
