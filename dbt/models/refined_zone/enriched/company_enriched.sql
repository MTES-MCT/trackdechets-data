{{
  config(
    materialized = 'table',
    indexes=[
        {"columns":["siret"],"unique":True},
        {"columns":["org_id"],"unique":True},
        {"columns":["code_commune_insee"]}
    ]
    )
}}

with communes as (
    --Il y a des duplicats dans les codes communes, ici on déduplique et on ajoute les données de la commune parente
    --en cas d'absence des données pour un commune déléguée. Cela concerne très peu de cas néanmoins.
    select
        cgc.code_commune,
        max(
            coalesce(cgc.code_departement, cgc2.code_departement)
        )                                                as code_departement,
        max(coalesce(cgc.code_region, cgc2.code_region)) as code_region
    from
        {{ ref('code_geo_communes') }} as cgc
    left join
        {{ ref('code_geo_communes') }} as cgc2
        on cgc.code_commune_parente = cgc2.code_commune
    group by cgc.code_commune
)

select
    c.*,
    naf.*,
    etabs.etat_administratif_etablissement,
    communes.code_commune     as "code_commune_insee",
    communes.code_departement as "code_departement_insee",
    communes.code_region      as "code_region_insee"
from
    {{ ref('company') }} as c
left join
    {{ ref('stock_etablissement') }}
    as etabs
    on c.siret = etabs.siret
left join
    communes
    on
        coalesce(
            etabs.code_commune_etablissement,
            etabs.code_commune_2_etablissement
        ) = communes.code_commune
left join
    {{ ref('nomenclature_activites_francaises') }}
    as naf
    on replace(
        coalesce(etabs.activite_principale_etablissement, c.code_naf),
        '.',
        ''
    ) = replace(
        naf.code_sous_classe,
        '.',
        ''
    )
