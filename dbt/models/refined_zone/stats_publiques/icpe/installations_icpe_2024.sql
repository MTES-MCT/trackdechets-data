{{
  config(
    materialized = 'table',
    indexes=[{"columns":["code_aiot"]},{"columns":["siret"]},{"columns":["rubrique"]}]
    )
}}

with coord_cp_data as (
    select
        code_postal,
        avg(latitude)  as latitude,
        avg(longitude) as longitude
    from
        {{ ref('base_codes_postaux') }}
    group by
        1
),

coord_commune_data as (
    select
        code_commune_insee,
        avg(latitude)  as latitude,
        avg(longitude) as longitude
    from
        {{ ref('base_codes_postaux') }}
    group by
        1
),

installations as (
    select
        ir.code_aiot,
        ir.raison_sociale,
        ir.siret,
        ir.quantite_totale as quantite_autorisee,
        ir.unite,
        ir.libelle_etat_site,
        i.latitude,
        i.longitude,
        i.adresse1,
        i.adresse2,
        i.code_postal,
        i.commune,
        i.code_insee,
        ir.rubrique
    from
        {{ ref('installations_rubriques_2024') }} as ir
    left join {{ ref('installations') }} as i on ir.code_aiot = i.code_aiot
    where
        (ir.libelle_etat_site = 'Avec titre') -- noqa: LXR
        and (ir.etat_administratif_rubrique in ('En vigueur', 'A l''arrêt'))
        and (ir.etat_technique_rubrique = 'Exploité')
        and (ir.raison_sociale !~* 'illégal|illicite')
)

select
    i.code_aiot,
    i.raison_sociale,
    i.siret,
    i.rubrique,
    i.quantite_autorisee,
    i.unite,
    i.libelle_etat_site,
    i.adresse1,
    i.adresse2,
    i.code_postal,
    i.commune,
    cgc.code_departement as code_departement_insee,
    cgc.code_region      as code_region_insee,
    coalesce(
        i.latitude,
        coord1.latitude,
        coord2.latitude
    )                    as latitude,
    coalesce(
        i.longitude,
        coord1.longitude,
        coord2.longitude
    )                    as longitude,
    coalesce(
        se.code_commune_etablissement,
        i.code_insee
    )                    as code_commune_insee
from
    installations as i
left join {{ ref('stock_etablissement') }} as se
    on i.siret = se.siret
left join coord_commune_data as coord1
    on
        coalesce(se.code_commune_etablissement, i.code_insee)
        = coord1.code_commune_insee
left join coord_cp_data as coord2
    on i.code_postal::int = coord2.code_postal
left join {{ ref('code_geo_communes') }} as cgc on
    coalesce(
        se.code_commune_etablissement,
        i.code_insee
    ) = cgc.code_commune
    and cgc.type_commune = 'COM'
