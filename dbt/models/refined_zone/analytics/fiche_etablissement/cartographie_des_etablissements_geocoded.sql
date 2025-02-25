{{
  config(
    materialized = 'table',
    ) }}

select
    c.siret,
    nom_etablissement,
    profils,
    profils_collecteur,
    profils_installation,
    bsdd,
    bsdnd,
    bsda,
    bsff,
    bsdasri,
    bsvhu,
    texs_dd,
    dnd,
    texs,
    ssd,
    pnttd,
    processing_operations_bsdd,
    processing_operations_bsdnd,
    processing_operations_bsda,
    processing_operations_bsff,
    processing_operations_bsdasri,
    processing_operations_bsvhu,
    processing_operation_dnd,
    processing_operation_texs,
    code_commune_insee,
    code_departement_insee,
    code_region_insee,
    adresse_td,
    adresse_insee,
    latitude_td,
    longitude_td,
    cban.latitude
        as latitude_ban,
    cban.longitude
        as longitude_ban,
    if(
        coalesce(c.longitude_td, cban.longitude) is null
        or coalesce(c.latitude_td, cban.latitude) is null,
        null,
        wkt((
            assumeNotNull(coalesce(c.longitude_td, cban.longitude)),
            assumeNotNull(coalesce(c.latitude_td, cban.latitude))
        ))
    )              as coords,
    geoToH3(coalesce(c.longitude_td, cban.longitude),coalesce(c.latitude_td, cban.latitude),9) as coords_h3_index
from {{ ref("cartographie_des_etablissements") }} as c
left join
    {{ ref("companies_geocoded_by_ban") }} as cban
    on c.siret = cban.siret and cban.result_status = 'ok'
