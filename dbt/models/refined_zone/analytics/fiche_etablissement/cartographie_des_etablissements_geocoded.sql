{{
  config(
    materialized = 'table',
    )}}

select
    siret,
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
    latitude_ban,
    longitude_ban,
    cban.latitude                        as latitude_ban,
    cban.longitude                       as longitude_ban,
    st_setsrid(
        st_point(
            coalesce(c.longitude, cban.longitude),
            coalesce(c.latitude, cban.latitude)
        ),
        4326
    )                                    as coords,
from {{ ref("cartographie_des_etablissements") }} c
left join
    {{ ref("companies_geocoded_by_ban") }} as cban
    on s.siret = cban.siret and cban.result_status = 'ok'