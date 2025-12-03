{{
  config(
    materialized = 'table',
    ) }}

select
    c.siret,
    nom_etablissement,
    date_inscription,
    profils,
    profils_collecteur,
    profils_installation,
    profils_installation_vhu,
    worker_company_has_sub_section_four,
    worker_company_has_sub_section_three,
    bsdd,
    bsdd_emitter,
    bsdd_transporter,
    bsdd_destination,
    bsdnd,
    bsdnd_emitter,
    bsdnd_transporter,
    bsdnd_destination,
    bsda,
    bsda_emitter,
    bsda_worker,
    bsda_transporter,
    bsda_destination,
    bsff,
    bsff_emitter,
    bsff_transporter,
    bsff_destination,
    bsdasri,
    bsdasri_emitter,
    bsdasri_transporter,
    bsdasri_destination,
    bsvhu,
    bsvhu_emitter,
    bsvhu_transporter,
    bsvhu_destination,
    texs_dd,
    texs_dd_emitter,
    texs_dd_transporter,
    texs_dd_destination,
    dnd,
    dnd_emitter,
    dnd_destination,
    texs,
    texs_destination,
    texs_emitter,
    ssd,
    pnttd,
    processing_operations_bsdd,
    processing_operations_bsdnd,
    processing_operations_bsda,
    processing_operations_bsff,
    processing_operations_bsdasri,
    processing_operations_bsvhu,
    processing_operations_dnd,
    processing_operations_texs,
    waste_codes_bordereaux,
    waste_codes_dnd_statements,
    waste_codes_texs_statements,
    waste_codes_processed,
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
    )
        as coords,
    geoToH3(
        coalesce(c.longitude_td, cban.longitude),
        coalesce(c.latitude_td, cban.latitude),
        9
    )              as coords_h3_index
from {{ ref("cartographie_etablissements") }} as c
left join
    {{ ref("companies_geocoded_by_ban") }} as cban
    on c.siret = cban.siret and cban.result_status = 'ok'
