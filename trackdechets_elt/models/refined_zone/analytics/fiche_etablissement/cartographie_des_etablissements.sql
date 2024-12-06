{{
  config(
    materialized = 'table',
    indexes = [ 
        {
            'columns': ['siret'],
            'unique': True 
        },
        {
            'columns': ['coords'],
            'type': 'GIST' 
        },
    ],
    tags = ['fiche_etablissement']
    )
}}

with "stats" as (
    select
        siret,
        sbs.processing_operations_as_destination_bsdd    as processing_operations_bsdd,
        sbs.processing_operations_as_destination_bsdnd   as processing_operations_bsdnd,
        sbs.processing_operations_as_destination_bsda    as processing_operations_bsda,
        sbs.processing_operations_as_destination_bsff    as processing_operations_bsff,
        sbs.processing_operations_as_destination_bsdasri as processing_operations_bsdasri,
        sbs.processing_operations_as_destination_bsvhu   as processing_operations_bsvhu,
        dnd_processing_operations_as_destination         as processing_operation_dnd,
        texs_processing_operations_as_destination        as processing_operation_texs,
        sbs.num_bsdd_as_emitter > 0
        or sbs.num_bsdd_as_transporter > 0
        or sbs.num_bsdd_as_destination > 0               as bsdd,
        sbs.num_bsdnd_as_emitter > 0
        or sbs.num_bsdnd_as_transporter > 0
        or sbs.num_bsdnd_as_destination > 0              as bsdnd,
        sbs.num_bsda_as_emitter > 0
        or sbs.num_bsda_as_transporter > 0
        or sbs.num_bsda_as_destination > 0               as bsda,
        sbs.num_bsff_as_emitter > 0
        or sbs.num_bsff_as_transporter > 0
        or sbs.num_bsff_as_destination > 0               as bsff,
        sbs.num_bsdasri_as_emitter > 0
        or sbs.num_bsdasri_as_transporter > 0
        or sbs.num_bsdasri_as_destination > 0            as bsdasri,
        sbs.num_bsvhu_as_emitter > 0
        or sbs.num_bsvhu_as_transporter > 0
        or sbs.num_bsvhu_as_destination > 0              as bsvhu,
        sbs.num_dnd_statements_as_destination > 0
        or sbs.num_dnd_statements_as_emitter > 0         as dnd,
        sbs.num_texs_statements_as_destination > 0
        or sbs.num_texs_statements_as_emitter > 0        as texs,
        sbs.num_ssd_statements_as_destination > 0        as ssd,
        sbs.num_pnttd_statements_as_destination > 0      as pnttd
    from {{ ref('statistics_by_siret') }} as sbs
    where char_length(siret) = 14
),

joined as (
    select
        s.*,
        c.company_types                      as profils,
        c.collector_types                    as profils_collecteur,
        c.waste_processor_types              as profils_installation,
        se.code_commune_etablissement        as code_commune_insee,
        cgc.code_departement                 as code_departement_insee,
        cgc.code_region                      as code_region_insee,
        c.address                            as adresse_td,
        c.latitude                           as latitude_td,
        c.longitude                          as longitude_td,
        cban.latitude                        as latitude_ban,
        cban.longitude                       as longitude_ban,
        et.num_texs_dd_as_emitter > 0
        or et.num_texs_dd_as_transporter > 0
        or et.num_texs_dd_as_destination > 0 as texs_dd,
        st_setsrid(
            st_point(
                coalesce(c.longitude, cban.longitude),
                coalesce(c.latitude, cban.latitude)
            ),
            4326
        )                                    as coords,
        nullif(
            coalesce(se.complement_adresse_etablissement || ' ', '')
            || coalesce(se.numero_voie_etablissement || ' ', '')
            || coalesce(se.indice_repetition_etablissement || ' ', '')
            || coalesce(se.type_voie_etablissement || ' ', '')
            || coalesce(se.libelle_voie_etablissement || ' ', '')
            || coalesce(se.code_postal_etablissement || ' ', '')
            || coalesce(se.libelle_commune_etablissement || ' ', '')
            || coalesce(se.libelle_commune_etranger_etablissement || ' ', '')
            || coalesce(se.distribution_speciale_etablissement, ''), ''
        )                                    as adresse_insee,
        coalesce(
            c.name, se.enseigne_1_etablissement,
            se.enseigne_2_etablissement,
            se.enseigne_3_etablissement,
            se.denomination_usuelle_etablissement
        )                                    as nom_etablissement
    from "stats" as s
    left join {{ ref('etablissements_texs_dd') }} as et on s.siret = et.siret
    left join {{ ref("stock_etablissement") }} as se on s.siret = se.siret
    left join {{ ref("company") }} as c on s.siret = c.siret
    left join
        {{ ref("code_geo_communes") }} as cgc
        on
            se.code_commune_etablissement = cgc.code_commune
            and cgc.type_commune != 'COMD'
    left join
        {{ ref("companies_geocoded_by_ban") }} as cban
        on s.siret = cban.siret and cban.result_status = 'ok'
)

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
    coords
from joined
