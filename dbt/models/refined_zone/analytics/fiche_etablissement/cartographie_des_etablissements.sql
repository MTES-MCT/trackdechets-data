{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":8
    }
    )}}

with stats as (
    select
        sbs.siret,
        sbs.processing_operations_as_destination_bsdd as processing_operations_bsdd,
        sbs.processing_operations_as_destination_bsdnd as processing_operations_bsdnd,
        sbs.processing_operations_as_destination_bsda as processing_operations_bsda,
        sbs.processing_operations_as_destination_bsff as processing_operations_bsff,
        sbs.processing_operations_as_destination_bsdasri as processing_operations_bsdasri,
        sbs.processing_operations_as_destination_bsvhu as processing_operations_bsvhu,
        sbs.dnd_processing_operations_as_destination as processing_operation_dnd,
        sbs.texs_processing_operations_as_destination as processing_operation_texs,
        sbs.waste_codes_as_destination as waste_codes_bordereaux,
        sbs.dnd_waste_codes_as_destination as waste_codes_dnd_statements,
        sbs.texs_waste_codes_as_destination as waste_codes_texs_statements,
        arrayConcat(sbs.waste_codes_as_destination,sbs.dnd_waste_codes_as_destination,sbs.texs_waste_codes_as_destination) as waste_codes_processed,
        coalesce(
            sbs.num_bsdd_as_emitter > 0, false
        ) as bsdd_emitter,
        coalesce(
            sbs.num_bsdd_as_transporter > 0, false
        ) as bsdd_transporter,
        coalesce(
            sbs.num_bsdd_as_destination > 0, false
        ) as bsdd_destination,
        coalesce(
            sbs.num_bsdd_as_emitter > 0
            or sbs.num_bsdd_as_transporter > 0
            or sbs.num_bsdd_as_destination > 0, false
        ) as bsdd,
        coalesce(
            sbs.num_bsdnd_as_emitter > 0, false
        ) as bsdnd_emitter,
        coalesce(
            sbs.num_bsdnd_as_transporter > 0, false
        ) as bsdnd_transporter,
        coalesce(
            sbs.num_bsdnd_as_destination > 0, false
        ) as bsdnd_destination,
        coalesce(
            sbs.num_bsdnd_as_emitter > 0
            or sbs.num_bsdnd_as_transporter > 0
            or sbs.num_bsdnd_as_destination > 0, false
        ) as bsdnd,
        coalesce(
            sbs.num_bsda_as_emitter > 0, false
        ) as bsda_emitter,
        coalesce(
            sbs.num_bsda_as_worker > 0, false
        ) as bsda_worker,
        coalesce(
            sbs.num_bsda_as_transporter > 0, false
        ) as bsda_transporter,
        coalesce(
            sbs.num_bsda_as_destination > 0, false
        ) as bsda_destination,
        coalesce(
            sbs.num_bsda_as_emitter > 0
            or sbs.num_bsda_as_worker > 0
            or sbs.num_bsda_as_transporter > 0
            or sbs.num_bsda_as_destination > 0, false
        ) as bsda,
        coalesce(
            sbs.num_bsff_as_emitter > 0, false
        ) as bsff_emitter,
        coalesce(
            sbs.num_bsff_as_transporter > 0, false
        ) as bsff_transporter,
        coalesce(
            sbs.num_bsff_as_destination > 0, false
        ) as bsff_destination,
        coalesce(
            sbs.num_bsff_as_emitter > 0
            or sbs.num_bsff_as_transporter > 0
            or sbs.num_bsff_as_destination > 0, false
        ) as bsff,
        coalesce(
            sbs.num_bsdasri_as_emitter > 0, false
        ) as bsdasri_emitter,
        coalesce(
            sbs.num_bsdasri_as_transporter > 0, false
        ) as bsdasri_transporter,
        coalesce(
            sbs.num_bsdasri_as_destination > 0, false
        ) as bsdasri_destination,
        coalesce(
            sbs.num_bsdasri_as_emitter > 0
            or sbs.num_bsdasri_as_transporter > 0
            or sbs.num_bsdasri_as_destination > 0, false
        ) as bsdasri,
        coalesce(
            sbs.num_bsvhu_as_emitter > 0, false
        ) as bsvhu_emitter,
        coalesce(
            sbs.num_bsvhu_as_transporter > 0, false
        ) as bsvhu_transporter,
        coalesce(
            sbs.num_bsvhu_as_destination > 0, false
        ) as bsvhu_destination,
        coalesce(
            sbs.num_bsvhu_as_emitter > 0
            or sbs.num_bsvhu_as_transporter > 0
            or sbs.num_bsvhu_as_destination > 0, false
        ) as bsvhu,
        coalesce(
            sbs.num_dnd_statements_as_emitter > 0, false
        ) as dnd_emitter,
        coalesce(
            sbs.num_texs_statements_as_destination > 0, false
        ) as dnd_destination,
        coalesce(
            sbs.num_dnd_statements_as_destination > 0
            or sbs.num_dnd_statements_as_emitter > 0, false
        ) as dnd,
        coalesce(
            sbs.num_texs_statements_as_destination > 0, false
        ) as texs_destination,
        coalesce(
            sbs.num_texs_statements_as_emitter > 0, false
        ) as texs_emitter,
        coalesce(
            sbs.num_texs_statements_as_destination > 0
            or sbs.num_texs_statements_as_emitter > 0, false
        ) as texs,
        coalesce(
            sbs.num_ssd_statements_as_emitter > 0, false
        ) as ssd,
        coalesce(
            sbs.num_pnttd_statements_as_destination > 0, false
        ) as pnttd
    from {{ ref('statistics_by_siret') }} as sbs
    where char_length(sbs.siret) = 14
),

joined as (
    select
        s.siret as siret,
        s.processing_operations_bsdd,
        s.processing_operations_bsdnd,
        s.processing_operations_bsda,
        s.processing_operations_bsff,
        s.processing_operations_bsdasri,
        s.processing_operations_bsvhu,
        s.processing_operation_dnd,
        s.processing_operation_texs,
        s.waste_codes_bordereaux,
        s.waste_codes_dnd_statements,
        s.waste_codes_texs_statements,
        s.waste_codes_processed,
        s.bsdd,
        s.bsdd_emitter,
        s.bsdd_transporter,
        s.bsdd_destination,
        s.bsdnd,
        s.bsdnd_emitter,
        s.bsdnd_transporter,
        s.bsdnd_destination,
        s.bsda,
        s.bsda_emitter,
        s.bsda_worker,
        s.bsda_transporter,
        s.bsda_destination,
        s.bsff,
        s.bsff_emitter,
        s.bsff_transporter,
        s.bsff_destination,
        s.bsdasri,
        s.bsdasri_emitter,
        s.bsdasri_transporter,
        s.bsdasri_destination,
        s.bsvhu,
        s.bsvhu_emitter,
        s.bsvhu_transporter,
        s.bsvhu_destination,
        coalesce(et.num_texs_dd_as_emitter > 0, false) as texs_dd_emitter,
        coalesce(
            et.num_texs_dd_as_transporter > 0, false
        )                                              as texs_dd_transporter,
        coalesce(
            et.num_texs_dd_as_destination > 0, false
        )                                              as texs_dd_destination,
        coalesce(
            et.num_texs_dd_as_emitter > 0
            or et.num_texs_dd_as_transporter > 0
            or et.num_texs_dd_as_destination > 0, false
        )                                              as texs_dd,
        s.dnd,
        s.dnd_emitter,
        s.dnd_destination,
        s.texs,
        s.texs_destination,
        s.texs_emitter,
        s.ssd,
        s.pnttd,
        c.created_at                         as date_inscription,
        c.company_types                      as profils,
        c.collector_types                    as profils_collecteur,
        c.waste_processor_types              as profils_installation,
        c.waste_vehicles_types               as profils_installation_vhu,
        wc.has_sub_section_four              as worker_company_has_sub_section_four,
        wc.has_sub_section_three             as worker_company_has_sub_section_three,
        se.code_commune_etablissement        as code_commune_insee,
        cgc.code_departement                 as code_departement_insee,
        cgc.code_region                      as code_region_insee,
        cgco.code_collectivite_outre_mer     as code_collectivite_outre_mer_insee,
        c.address                            as adresse_td,
        c.latitude                           as latitude_td,
        c.longitude                          as longitude_td,
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
    from stats as s
    left join {{ ref('etablissements_texs_dd') }} as et on s.siret = et.siret
    left join {{ ref("stock_etablissement") }} as se on s.siret = se.siret
    left join {{ ref("company") }} as c on s.siret = c.siret
    left join {{ ref('worker_certification') }} wc on wc.id=c.worker_certification_id
    left join
        {{ ref("code_geo_communes") }} as cgc
        on
            se.code_commune_etablissement = cgc.code_commune
            and cgc.type_commune != 'COMD'
    left join
        {{ ref("code_geo_territoires_outre_mer") }} as cgco
        on
            se.code_commune_etablissement = cgco.code_zonage_outre_mer
            and cgc.type_commune != 'COMD'
)

select
    siret,
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
    processing_operation_dnd,
    processing_operation_texs,
    waste_codes_bordereaux,
    waste_codes_dnd_statements,
    waste_codes_texs_statements,
    waste_codes_processed,
    code_commune_insee,
    code_departement_insee,
    code_region_insee,
    code_collectivite_outre_mer_insee,
    adresse_td,
    adresse_insee,
    latitude_td,
    longitude_td
from joined
