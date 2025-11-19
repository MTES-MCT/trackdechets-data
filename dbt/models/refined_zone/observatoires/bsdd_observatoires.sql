{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":64
    }
    )
}}

with transporters as (
    select
        form_id,
        array_agg(
            transporter_company_siret
        )                  as transporters_company_siret,
        array_agg(
            transporter_company_name
        )                  as transporters_company_name,
        array_agg(
            transporter_company_address
        )                  as transporters_company_address,
        min(taken_over_at) as taken_over_at
    from {{ ref('bsdd_transporter') }}
    group by 1
)

select
    readable_id                           as id,
    created_at                            as date_creation,
    updated_at                            as date_mise_a_jour,
    status                                as statut,
    emitted_by_eco_organisme              as emis_par_eco_organisme,
    emitter_type                          as emetteur_type,
    emitter_is_foreign_ship               as emetteur_navire_etranger,
    emitter_company_omi_number            as emetteur_numero_omi,
    emitter_is_private_individual         as emetteur_est_particulier,
    emitter_company_name                  as emetteur_nom_etablissement,
    emitter_company_siret                 as emetteur_siret_etablissement,
    emitter_company_address               as emetteur_adresse_etablissement,
    emitter_work_site_name                as emetteur_nom_chantier,
    emitter_work_site_address             as emetteur_adresse_chantier,
    emitter_work_site_city                as emetteur_ville_chantier,
    emitter_work_site_postal_code         as emetteur_code_postal_chantier,
    emitter_work_site_infos               as emetteur_informations_chantier,
    emitter_commune                       as emetteur_code_commune,
    emitter_departement                   as emetteur_code_departement,
    emitter_region                        as emetteur_code_region,
    emitter_naf                           as emetteur_code_naf,
    emitted_at                            as emetteur_date_signature_emission,
    quantity_grouped                      as emetteur_quantite_groupee,
    t.transporters_company_name           as transporteurs_noms_etablissements,
    t.transporters_company_siret
        as transporteurs_sirets_etablissements,
    t.transporters_company_address
        as transporteurs_adresses_etablissements,
    t.taken_over_at                       as transporteur_date_prise_en_charge,
    recipient_company_name                as destinataire_nom_etablissement,
    recipient_company_siret               as destinataire_siret_etablissement,
    recipient_company_address             as destinataire_adresse_etablissement,
    recipient_commune                     as destinataire_code_commune,
    recipient_departement                 as destinataire_code_departement,
    recipient_region                      as destinataire_code_region,
    recipient_naf                         as destinataire_code_naf,
    recipient_cap                         as destinataire_cap_etablissement,
    recipient_is_temp_storage
        as destinataire_entreposage_provisoire,
    received_at                           as destinataire_date_reception,
    signed_at
        as destinataire_date_signature_acceptation,
    quantity_received                     as destinataire_quantite_recue,
    quantity_received_type                as destinataire_quantite_recue_type,
    waste_acceptation_status
        as destinataire_dechet_statut_acceptation,
    waste_refusal_reason                  as destinataire_dechet_raison_refus,
    quantity_refused                      as destinataire_quantite_refusee,
    recipient_processing_operation
        as destinataire_operation_traitement_prevue_code,
    processed_at
        as destinataire_date_operation_traitement,
    processing_operation_done
        as destinataire_operation_traitement_realisee_code,
    processing_operation_description
        as destinataire_operation_traitement_realisee_description,
    destination_operation_mode
        as destinataire_operation_traitement_realisee_mode,
    waste_details_code                    as dechet_code,
    waste_details_onu_code                as dechet_code_onu,
    waste_details_pop                     as dechet_pop,
    waste_details_is_dangerous            as dechet_declare_dangereux,
    waste_details_name                    as dechet_denomination_usuelle,
    waste_details_quantity                as dechet_quantite,
    waste_details_quantity_type           as dechet_quantite_type,
    waste_details_consistence             as dechet_consistance,
    waste_details_packaging_infos         as dechets_conditionnements,
    waste_details_parcel_numbers          as dechet_identifiants_parcelles,
    waste_details_land_identifiers        as dechet_identifiants_terrains,
    no_traceability                       as rupture_tracabilite,
    next_destination_company_vat_number
        as destinataire_ulterieur_numero_tva_etablissement,
    next_destination_company_name
        as destinataire_ulterieur_nom_etablissement,
    next_destination_company_siret
        as destinataire_ulterieur_siret_etablissement,
    next_destination_company_address
        as destinataire_ulterieur_adresse_etablissement,
    next_destination_company_country
        as destinataire_ulterieur_pays_etablissement,
    next_destination_processing_operation
        as destinataire_ulterieur_operation_prevue_code,
    eco_organisme_name                    as eco_organisme_nom,
    eco_organisme_siret,
    trader_company_name                   as negociant_nom_etablissement,
    trader_company_siret                  as negociant_siret_etablissement,
    trader_company_address                as negociant_adresse_etablissement,
    trader_department
        as negociant_code_departement_etablissement,
    broker_company_name                   as courtier_nom_etablissement,
    broker_company_siret                  as courtier_siret_etablissement,
    broker_company_address                as courtier_adresse_etablissement,
    broker_department
        as courtier_code_departement_etablissement
from
    {{ ref('bsdd_enriched') }} as b
left join transporters as t on b.id = t.form_id
where not is_deleted and status != 'DRAFT'
