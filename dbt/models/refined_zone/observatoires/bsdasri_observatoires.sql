{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":16
    }
    )
}}

select
    id,
    created_at                                       as date_creation,
    updated_at                                       as date_mise_a_jour,
    type                                             as bsdasri_type,
    status                                           as statut,
    emitted_by_eco_organisme                         as emis_par_eco_organisme,
    emitter_company_name                             as emetteur_nom_etablissement,
    emitter_company_siret                            as emetteur_siret_etablissement,
    emitter_company_address                          as emetteur_adresse_etablissement,
    emitter_commune                                  as emetteur_code_commune,
    emitter_departement                              as emetteur_code_departement,
    emitter_region                                   as emetteur_code_region,
    emitter_naf                                      as emetteur_code_naf,
    emitter_pickup_site_name                         as emetteur_nom_chantier,
    emitter_pickup_site_address                      as emetteur_adresse_chantier,
    emitter_pickup_site_city                         as emetteur_ville_chantier,
    emitter_pickup_site_postal_code                  as emetteur_code_postal_chantier,
    emitter_pickup_site_infos                        as emetteur_informations_chantier,
    emitter_emission_signature_date                  as emetteur_date_signature_emission,
    transporter_company_name                         as transporteur_nom_etablissement,
    transporter_company_siret                        as transporteur_siret_etablissement,
    transporter_company_address                      as transporteur_adresse_etablissement,
    transporter_acceptation_status                   as transporteur_dechet_statut_acceptation,
    transporter_waste_weight_value                   as transporteur_dechet_quantite,
    transporter_waste_volume                         as transporteur_dechet_volume,
    transporter_waste_refusal_reason                 as transporteur_dechet_raison_refus,
    transporter_waste_refused_weight_value           as transporteur_dechet_quantite_refus,
    transporter_taken_over_at                        as transporteur_date_prise_en_charge,
    transporter_transport_signature_date             as transporteur_date_signature,
    destination_company_name                         as destinataire_nom_etablissement,
    destination_company_siret                        as destinataire_siret_etablissement,
    destination_company_address                      as destinataire_adresse_etablissement,
    destination_commune                              as destinataire_code_commune,
    destination_departement                          as destinataire_code_departement,
    destination_region                               as destinataire_code_region,
    destination_naf                                  as destinataire_code_naf,
    destination_reception_acceptation_status         as destinataire_dechet_statut_acceptation,
    destination_reception_waste_refusal_reason       as destinataire_dechet_raison_refus,
    destination_reception_waste_refused_weight_value as destinataire_dechet_quantite_refus,
    destination_reception_waste_weight_value         as destinataire_quantite_recue,
    destination_reception_waste_volume               as destinataire_volume_recu,
    destination_reception_signature_date             as destinataire_date_signature_reception,
    destination_reception_date                       as destinataire_date_reception,
    destination_operation_code                       as destinataire_operation_traitement_realisee_code,
    destination_operation_mode                       as destinataire_operation_traitement_realisee_mode,
    destination_operation_signature_date             as destinataire_date_signature_traitement,
    destination_operation_date                       as destinataire_date_operation_traitement,
    transporter_transport_mode                       as transporteur_mode_transport,
    waste_code                                       as dechet_code,
    waste_adr                                        as dechet_adr,
    emitter_waste_weight_value                       as dechet_quantite,
    emitter_waste_weight_is_estimate                 as dechet_type_quantite,
    emitter_waste_volume                             as dechet_volume,
    emitter_waste_packagings                         as contenants,
    eco_organisme_name                               as eco_organisme_nom,
    eco_organisme_siret
from
    {{ ref('bsdasri_enriched') }}
where
    not is_deleted
    and not is_draft
