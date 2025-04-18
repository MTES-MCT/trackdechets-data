
{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":8
    }
    )
}}

select
    id,
    created_at                                                as date_creation,
    updated_at                                                as date_mise_a_jour,
    status                                                    as statut,
    emitter_company_name                                      as emetteur_nom_etablissement,
    emitter_company_siret                                     as emetteur_siret_etablissement,
    emitter_company_address                                   as emetteur_adresse_etablissement,
    emitter_commune                                           as emetteur_code_commune,
    emitter_departement                                       as emetteur_code_departement,
    emitter_region                                            as emetteur_code_region,
    emitter_naf                                               as emetteur_code_naf,
    emitter_emission_signature_date                           as emetteur_date_signature_emission,
    transporter_company_name                                  as transporteur_nom_etablissement,
    transporter_company_siret                                 as transporteur_siret_etablissement,
    transporter_company_address                               as transporteur_adresse_etablissement,
    transporter_transport_signature_date                      as transporteur_date_signature,
    transporter_transport_taken_over_at                       as transporteur_date_prise_en_charge,
    destination_type                                          as destinataire_type,
    destination_company_name                                  as destinataire_nom_etablissement,
    destination_company_siret                                 as destinataire_siret_etablissement,
    destination_company_address                               as destinataire_adresse_etablissement,
    destination_commune                                       as destinataire_code_commune,
    destination_departement                                   as destinataire_code_departement,
    destination_region                                        as destinataire_code_region,
    destination_naf                                           as destinataire_code_naf,
    destination_reception_acceptation_status                  as destinataire_dechet_statut_acceptation,
    destination_reception_refusal_reason                      as destinataire_dechet_raison_refus,
    destination_reception_date                                as destinataire_date_reception,
    destination_reception_quantity                            as destinataire_nombre_vhu_recus,
    destination_reception_weight                              as destinataire_quantite_recue,
    destination_planned_operation_code                        as destinataire_operation_traitement_prevue_code,
    destination_operation_signature_date                      as destinataire_date_signature_traitement,
    destination_operation_date                                as destinataire_date_operation_traitement,
    destination_operation_code                                as destinataire_operation_traitement_realisee_code,
    destination_operation_mode                                as destinataire_operation_traitement_realisee_mode,
    destination_operation_next_destination_company_vat_number as destinataire_ulterieur_numero_tva_etablissement,
    destination_operation_next_destination_company_name       as destinataire_ulterieur_nom_etablissement,
    destination_operation_next_destination_company_siret      as destinataire_ulterieur_siret_etablissement,
    destination_operation_next_destination_company_address    as destinataire_ulterieur_adresse_etablissement,
    waste_code                                                as dechet_code,
    quantity                                                  as dechet_nombre_vhu,
    weight_value                                              as dechet_quantite,
    weight_is_estimate                                        as dechet_type_quantite,
    packaging                                                 as contenants
from {{ ref('bsvhu_enriched') }}
