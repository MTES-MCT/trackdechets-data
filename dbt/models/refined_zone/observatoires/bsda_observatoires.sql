{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":8
    }
    )
}}

with transporters as (
    select
        bsda_id,
        array_agg(
            transporter_company_siret
            
        ) as transporters_company_siret,
        array_agg(
            transporter_company_name
            
        ) as transporters_company_name,
        array_agg(
            transporter_company_address
            
        ) as transporters_company_address,
        min(
            transporter_transport_taken_over_at
        ) as transporter_transport_taken_over_at
    from
        {{ ref('bsda_transporter') }}
    group by 1
)

select
    id,
    created_at                                                    as date_creation,
    updated_at                                                    as date_mise_a_jour,
    status                                                        as statut,
    type                                                          as emetteur_type,
    emitter_is_private_individual                                 as emetteur_est_particulier,
    emitter_company_name                                          as emetteur_nom_etablissement,
    emitter_company_siret                                         as emetteur_siret_etablissement,
    emitter_company_address                                       as emetteur_adresse_etablissement,
    emitter_commune                                               as emetteur_code_commune,
    emitter_departement                                           as emetteur_code_departement,
    emitter_region                                                as emetteur_code_region,
    emitter_naf                                                   as emetteur_code_naf,
    emitter_pickup_site_name                                      as emetteur_nom_chantier,
    emitter_pickup_site_address                                   as emetteur_adresse_chantier,
    emitter_pickup_site_city                                      as emetteur_ville_chantier,
    emitter_pickup_site_postal_code                               as emetteur_code_postal_chantier,
    emitter_pickup_site_infos                                     as emetteur_informations_chantier,
    emitter_emission_signature_date                               as emetteur_date_signature_emission,
    worker_company_name                                           as entreprise_travaux_nom_etablissement,
    worker_company_siret                                          as entreprise_travaux_siret_etablissement,
    worker_company_address                                        as entreprise_travaux_adresse_etablissement,
    worker_certification_has_sub_section_three                    as entreprise_travaux_sous_section_trois,
    worker_certification_has_sub_section_four                     as entreprise_travaux_sous_section_quatre,
    worker_work_signature_date                                    as entreprise_travaux_date_signature,
    t.transporters_company_name                                   as transporteurs_noms_etablissements,
    t.transporters_company_siret                                  as transporteurs_sirets_etablissements,
    t.transporters_company_address                                as transporteurs_adresses_etablissements,
    t.transporter_transport_taken_over_at                         as transporteur_date_prise_en_charge,
    transporter_transport_signature_date                          as transporteur_date_signature,
    destination_company_name                                      as destinataire_nom_etablissement,
    destination_company_siret                                     as destinataire_siret_etablissement,
    destination_company_address                                   as destinataire_adresse_etablissement,
    destination_commune                                           as destinataire_code_commune,
    destination_departement                                       as destinataire_code_departement,
    destination_region                                            as destinataire_code_region,
    destination_naf                                               as destinataire_code_naf,
    destination_cap                                               as destinataire_cap_etablissement,
    destination_reception_date                                    as destinataire_date_reception,
    destination_reception_weight                                  as destinataire_quantite_recue,
    destination_reception_acceptation_status                      as destinataire_dechet_statut_acceptation,
    destination_reception_refusal_reason                          as destinataire_dechet_raison_refus,
    destination_operation_signature_date                          as destinataire_date_signature_traitement,
    destination_planned_operation_code                            as destinataire_operation_traitement_prevue_code,
    destination_operation_date                                    as destinataire_date_operation_traitement,
    destination_operation_code                                    as destinataire_operation_traitement_realisee_code,
    destination_operation_description                             as destinataire_operation_traitement_realisee_description,
    destination_operation_mode                                    as destinataire_operation_traitement_realisee_mode,
    waste_code                                                    as dechet_code,
    waste_family_code                                             as dechet_code_famille,
    waste_material_name                                           as dechet_nom_materiau,
    waste_consistence                                             as dechet_consistance,
    waste_pop                                                     as dechet_pop,
    waste_adr                                                     as dechet_adr,
    weight_value                                                  as dechet_quantite,
    weight_is_estimate                                            as dechet_quantite_type,
    packagings                                                    as contenants,
    destination_operation_next_destination_company_vat_number     as destinataire_ulterieur_numero_tva_etablissement,
    destination_operation_next_destination_company_name           as destinataire_ulterieur_nom_etablissement,
    destination_operation_next_destination_company_siret          as destinataire_ulterieur_siret_etablissement,
    destination_operation_next_destination_company_address        as destinataire_ulterieur_adresse_etablissement,
    destination_operation_next_destination_cap                    as destinataire_ulterieur_cap,
    destination_operation_next_destination_planned_operation_code as destinataire_ulterieur_operation_prevue_code,
    eco_organisme_name                                            as eco_organisme_nom,
    eco_organisme_siret,
    broker_company_name                                           as courtier_nom_etablissement,
    broker_company_siret                                          as courtier_siret_etablissement,
    broker_company_address                                        as courtier_adresse_etablissement,
    broker_recepisse_department                                   as courtier_code_departement_etablissement
from
    {{ ref('bsda_enriched') }} as b
left join transporters as t
    on b.id = t.bsda_id
where
    not is_deleted
    and not is_draft
