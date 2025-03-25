with transporters as (
    select
        bsff_id,
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
        {{ ref('bsff_transporter') }}
    group by 1
)

select
    id,
    created_at                            as date_creation,
    updated_at                            as date_mise_a_jour,
    status                                as statut,
    type                                  as bsff_type,
    emitter_company_name                  as emetteur_nom_etablissement,
    emitter_company_siret                 as emetteur_siret_etablissement,
    emitter_company_address               as emetteur_adresse_etablissement,
    emitter_commune                       as emetteur_code_commune,
    emitter_departement                   as emetteur_code_departement,
    emitter_region                        as emetteur_code_region,
    emitter_naf                           as emetteur_code_naf,
    emitter_emission_signature_date       as emetteur_date_signature_emission,
    t.transporters_company_name           as transporteurs_noms_etablissements,
    t.transporters_company_siret          as transporteurs_sirets_etablissements,
    t.transporters_company_address        as transporteurs_adresses_etablissements,
    t.transporter_transport_taken_over_at as transporteur_date_prise_en_charge,
    transporter_transport_signature_date  as transporteur_date_signature,
    destination_company_name              as destinataire_nom_etablissement,
    destination_company_siret             as destinataire_siret_etablissement,
    destination_company_address           as destinataire_adresse_etablissement,
    destination_commune                   as destinataire_code_commune,
    destination_departement               as destinataire_code_departement,
    destination_region                    as destinataire_code_region,
    destination_naf                       as destinataire_code_naf,
    destination_cap                       as destinataire_cap_etablissement,
    destination_reception_signature_date  as destinataire_date_signature_reception,
    destination_reception_date            as destinataire_date_reception,
    destination_planned_operation_code    as destinataire_operation_traitement_prevue_code,
    last_operation_date_packagings        as destinataire_date_derniere_operation_traitement_contenants,
    operations_codes_packagings           as destinataire_operations_traitements_realisees_codes_contenants,
    num_accepted_packagings               as contenants_nombre_acceptes,
    num_processed_packagings              as contenants_nombre_traites,
    num_packagings                        as contenants_nombre,
    waste_code                            as dechet_code,
    waste_description                     as dechet_denomination_usuelle,
    waste_adr                             as dechet_adr,
    weight_value                          as dechet_quantite,
    weight_is_estimate                    as dechet_quantite_type,
    accepted_quantity_packagings          as dechet_quantite_acceptee_contenants
from
    {{ ref('bsff_enriched') }} as b
left join transporters as t
    on b.id = t.bsff_id
where
    not is_deleted
    and not is_draft
