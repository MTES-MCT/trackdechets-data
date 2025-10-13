{{
  config(
    materialized = 'table',
    )
}}

with texs as (
    select * from {{ ref('texs_entrant') }}
)

select
    id                                         as id,
    created_at                                 as date_creation,
    updated_at                                 as date_mise_a_jour,

    is_cancelled                               as declaration_annulee,

    public_id                                  as id_public,
    report_as_company_siret                    as declarant_siret_etablissement,
    report_for_company_siret
        as destinataire_siret_etablissement,
    report_for_company_name
        as destinataire_nom_etablissement,
    report_for_company_address
        as destinataire_adresse_etablissement,
    report_for_company_city
        as destinataire_ville_etablissement,
    report_for_company_postal_code
        as destinataire_code_postal_etablissement,
    reception_date                             as destinataire_date_reception,

    waste_code                                 as dechet_code,
    waste_description                          as dechet_denomination,
    waste_pop                                  as dechet_pop,
    waste_is_dangerous                         as dechet_declare_dangereux,
    waste_code_bale                            as dechet_code_bale,

    weight_value                               as dechet_quantite,
    weight_is_estimate                         as dechet_quantite_type,
    volume                                     as dechet_volume,
    parcel_insee_codes                         as parcelles_codes_insee,
    parcel_numbers                             as parcelles_numeros,
    parcel_coordinates                         as parcelles_coordonnees,
    initial_emitter_company_type
        as emetteur_initial_siret_etablissement,
    initial_emitter_company_org_id
        as emetteur_initial_id_etablissement,
    initial_emitter_company_name
        as emetteur_initial_nom_etablissement,
    initial_emitter_company_address
        as emetteur_initial_adresse_etablissement,
    initial_emitter_company_postal_code
        as emetteur_initial_code_postal_etablissement,
    initial_emitter_company_city
        as emetteur_initial_ville_etablissement,
    initial_emitter_company_country_code
        as emetteur_initial_code_pays_etablissement,
    initial_emitter_municipalities_insee_codes
        as emetteur_initial_municipalites_codes_insee_etablissement,
    emitter_company_type                       as emetteur_profil_etablissement,

    emitter_company_name                       as emetteur_nom_etablissement,
    emitter_pickup_site_address
        as emetteur_adresse_point_de_retrait,
    emitter_pickup_site_postal_code
        as emetteur_code_postal_point_de_retrait,
    emitter_pickup_site_city
        as emetteur_ville_point_de_retrait,
    emitter_pickup_site_country_code
        as emetteur_code_pays_point_de_retrait,
    emitter_company_address
        as emetteur_adresse_etablissement,
    emitter_company_postal_code
        as emetteur_code_postal_etablissement,
    emitter_company_city                       as emetteur_ville_etablissement,
    emitter_company_country_code
        as emetteur_code_pays_etablissement,
    broker_company_siret                       as courtier_siret_etablissement,
    broker_company_name                        as courtier_nom_etablissement,
    -- ?? broker_recepisse_number as courtier_numero_recepisse_etablissement,
    trader_company_siret                       as negociant_siret_etablissement,
    trader_company_name                        as negociant_nom_etablissement,
    -- ?? trader_recepisse_number as negociant_numero_recepisse_etablissement,
    operation_code
        as operation_traitement_realisee_code,
    operation_mode
        as operation_traitement_realisee_mode,
    no_traceability                            as rupture_tracabilite,
    movement_number                            as numero_mouvement,
    next_operation_code
        as operation_traitement_prevue_code,
    -- ??is_upcycled as operation_traitement_realisee_upcycle,
    destination_parcel_insee_codes
        as dechet_identifiants_parcelles_code_insee,
    destination_parcel_numbers
        as dechet_identifiants_parcelles_numero,
    destination_parcel_coordinates
        as dechet_identifiants_parcelles_coordonnees,
    transporter1_transport_mode                as transporteur1_mode_transport,
    transporter1_company_type
        as transporteur1_profil_etablissement,
    transporter1_company_org_id
        as transporteur1_id_etablissement,
    -- ?? transporter1_recepisse_is_exempted as transporteur_1_recepisse_optionnel,
    -- ?? transporter1_recepisse_number as transporteur_1_numero_recepisse,
    transporter1_company_name
        as transporteur_1_nom_etablissement,
    transporter1_company_address
        as transporteur_1_adresse_etablissement,
    transporter1_company_postal_code
        as transporteur_1_code_postal_etablissement,
    transporter1_company_city
        as transporteur_1_ville_etablissement,
    transporter1_company_country_code
        as transporteur_1_code_pays_etablissement,
    transporter2_transport_mode                as transporteur2_mode_transport,
    transporter2_company_type
        as transporteur2_profil_etablissement,
    transporter2_company_org_id
        as transporteur2_id_etablissement,
    -- ?? transporter2_recepisse_is_exempted as transporteur2_recepisse_optionnel,
    -- ?? transporter2_recepisse_number as transporteur2_numero_recepisse,
    transporter2_company_name
        as transporteur2_nom_etablissement,
    transporter2_company_address
        as transporteur2_adresse_etablissement,
    transporter2_company_postal_code
        as transporteur2_code_postal_etablissement,
    transporter2_company_city
        as transporteur2_ville_etablissement,
    transporter2_company_country_code
        as transporteur2_code_pays_etablissement,
    transporter3_transport_mode                as transporteur3_mode_transport,
    transporter3_company_type
        as transporteur3_profil_etablissement,
    transporter3_company_org_id
        as transporteur3_id_etablissement,
    -- ?? transporter3_recepisse_is_exempted as transporteur3_recepisse_optionnel,
    -- ?? transporter3_recepisse_number as transporteur3_numero_recepisse,
    transporter3_company_name
        as transporteur3_nom_etablissement,
    transporter3_company_address
        as transporteur3_adresse_etablissement,
    transporter3_company_postal_code
        as transporteur3_code_postal_etablissement,
    transporter3_company_city
        as transporteur3_ville_etablissement,
    transporter3_company_country_code
        as transporteur3_code_pays_etablissement,
    transporter4_transport_mode                as transporteur4_mode_transport,
    transporter4_company_type
        as transporteur4_profil_etablissement,
    transporter4_company_org_id
        as transporteur4_id_etablissement,
    -- ?? transporter4_recepisse_is_exempted as transporteur4_recepisse_optionnel,
    -- ?? transporter4_recepisse_number as transporteur4_numero_recepisse,
    transporter4_company_name
        as transporteur4_nom_etablissement,
    transporter4_company_address
        as transporteur4_adresse_etablissement,
    transporter4_company_postal_code
        as transporteur4_code_postal_etablissement,
    transporter4_company_city
        as transporteur4_ville_etablissement,
    transporter4_company_country_code
        as transporteur4_code_pays_etablissement,
    transporter5_transport_mode                as transporteur5_mode_transport,
    transporter5_company_type
        as transporteur5_profil_etablissement,
    transporter5_company_org_id
        as transporteur5_id_etablissement,
    -- ?? transporter5_recepisse_is_exempted as transporteur5_recepisse_optionnel,
    -- ?? transporter5_recepisse_number as transporteur5_numero_recepisse,
    transporter5_company_name
        as transporteur5_nom_etablissement,
    transporter5_company_address
        as transporteur5_adresse_etablissement,
    transporter5_company_postal_code
        as transporteur5_code_postal_etablissement,
    transporter5_company_city
        as transporteur5_ville_etablissement,
    transporter5_company_country_code
        as transporteur5_code_pays_etablissement,
    emitter_pickup_site_name                   as emetteur_nom_point_de_retrait,
    -- ?? is_direct_supply as livraison_directe,
    eco_organisme_name                         as eco_organisme_nom,
    eco_organisme_siret                        as eco_organisme_siret
-- sis_identifier as ,
-- ttd_import_number as ,
from texs
