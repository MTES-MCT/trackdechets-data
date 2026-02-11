{{
  config(
    materialized = 'table',
    )
}}

with dnd as (
    select * from {{ ref('latest_registry_outgoing_waste') }}
    where not {{dangerous_waste_filter('registry')}}
)

select
    id as id,
    created_at as date_creation,
    updated_at as date_mise_a_jour,
    is_cancelled as declaration_annulee,
    public_id as id_public,
    report_for_company_siret as expediteur_siret_etablissement,
    report_for_company_name as expediteur_nom_etablissement,
    report_for_company_address as expediteur_adresse_etablissement,
    report_for_company_city as expediteur_ville_etablissement,
    report_for_company_postal_code as expediteur_code_postal_etablissement,
    report_for_pickup_site_name as lieu_de_collecte_nom_etablissement,
    report_for_pickup_site_address as lieu_de_collecte_adresse_etablissement,
    report_for_pickup_site_postal_code as lieu_de_collecte_code_postal_etablissement,
    report_for_pickup_site_city as lieu_de_collecte_ville_etablissement,
    report_for_pickup_site_country_code as lieu_de_collecte_code_pays_etablissement,
    report_as_company_siret as declarant_siret_etablissement,
    waste_description as dechet_denomination,
    waste_code as dechet_code,
    waste_code_bale as dechet_code_bale,
    waste_pop as dechet_pop,
    waste_is_dangerous as dechet_declare_dangereux,
    dispatch_date as date_expedition,
    weight_value as dechet_quantite,
    multiIf(weight_is_estimate, 'estimee', 'reelle') as dechet_quantite_type,
    volume as dechet_volume,
    initial_emitter_company_type as emetteur_initial_profil_etablissement,
    initial_emitter_company_org_id as emetteur_initial_id_etablissement,
    initial_emitter_company_name as emetteur_initial_nom_etablissement,
    initial_emitter_company_address as emetteur_initial_adresse_etablissement,
    initial_emitter_company_postal_code as emetteur_initial_code_postal_etablissement,
    initial_emitter_company_city as emetteur_initial_ville_etablissement,
    initial_emitter_company_country_code as emetteur_initial_code_pays_etablissement,
    initial_emitter_municipalities_insee_codes as emetteur_initial_municipalites_codes_insee_etablissement,
    destination_company_type as destinataire_profil_etablissement,
    destination_company_org_id as destinataire_siret_etablissement,
    destination_company_name as destinataire_nom_etablissement,
    destination_company_address as destinataire_adresse_etablissement,
    destination_company_city as destinataire_code_postal_etablissement,
    destination_company_postal_code as destinataire_ville_etablissement,
    destination_company_country_code as destinataire_code_pays_etablissement,
    destination_drop_site_address as destinataire_adresse_lieu_de_depot,
    destination_drop_site_postal_code as destinataire_code_postal_lieu_de_depot,
    destination_drop_site_city as destinataire_ville_lieu_de_depot,
    destination_drop_site_country_code as destinataire_code_pays_lieu_de_depot,
    operation_code as traitement_realise_code,
    operation_mode as traitement_realise_mode,
    eco_organisme_siret as eco_organisme_siret,
    eco_organisme_name as eco_organisme_nom,
    broker_company_siret as courtier_siret_etablissement,
    broker_company_name as courtier_nom_etablissement,
    broker_recepisse_number as courtier_numero_recepisse,
    trader_company_siret as negociant_siret_etablissement,
    trader_company_name as negociant_nom_etablissement,
    trader_recepisse_number as negociant_numero_recepisse,
    arrayFilter(
        x -> notEmpty(x),
		transporters_org_ids
        )
    as numeros_identification_transporteurs
from dnd
