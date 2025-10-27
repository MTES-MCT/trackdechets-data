{{
  config(
    materialized = 'table',
    )
}}

with dnd as (
    select * from {{ ref('dnd_entrant') }}
)

select
    id                                    as identifiant,
    created_year_utc                      as annee_creation_utc,
    code_dechet                           as code_dechet,
    created_date                          as date_creation,
    date_reception                        as date_reception,
    is_dechet_pop                         as est_dechet_pop,
    denomination_usuelle,
    heure_pesee,
    last_modified_date                    as date_derniere_modification,
    numero_document                       as numero_document,
    numero_notification                   as numero_notification,
    numero_saisie                         as numero_saisie,
    quantite                              as quantite,
    code_traitement                       as code_traitement,
    etablissement_id                      as id_etablissement,
    etablissement_numero_identification
        as numero_identification_etablissement,
    -- created_by_id as created_by_id,
    -- last_modified_by_id as last_modified_by_id,
    code_unite                            as code_unite,
    --  public_id as public_id,
    delegation_id                         as id_delegation,
    origine                               as origine,
    code_dechet_bale                      as code_dechet_bale,
    identifiant_metier                    as identifiant_metier,
    canceled_by_id                        as annule_par_id,
    canceled_comment                      as commentaire_annulation,
    canceled_date                         as date_annulation,
    -- import_id as id_import,
    producteur_type,
    producteur_numero_identification,
    producteur_raison_sociale,
    producteur_adresse_libelle,
    producteur_adresse_commune,
    producteur_adresse_code_postal,
    producteur_adresse_pays,
    expediteur_type,
    expediteur_numero_identification,
    expediteur_raison_sociale,
    expediteur_adresse_prise_en_charge,
    expediteur_adresse_libelle,
    expediteur_adresse_commune,
    expediteur_adresse_code_postal,
    expediteur_adresse_pays,
    eco_organisme_type                    as type_eco_organisme,
    eco_organisme_numero_identification
        as numero_identification_eco_organisme,
    eco_organisme_raison_sociale          as raison_sociale_eco_organisme,
    courtier_type,
    courtier_numero_identification,
    courtier_raison_sociale,
    courtier_numero_recepisse,
    numeros_indentification_transporteurs
        as numeros_identification_transporteurs
from dnd
