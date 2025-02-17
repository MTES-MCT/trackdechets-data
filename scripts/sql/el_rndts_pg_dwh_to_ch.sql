-- create database

create database raw_zone_rndts;

--- dnd_entrant
create table if not exists raw_zone_rndts.dnd_entrant engine = MergeTree ORDER BY () as (
SELECT
    assumeNotNull(toInt256("dnd_entrant_id")) as dnd_entrant_id,
    assumeNotNull(toString("created_year_utc")) as created_year_utc,
    assumeNotNull(toString("code_dechet")) as code_dechet,
    assumeNotNull(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    assumeNotNull(toDate("date_reception")) as date_reception,
    toNullable(toBool("is_dechet_pop")) as is_dechet_pop,
    assumeNotNull(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toString("heure_pesee")) as heure_pesee,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    assumeNotNull(toDecimal256("quantite", 9)) as quantite,
    assumeNotNull(toString("code_traitement")) as code_traitement,
    assumeNotNull(toInt256("etablissement_id")) as etablissement_id,
    assumeNotNull(toInt256("created_by_id")) as created_by_id,
    assumeNotNull(toInt256("last_modified_by_id")) as last_modified_by_id,
    assumeNotNull(toString("unite_code")) as unite_code,
    assumeNotNull(toString("public_id")) as public_id,
    toNullable(toInt256("delegation_id")) as delegation_id,
    toNullable(toString("origine")) as origine,
    toNullable(toString("code_dechet_bale")) as code_dechet_bale,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toNullable(toString("producteur_type")) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    toNullable(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    toNullable(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    toNullable(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    toNullable(toString("producteur_adresse_code_postal")) as producteur_adresse_code_postal,
    toNullable(toString("producteur_adresse_pays")) as producteur_adresse_pays,
    assumeNotNull(toString("expediteur_type")) as expediteur_type,
    toNullable(toString("expediteur_numero_identification")) as expediteur_numero_identification,
    assumeNotNull(toString("expediteur_raison_sociale")) as expediteur_raison_sociale,
    toNullable(toString("expediteur_adresse_prise_en_charge")) as expediteur_adresse_prise_en_charge,
    assumeNotNull(toString("expediteur_adresse_libelle")) as expediteur_adresse_libelle,
    assumeNotNull(toString("expediteur_adresse_commune")) as expediteur_adresse_commune,
    assumeNotNull(toString("expediteur_adresse_code_postal")) as expediteur_adresse_code_postal,
    assumeNotNull(toString("expediteur_adresse_pays")) as expediteur_adresse_pays,
    toNullable(toString("eco_organisme_type")) as eco_organisme_type,
    toNullable(toString("eco_organisme_numero_identification")) as eco_organisme_numero_identification,
    toNullable(toString("eco_organisme_raison_sociale")) as eco_organisme_raison_sociale,
    toNullable(toString("courtier_type")) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse
 FROM pg_dwh_raw_zone_rndts.dnd_entrant);

-- dnd sortant
create table if not exists raw_zone_rndts.dnd_sortant engine = MergeTree ORDER BY () as (
SELECT
    assumeNotNull(toInt256("dnd_sortant_id")) as dnd_sortant_id,
    assumeNotNull(toString("created_year_utc")) as created_year_utc,
    assumeNotNull(toString("code_dechet")) as code_dechet,
    assumeNotNull(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    assumeNotNull(toDate("date_expedition")) as date_expedition,
    toNullable(toBool("is_dechet_pop")) as is_dechet_pop,
    assumeNotNull(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    assumeNotNull(toDecimal256("quantite", 9)) as quantite,
    assumeNotNull(toString("code_traitement")) as code_traitement,
    assumeNotNull(toInt256("etablissement_id")) as etablissement_id,
    assumeNotNull(toInt256("created_by_id")) as created_by_id,
    assumeNotNull(toInt256("last_modified_by_id")) as last_modified_by_id,
    assumeNotNull(toString("unite_code")) as unite_code,
    assumeNotNull(toString("public_id")) as public_id,
    assumeNotNull(toString("qualification_code")) as qualification_code,
    toNullable(toInt256("delegation_id")) as delegation_id,
    assumeNotNull(toString("origine")) as origine,
    toNullable(toString("code_dechet_bale")) as code_dechet_bale,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toNullable(toString("producteur_type")) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    toNullable(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    toNullable(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    toNullable(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    toNullable(toString("producteur_adresse_code_postal")) as producteur_adresse_code_postal,
    toNullable(toString("producteur_adresse_pays")) as producteur_adresse_pays,
    assumeNotNull(toString("destinataire_type")) as destinataire_type,
    toNullable(toString("destinataire_numero_identification")) as destinataire_numero_identification,
    assumeNotNull(toString("destinataire_raison_sociale")) as destinataire_raison_sociale,
    toNullable(toString("destinataire_adresse_destination")) as destinataire_adresse_destination,
    assumeNotNull(toString("destinataire_adresse_libelle")) as destinataire_adresse_libelle,
    assumeNotNull(toString("destinataire_adresse_commune")) as destinataire_adresse_commune,
    assumeNotNull(toString("destinataire_adresse_code_postal")) as destinataire_adresse_code_postal,
    assumeNotNull(toString("destinataire_adresse_pays")) as destinataire_adresse_pays,
    toNullable(toString("etablissement_origine_adresse_prise_en_charge")) as etablissement_origine_adresse_prise_en_charge,
    assumeNotNull(toString("etablissement_origine_adresse_libelle")) as etablissement_origine_adresse_libelle,
    assumeNotNull(toString("etablissement_origine_adresse_commune")) as etablissement_origine_adresse_commune,
    assumeNotNull(toString("etablissement_origine_adresse_code_postal")) as etablissement_origine_adresse_code_postal,
    assumeNotNull(toString("etablissement_origine_adresse_pays")) as etablissement_origine_adresse_pays,
    toNullable(toString("eco_organisme_type")) as eco_organisme_type,
    toNullable(toString("eco_organisme_numero_identification")) as eco_organisme_numero_identification,
    toNullable(toString("eco_organisme_raison_sociale")) as eco_organisme_raison_sociale,
    toNullable(toString("courtier_type")) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse
 FROM pg_dwh_raw_zone_rndts.dnd_sortant
);

-- dd_entrant
create table if not exists raw_zone_rndts.dd_entrant engine = MergeTree ORDER BY () as (
SELECT
    assumeNotNull(toInt256("dd_entrant_id")) as dd_entrant_id,
    assumeNotNull(toString("created_year_utc")) as created_year_utc,
    assumeNotNull(toString("public_id")) as public_id,
    assumeNotNull(toString("code_dechet")) as code_dechet,
    toNullable(toString("code_dechet_bale")) as code_dechet_bale,
    assumeNotNull(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    assumeNotNull(toDate("date_reception")) as date_reception,
    toNullable(toInt256("delegation_id")) as delegation_id,
    toNullable(toBool("is_dechet_pop")) as is_dechet_pop,
    assumeNotNull(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toString("heure_pesee")) as heure_pesee,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    assumeNotNull(toString("origine")) as origine,
    assumeNotNull(toDecimal256("quantite", 9)) as quantite,
    assumeNotNull(toString("code_traitement")) as code_traitement,
    assumeNotNull(toInt256("etablissement_id")) as etablissement_id,
    assumeNotNull(toInt256("created_by_id")) as created_by_id,
    assumeNotNull(toInt256("last_modified_by_id")) as last_modified_by_id,
    assumeNotNull(toString("unite_code")) as unite_code,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toNullable(toString("producteur_type")) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    toNullable(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    toNullable(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    toNullable(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    toNullable(toString("producteur_adresse_code_postal")) as producteur_adresse_code_postal,
    toNullable(toString("producteur_adresse_pays")) as producteur_adresse_pays,
    assumeNotNull(toString("expediteur_type")) as expediteur_type,
    toNullable(toString("expediteur_numero_identification")) as expediteur_numero_identification,
    assumeNotNull(toString("expediteur_raison_sociale")) as expediteur_raison_sociale,
    toNullable(toString("expediteur_adresse_prise_en_charge")) as expediteur_adresse_prise_en_charge,
    assumeNotNull(toString("expediteur_adresse_libelle")) as expediteur_adresse_libelle,
    assumeNotNull(toString("expediteur_adresse_commune")) as expediteur_adresse_commune,
    assumeNotNull(toString("expediteur_adresse_code_postal")) as expediteur_adresse_code_postal,
    assumeNotNull(toString("expediteur_adresse_pays")) as expediteur_adresse_pays,
    toNullable(toString("eco_organisme_type")) as eco_organisme_type,
    toNullable(toString("eco_organisme_numero_identification")) as eco_organisme_numero_identification,
    toNullable(toString("eco_organisme_raison_sociale")) as eco_organisme_raison_sociale,
    toNullable(toString("courtier_type")) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse
 FROM pg_dwh_raw_zone_rndts.dd_entrant
);

-- dd_sortant
create table if not exists raw_zone_rndts.dd_sortant engine = MergeTree ORDER BY () as (
SELECT
    assumeNotNull(toInt256("dd_sortant_id")) as dd_sortant_id,
    assumeNotNull(toString("created_year_utc")) as created_year_utc,
    assumeNotNull(toString("public_id")) as public_id,
    assumeNotNull(toString("code_dechet")) as code_dechet,
    toNullable(toString("code_dechet_bale")) as code_dechet_bale,
    assumeNotNull(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    assumeNotNull(toDate("date_expedition")) as date_expedition,
    toNullable(toInt256("delegation_id")) as delegation_id,
    assumeNotNull(toBool("is_dechet_pop")) as is_dechet_pop,
    assumeNotNull(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    assumeNotNull(toString("origine")) as origine,
    assumeNotNull(toDecimal256("quantite", 9)) as quantite,
    assumeNotNull(toString("qualification_code")) as qualification_code,
    assumeNotNull(toString("code_traitement")) as code_traitement,
    toNullable(toInt256("etablissement_origine_id")) as etablissement_origine_id,
    assumeNotNull(toInt256("etablissement_id")) as etablissement_id,
    assumeNotNull(toInt256("created_by_id")) as created_by_id,
    assumeNotNull(toInt256("last_modified_by_id")) as last_modified_by_id,
    assumeNotNull(toString("unite_code")) as unite_code,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toNullable(toString("producteur_type")) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    toNullable(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    toNullable(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    toNullable(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    toNullable(toString("producteur_adresse_code_postal")) as producteur_adresse_code_postal,
    toNullable(toString("producteur_adresse_pays")) as producteur_adresse_pays,
    assumeNotNull(toString("destinataire_type")) as destinataire_type,
    toNullable(toString("destinataire_numero_identification")) as destinataire_numero_identification,
    assumeNotNull(toString("destinataire_raison_sociale")) as destinataire_raison_sociale,
    toNullable(toString("destinataire_adresse_destination")) as destinataire_adresse_destination,
    assumeNotNull(toString("destinataire_adresse_libelle")) as destinataire_adresse_libelle,
    assumeNotNull(toString("destinataire_adresse_commune")) as destinataire_adresse_commune,
    assumeNotNull(toString("destinataire_adresse_code_postal")) as destinataire_adresse_code_postal,
    assumeNotNull(toString("destinataire_adresse_pays")) as destinataire_adresse_pays,
    toNullable(toString("etablissement_origine_adresse_prise_en_charge")) as etablissement_origine_adresse_prise_en_charge,
    assumeNotNull(toString("etablissement_origine_adresse_libelle")) as etablissement_origine_adresse_libelle,
    assumeNotNull(toString("etablissement_origine_adresse_commune")) as etablissement_origine_adresse_commune,
    assumeNotNull(toString("etablissement_origine_adresse_code_postal")) as etablissement_origine_adresse_code_postal,
    assumeNotNull(toString("etablissement_origine_adresse_pays")) as etablissement_origine_adresse_pays,
    toNullable(toString("eco_organisme_type")) as eco_organisme_type,
    toNullable(toString("eco_organisme_numero_identification")) as eco_organisme_numero_identification,
    toNullable(toString("eco_organisme_raison_sociale")) as eco_organisme_raison_sociale,
    toNullable(toString("courtier_type")) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse
 FROM pg_dwh_raw_zone_rndts.dd_sortant
);
 
-- sortie_statut_dechet
create table if not exists raw_zone_rndts.sortie_statut_dechet engine = MergeTree ORDER BY () as (
SELECT
    assumeNotNull(toInt256("sortie_statut_dechet_id")) as sortie_statut_dechet_id,
    assumeNotNull(toString("created_year_utc")) as created_year_utc,
    assumeNotNull(toString("public_id")) as public_id,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    assumeNotNull(toInt256("etablissement_id")) as etablissement_id,
    assumeNotNull(toInt256("created_by_id")) as created_by_id,
    assumeNotNull(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    assumeNotNull(toInt256("last_modified_by_id")) as last_modified_by_id,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toInt256("delegation_id")) as delegation_id,
    assumeNotNull(toString("denomination_usuelle")) as denomination_usuelle,
    assumeNotNull(toString("code_dechet")) as code_dechet,
    toNullable(toString("code_dechet_bale")) as code_dechet_bale,
    toNullable(toDate("date_utilisation")) as date_utilisation,
    toNullable(toDate("date_expedition")) as date_expedition,
    assumeNotNull(toString("nature")) as nature,
    assumeNotNull(toDecimal256("quantite", 9)) as quantite,
    assumeNotNull(toString("unite_code")) as unite_code,
    assumeNotNull(toDate("date_traitement")) as date_traitement,
    toNullable(toDate("date_fin_traitement")) as date_fin_traitement,
    assumeNotNull(toString("code_traitement")) as code_traitement,
    assumeNotNull(toString("qualification_code")) as qualification_code,
    toNullable(toString("reference_acte_administratif")) as reference_acte_administratif,
    assumeNotNull(toString("origine")) as origine,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    assumeNotNull(toString("destinataire_type")) as destinataire_type,
    toNullable(toString("destinataire_numero_identification")) as destinataire_numero_identification,
    assumeNotNull(toString("destinataire_raison_sociale")) as destinataire_raison_sociale,
    toNullable(toString("destinataire_adresse_destination")) as destinataire_adresse_destination,
    assumeNotNull(toString("destinataire_adresse_libelle")) as destinataire_adresse_libelle,
    assumeNotNull(toString("destinataire_adresse_commune")) as destinataire_adresse_commune,
    assumeNotNull(toString("destinataire_adresse_code_postal")) as destinataire_adresse_code_postal,
    assumeNotNull(toString("destinataire_adresse_pays")) as destinataire_adresse_pays
 FROM pg_dwh_raw_zone_rndts.sortie_statut_dechet
);

-- texs_entrant
create table if not exists raw_zone_rndts.texs_entrant engine = MergeTree ORDER BY () as (
SELECT
    assumeNotNull(toInt256("texs_entrant_id")) as texs_entrant_id,
    assumeNotNull(toString("created_year_utc")) as created_year_utc,
    toNullable(toString("code_dechet")) as code_dechet,
    assumeNotNull(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    assumeNotNull(toDate("date_reception")) as date_reception,
    assumeNotNull(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toString("identifiant_terrain_sis")) as identifiant_terrain_sis,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    assumeNotNull(toDecimal256("quantite", 9)) as quantite,
    toNullable(toBool("is_tex_pop")) as is_tex_pop,
    assumeNotNull(toString("code_traitement")) as code_traitement,
    assumeNotNull(toInt256("etablissement_id")) as etablissement_id,
    assumeNotNull(toInt256("created_by_id")) as created_by_id,
    assumeNotNull(toInt256("last_modified_by_id")) as last_modified_by_id,
    assumeNotNull(toString("unite_code")) as unite_code,
    toNullable(toString("numero_bordereau")) as numero_bordereau,
    assumeNotNull(toString("public_id")) as public_id,
    toNullable(toString("coordonnees_geographiques")) as coordonnees_geographiques,
    toNullable(toString("coordonnees_geographiques_valorisee")) as coordonnees_geographiques_valorisee,
    toNullable(toInt256("delegation_id")) as delegation_id,
    assumeNotNull(toString("origine")) as origine,
    toNullable(toString("code_dechet_bale")) as code_dechet_bale,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    assumeNotNull(toString("producteur_type")) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    assumeNotNull(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    assumeNotNull(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    assumeNotNull(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    assumeNotNull(toString("producteur_adresse_code_postal")) as producteur_adresse_code_postal,
    assumeNotNull(toString("producteur_adresse_pays")) as producteur_adresse_pays,
    assumeNotNull(toString("expediteur_type")) as expediteur_type,
    toNullable(toString("expediteur_numero_identification")) as expediteur_numero_identification,
    assumeNotNull(toString("expediteur_raison_sociale")) as expediteur_raison_sociale,
    toNullable(toString("expediteur_adresse_prise_en_charge")) as expediteur_adresse_prise_en_charge,
    assumeNotNull(toString("expediteur_adresse_libelle")) as expediteur_adresse_libelle,
    assumeNotNull(toString("expediteur_adresse_commune")) as expediteur_adresse_commune,
    assumeNotNull(toString("expediteur_adresse_code_postal")) as expediteur_adresse_code_postal,
    assumeNotNull(toString("expediteur_adresse_pays")) as expediteur_adresse_pays,
    toNullable(toString("courtier_type")) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse
 FROM pg_dwh_raw_zone_rndts.texs_entrant
);

-- texs_sortant
create table if not exists raw_zone_rndts.texs_sortant engine = MergeTree ORDER BY () as (
SELECT
    assumeNotNull(toInt256("texs_sortant_id")) as texs_sortant_id,
    assumeNotNull(toString("created_year_utc")) as created_year_utc,
    toNullable(toString("code_dechet")) as code_dechet,
    assumeNotNull(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    assumeNotNull(toDate("date_expedition")) as date_expedition,
    assumeNotNull(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toString("identifiant_terrain_sis")) as identifiant_terrain_sis,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    assumeNotNull(toDecimal256("quantite", 9)) as quantite,
    toNullable(toBool("is_tex_pop")) as is_tex_pop,
    assumeNotNull(toString("code_traitement")) as code_traitement,
    assumeNotNull(toInt256("etablissement_id")) as etablissement_id,
    assumeNotNull(toInt256("created_by_id")) as created_by_id,
    assumeNotNull(toInt256("last_modified_by_id")) as last_modified_by_id,
    assumeNotNull(toString("unite_code")) as unite_code,
    toNullable(toString("numero_bordereau")) as numero_bordereau,
    assumeNotNull(toString("public_id")) as public_id,
    toNullable(toString("coordonnees_geographiques")) as coordonnees_geographiques,
    toNullable(toString("coordonnees_geographiques_valorisee")) as coordonnees_geographiques_valorisee,
    assumeNotNull(toString("qualification_code")) as qualification_code,
    toNullable(toInt256("delegation_id")) as delegation_id,
    assumeNotNull(toString("origine")) as origine,
    toNullable(toString("code_dechet_bale")) as code_dechet_bale,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    assumeNotNull(toString("producteur_type")) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    assumeNotNull(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    assumeNotNull(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    assumeNotNull(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    assumeNotNull(toString("producteur_adresse_code_postal")) as producteur_adresse_code_postal,
    assumeNotNull(toString("producteur_adresse_pays")) as producteur_adresse_pays,
    assumeNotNull(toString("destinataire_type")) as destinataire_type,
    toNullable(toString("destinataire_numero_identification")) as destinataire_numero_identification,
    assumeNotNull(toString("destinataire_raison_sociale")) as destinataire_raison_sociale,
    toNullable(toString("destinataire_adresse_destination")) as destinataire_adresse_destination,
    assumeNotNull(toString("destinataire_adresse_libelle")) as destinataire_adresse_libelle,
    assumeNotNull(toString("destinataire_adresse_commune")) as destinataire_adresse_commune,
    assumeNotNull(toString("destinataire_adresse_code_postal")) as destinataire_adresse_code_postal,
    assumeNotNull(toString("destinataire_adresse_pays")) as destinataire_adresse_pays,
    toNullable(toString("courtier_type")) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse
 FROM pg_dwh_raw_zone_rndts.texs_sortant
);

-- dd_entrant_transporteur
create table if not exists raw_zone_rndts.dd_entrant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("dd_entrant_id")) as dd_entrant_id,
    assumeNotNull(toString("dd_entrant_created_year_utc")) as dd_entrant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    assumeNotNull(toString("transporteur_adresse_code_postal")) as transporteur_adresse_code_postal,
    assumeNotNull(toString("transporteur_adresse_pays")) as transporteur_adresse_pays
 FROM pg_dwh_raw_zone_rndts.dd_entrant_transporteur
);

-- dd_sortant_transporteur
create table if not exists raw_zone_rndts.dd_sortant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("dd_sortant_id")) as dd_sortant_id,
    assumeNotNull(toString("dd_sortant_created_year_utc")) as dd_sortant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    assumeNotNull(toString("transporteur_adresse_code_postal")) as transporteur_adresse_code_postal,
    assumeNotNull(toString("transporteur_adresse_pays")) as transporteur_adresse_pays
 FROM pg_dwh_raw_zone_rndts.dd_sortant_transporteur
);

-- dnd_sortant_transporteur
create table if not exists raw_zone_rndts.dnd_sortant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("dnd_sortant_id")) as dnd_sortant_id,
    assumeNotNull(toString("dnd_sortant_created_year_utc")) as dnd_sortant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    assumeNotNull(toString("transporteur_adresse_code_postal")) as transporteur_adresse_code_postal,
    assumeNotNull(toString("transporteur_adresse_pays")) as transporteur_adresse_pays
 FROM pg_dwh_raw_zone_rndts.dnd_sortant_transporteur
);

-- dnd_entrant_transporteur
create table if not exists raw_zone_rndts.dnd_entrant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("dnd_entrant_id")) as dnd_entrant_id,
    assumeNotNull(toString("dnd_entrant_created_year_utc")) as dnd_entrant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    assumeNotNull(toString("transporteur_adresse_code_postal")) as transporteur_adresse_code_postal,
    assumeNotNull(toString("transporteur_adresse_pays")) as transporteur_adresse_pays
 FROM pg_dwh_raw_zone_rndts.dnd_entrant_transporteur
);


-- texs_entrant_transporteur
create table if not exists raw_zone_rndts.texs_entrant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("texs_entrant_id")) as texs_entrant_id,
    assumeNotNull(toString("texs_entrant_created_year_utc")) as texs_entrant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    assumeNotNull(toString("transporteur_adresse_code_postal")) as transporteur_adresse_code_postal,
    assumeNotNull(toString("transporteur_adresse_pays")) as transporteur_adresse_pays
 FROM pg_dwh_raw_zone_rndts.texs_entrant_transporteur
);

-- texs_sortant_transporteur
create table if not exists raw_zone_rndts.texs_sortant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("texs_sortant_id")) as texs_sortant_id,
    assumeNotNull(toString("texs_sortant_created_year_utc")) as texs_sortant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    assumeNotNull(toString("transporteur_adresse_code_postal")) as transporteur_adresse_code_postal,
    assumeNotNull(toString("transporteur_adresse_pays")) as transporteur_adresse_pays
 FROM pg_dwh_raw_zone_rndts.texs_sortant_transporteur
);

-- etablissement
create table if not exists raw_zone_rndts.etablissement engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("etablissement_id")) as etablissement_id,
    toNullable(toString("numero_identification")) as numero_identification,
    assumeNotNull(toString("raison_sociale")) as raison_sociale,
    assumeNotNull(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("type_code")) as type_code,
    assumeNotNull(toString("timezone_code")) as timezone_code,
    assumeNotNull(toString("public_id")) as public_id,
    toNullable(toInt256("created_by_id")) as created_by_id,
    toNullable(toInt256("last_modified_by_id")) as last_modified_by_id,
    toNullable(toDateTime64("disabled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("disabled_date",'Europe/Paris'))) as disabled_date,
    toNullable(toString("disabled_reason_code")) as disabled_reason_code,
    toNullable(toInt256("etablissement_validateur_id")) as etablissement_validateur_id
 FROM pg_dwh_raw_zone_rndts.etablissement
);
 