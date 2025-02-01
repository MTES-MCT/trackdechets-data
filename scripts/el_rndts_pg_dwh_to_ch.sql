--- dnd_entrant
create table trusted_zone_rndts.dnd_entrant engine = MergeTree ORDER BY () as (
SELECT
    toNullable(toInt256("id")) as id,
    toNullable(toString("created_year_utc")) as created_year_utc,
    toLowCardinality(toNullable(toString("code_dechet"))) as code_dechet,
    toNullable(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    toNullable(toDate("date_reception")) as date_reception,
    toNullable(toBool("is_dechet_pop")) as is_dechet_pop,
    toNullable(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toString("heure_pesee")) as heure_pesee,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    toNullable(toDecimal256("quantite", 9)) as quantite,
    toLowCardinality(toNullable(toString("code_traitement"))) as code_traitement,
    toNullable(toInt256("etablissement_id")) as etablissement_id,
    toNullable(toString("etablissement_numero_identification")) as etablissement_numero_identification,
    toNullable(toInt256("created_by_id")) as created_by_id,
    toNullable(toInt256("last_modified_by_id")) as last_modified_by_id,
    toLowCardinality(toNullable(toString("code_unite"))) as code_unite,
    toNullable(toString("public_id")) as public_id,
    toNullable(toInt256("delegation_id")) as delegation_id,
    toLowCardinality(toNullable(toString("origine"))) as origine,
    toLowCardinality(toNullable(toString("code_dechet_bale"))) as code_dechet_bale,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toLowCardinality(toNullable(toString("producteur_type"))) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    toNullable(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    toNullable(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    toNullable(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    toLowCardinality(toNullable(toString("producteur_adresse_code_postal"))) as producteur_adresse_code_postal,
    toLowCardinality(toNullable(toString("producteur_adresse_pays"))) as producteur_adresse_pays,
    toLowCardinality(toNullable(toString("expediteur_type"))) as expediteur_type,
    toNullable(toString("expediteur_numero_identification")) as expediteur_numero_identification,
    toNullable(toString("expediteur_raison_sociale")) as expediteur_raison_sociale,
    toNullable(toString("expediteur_adresse_prise_en_charge")) as expediteur_adresse_prise_en_charge,
    toNullable(toString("expediteur_adresse_libelle")) as expediteur_adresse_libelle,
    toNullable(toString("expediteur_adresse_commune")) as expediteur_adresse_commune,
    toLowCardinality(toNullable(toString("expediteur_adresse_code_postal"))) as expediteur_adresse_code_postal,
    toLowCardinality(toNullable(toString("expediteur_adresse_pays"))) as expediteur_adresse_pays,
    toLowCardinality(toNullable(toString("eco_organisme_type"))) as eco_organisme_type,
    toNullable(toString("eco_organisme_numero_identification")) as eco_organisme_numero_identification,
    toNullable(toString("eco_organisme_raison_sociale")) as eco_organisme_raison_sociale,
    toLowCardinality(toNullable(toString("courtier_type"))) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("numeros_indentification_transporteurs"),2,length("numeros_indentification_transporteurs")-2),''))) as numeros_indentification_transporteurs
 FROM old_dwh_rndts.dnd_entrant);

-- dnd sortant
create table trusted_zone_rndts.dnd_sortant engine = MergeTree ORDER BY () as (
SELECT
    toNullable(toInt256("id")) as id,
    toNullable(toString("created_year_utc")) as created_year_utc,
    toLowCardinality(toNullable(toString("code_dechet"))) as code_dechet,
    toNullable(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    toNullable(toDate("date_expedition")) as date_expedition,
    toNullable(toBool("is_dechet_pop")) as is_dechet_pop,
    toNullable(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    toNullable(toDecimal256("quantite", 9)) as quantite,
    toLowCardinality(toNullable(toString("code_traitement"))) as code_traitement,
    toNullable(toInt256("etablissement_id")) as etablissement_id,
    toNullable(toString("etablissement_numero_identification")) as etablissement_numero_identification,
    toNullable(toInt256("created_by_id")) as created_by_id,
    toNullable(toInt256("last_modified_by_id")) as last_modified_by_id,
    toLowCardinality(toNullable(toString("code_unite"))) as code_unite,
    toNullable(toString("public_id")) as public_id,
    toLowCardinality(toNullable(toString("qualification_code"))) as qualification_code,
    toNullable(toInt256("delegation_id")) as delegation_id,
    toLowCardinality(toNullable(toString("origine"))) as origine,
    toLowCardinality(toNullable(toString("code_dechet_bale"))) as code_dechet_bale,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toLowCardinality(toNullable(toString("producteur_type"))) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    toNullable(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    toNullable(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    toNullable(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    toLowCardinality(toNullable(toString("producteur_adresse_code_postal"))) as producteur_adresse_code_postal,
    toLowCardinality(toNullable(toString("producteur_adresse_pays"))) as producteur_adresse_pays,
    toLowCardinality(toNullable(toString("destinataire_type"))) as destinataire_type,
    toNullable(toString("destinataire_numero_identification")) as destinataire_numero_identification,
    toNullable(toString("destinataire_raison_sociale")) as destinataire_raison_sociale,
    toNullable(toString("destinataire_adresse_destination")) as destinataire_adresse_destination,
    toNullable(toString("destinataire_adresse_libelle")) as destinataire_adresse_libelle,
    toNullable(toString("destinataire_adresse_commune")) as destinataire_adresse_commune,
    toLowCardinality(toNullable(toString("destinataire_adresse_code_postal"))) as destinataire_adresse_code_postal,
    toLowCardinality(toNullable(toString("destinataire_adresse_pays"))) as destinataire_adresse_pays,
    toNullable(toString("etablissement_origine_adresse_prise_en_charge")) as etablissement_origine_adresse_prise_en_charge,
    toNullable(toString("etablissement_origine_adresse_libelle")) as etablissement_origine_adresse_libelle,
    toNullable(toString("etablissement_origine_adresse_commune")) as etablissement_origine_adresse_commune,
    toLowCardinality(toNullable(toString("etablissement_origine_adresse_code_postal"))) as etablissement_origine_adresse_code_postal,
    toLowCardinality(toNullable(toString("etablissement_origine_adresse_pays"))) as etablissement_origine_adresse_pays,
    toLowCardinality(toNullable(toString("eco_organisme_type"))) as eco_organisme_type,
    toNullable(toString("eco_organisme_numero_identification")) as eco_organisme_numero_identification,
    toNullable(toString("eco_organisme_raison_sociale")) as eco_organisme_raison_sociale,
    toLowCardinality(toNullable(toString("courtier_type"))) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("numeros_indentification_transporteurs"),2,length("numeros_indentification_transporteurs")-2),''))) as numeros_indentification_transporteurs
 FROM old_dwh_rndts.dnd_sortant
);

-- dd_entrant
create table trusted_zone_rndts.dd_entrant engine = MergeTree ORDER BY () as (
SELECT
    toNullable(toInt256("id")) as id,
    toNullable(toString("created_year_utc")) as created_year_utc,
    toNullable(toString("public_id")) as public_id,
    toLowCardinality(toNullable(toString("code_dechet"))) as code_dechet,
    toLowCardinality(toNullable(toString("code_dechet_bale"))) as code_dechet_bale,
    toNullable(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    toNullable(toDate("date_reception")) as date_reception,
    toNullable(toInt256("delegation_id")) as delegation_id,
    toNullable(toBool("is_dechet_pop")) as is_dechet_pop,
    toNullable(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toString("heure_pesee")) as heure_pesee,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    toLowCardinality(toNullable(toString("origine"))) as origine,
    toNullable(toDecimal256("quantite", 9)) as quantite,
    toLowCardinality(toNullable(toString("code_traitement"))) as code_traitement,
    toNullable(toInt256("etablissement_id")) as etablissement_id,
    toNullable(toString("etablissement_numero_identification")) as etablissement_numero_identification,
    toNullable(toInt256("created_by_id")) as created_by_id,
    toNullable(toInt256("last_modified_by_id")) as last_modified_by_id,
    toLowCardinality(toNullable(toString("code_unite"))) as code_unite,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toLowCardinality(toNullable(toString("producteur_type"))) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    toNullable(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    toNullable(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    toNullable(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    toLowCardinality(toNullable(toString("producteur_adresse_code_postal"))) as producteur_adresse_code_postal,
    toLowCardinality(toNullable(toString("producteur_adresse_pays"))) as producteur_adresse_pays,
    toLowCardinality(toNullable(toString("expediteur_type"))) as expediteur_type,
    toNullable(toString("expediteur_numero_identification")) as expediteur_numero_identification,
    toNullable(toString("expediteur_raison_sociale")) as expediteur_raison_sociale,
    toNullable(toString("expediteur_adresse_prise_en_charge")) as expediteur_adresse_prise_en_charge,
    toNullable(toString("expediteur_adresse_libelle")) as expediteur_adresse_libelle,
    toNullable(toString("expediteur_adresse_commune")) as expediteur_adresse_commune,
    toLowCardinality(toNullable(toString("expediteur_adresse_code_postal"))) as expediteur_adresse_code_postal,
    toLowCardinality(toNullable(toString("expediteur_adresse_pays"))) as expediteur_adresse_pays,
    toLowCardinality(toNullable(toString("eco_organisme_type"))) as eco_organisme_type,
    toNullable(toString("eco_organisme_numero_identification")) as eco_organisme_numero_identification,
    toNullable(toString("eco_organisme_raison_sociale")) as eco_organisme_raison_sociale,
    toLowCardinality(toNullable(toString("courtier_type"))) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("numeros_indentification_transporteurs"),2,length("numeros_indentification_transporteurs")-2),''))) as numeros_indentification_transporteurs
    from old_dwh_rndts.dd_entrant
)

-- dd_sortant
create table trusted_zone_rndts.dd_sortant engine = MergeTree ORDER BY () as (
SELECT
    toNullable(toInt256("id")) as id,
    toNullable(toString("created_year_utc")) as created_year_utc,
    toNullable(toString("public_id")) as public_id,
    toLowCardinality(toNullable(toString("code_dechet"))) as code_dechet,
    toLowCardinality(toNullable(toString("code_dechet_bale"))) as code_dechet_bale,
    toNullable(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    toNullable(toDate("date_expedition")) as date_expedition,
    toNullable(toInt256("delegation_id")) as delegation_id,
    toNullable(toBool("is_dechet_pop")) as is_dechet_pop,
    toNullable(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    toLowCardinality(toNullable(toString("origine"))) as origine,
    toNullable(toDecimal256("quantite", 9)) as quantite,
    toLowCardinality(toNullable(toString("qualification_code"))) as qualification_code,
    toLowCardinality(toNullable(toString("code_traitement"))) as code_traitement,
    toNullable(toInt256("etablissement_origine_id")) as etablissement_origine_id,
    toNullable(toInt256("etablissement_id")) as etablissement_id,
    toNullable(toString("etablissement_numero_identification")) as etablissement_numero_identification,
    toNullable(toInt256("created_by_id")) as created_by_id,
    toNullable(toInt256("last_modified_by_id")) as last_modified_by_id,
    toLowCardinality(toNullable(toString("code_unite"))) as code_unite,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toLowCardinality(toNullable(toString("producteur_type"))) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    toNullable(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    toNullable(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    toNullable(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    toLowCardinality(toNullable(toString("producteur_adresse_code_postal"))) as producteur_adresse_code_postal,
    toLowCardinality(toNullable(toString("producteur_adresse_pays"))) as producteur_adresse_pays,
    toLowCardinality(toNullable(toString("destinataire_type"))) as destinataire_type,
    toNullable(toString("destinataire_numero_identification")) as destinataire_numero_identification,
    toNullable(toString("destinataire_raison_sociale")) as destinataire_raison_sociale,
    toNullable(toString("destinataire_adresse_destination")) as destinataire_adresse_destination,
    toNullable(toString("destinataire_adresse_libelle")) as destinataire_adresse_libelle,
    toNullable(toString("destinataire_adresse_commune")) as destinataire_adresse_commune,
    toLowCardinality(toNullable(toString("destinataire_adresse_code_postal"))) as destinataire_adresse_code_postal,
    toLowCardinality(toNullable(toString("destinataire_adresse_pays"))) as destinataire_adresse_pays,
    toNullable(toString("etablissement_origine_adresse_prise_en_charge")) as etablissement_origine_adresse_prise_en_charge,
    toNullable(toString("etablissement_origine_adresse_libelle")) as etablissement_origine_adresse_libelle,
    toNullable(toString("etablissement_origine_adresse_commune")) as etablissement_origine_adresse_commune,
    toLowCardinality(toNullable(toString("etablissement_origine_adresse_code_postal"))) as etablissement_origine_adresse_code_postal,
    toLowCardinality(toNullable(toString("etablissement_origine_adresse_pays"))) as etablissement_origine_adresse_pays,
    toLowCardinality(toNullable(toString("eco_organisme_type"))) as eco_organisme_type,
    toNullable(toString("eco_organisme_numero_identification")) as eco_organisme_numero_identification,
    toNullable(toString("eco_organisme_raison_sociale")) as eco_organisme_raison_sociale,
    toLowCardinality(toNullable(toString("courtier_type"))) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("numeros_indentification_transporteurs"),2,length("numeros_indentification_transporteurs")-2),''))) as numeros_indentification_transporteurs
 FROM old_dwh_rndts.dd_sortant
)
 
-- sortie_statut_dechet
create table trusted_zone_rndts.sortie_statut_dechet engine = MergeTree ORDER BY () as (
SELECT
    toNullable(toInt256("id")) as id,
    toNullable(toString("created_year_utc")) as created_year_utc,
    toNullable(toString("public_id")) as public_id,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("etablissement_id")) as etablissement_id,
    toNullable(toString("etablissement_numero_identification")) as etablissement_numero_identification,
    toNullable(toInt256("created_by_id")) as created_by_id,
    toNullable(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    toNullable(toInt256("last_modified_by_id")) as last_modified_by_id,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toInt256("delegation_id")) as delegation_id,
    toNullable(toString("denomination_usuelle")) as denomination_usuelle,
    toLowCardinality(toNullable(toString("code_dechet"))) as code_dechet,
    toLowCardinality(toNullable(toString("code_dechet_bale"))) as code_dechet_bale,
    toNullable(toDate("date_utilisation")) as date_utilisation,
    toNullable(toDate("date_expedition")) as date_expedition,
    toNullable(toString("nature")) as nature,
    toNullable(toDecimal256("quantite", 9)) as quantite,
    toLowCardinality(toNullable(toString("code_unite"))) as code_unite,
    toNullable(toDate("date_traitement")) as date_traitement,
    toNullable(toDate("date_fin_traitement")) as date_fin_traitement,
    toLowCardinality(toNullable(toString("code_traitement"))) as code_traitement,
    toLowCardinality(toNullable(toString("qualification_code"))) as qualification_code,
    toNullable(toString("reference_acte_administratif")) as reference_acte_administratif,
    toLowCardinality(toNullable(toString("origine"))) as origine,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toLowCardinality(toNullable(toString("destinataire_type"))) as destinataire_type,
    toNullable(toString("destinataire_numero_identification")) as destinataire_numero_identification,
    toNullable(toString("destinataire_raison_sociale")) as destinataire_raison_sociale,
    toNullable(toString("destinataire_adresse_destination")) as destinataire_adresse_destination,
    toNullable(toString("destinataire_adresse_libelle")) as destinataire_adresse_libelle,
    toNullable(toString("destinataire_adresse_commune")) as destinataire_adresse_commune,
    toLowCardinality(toNullable(toString("destinataire_adresse_code_postal"))) as destinataire_adresse_code_postal,
    toLowCardinality(toNullable(toString("destinataire_adresse_pays"))) as destinataire_adresse_pays
 FROM old_dwh_rndts.sortie_statut_dechet
)

-- texs_entrant
create table trusted_zone_rndts.texs_entrant engine = MergeTree ORDER BY () as (
SELECT
    toNullable(toInt256("id")) as id,
    toNullable(toString("created_year_utc")) as created_year_utc,
    toLowCardinality(toNullable(toString("code_dechet"))) as code_dechet,
    toNullable(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    toNullable(toDate("date_reception")) as date_reception,
    toNullable(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toString("identifiant_terrain_sis")) as identifiant_terrain_sis,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    toNullable(toDecimal256("quantite", 9)) as quantite,
    toNullable(toBool("is_tex_pop")) as is_tex_pop,
    toLowCardinality(toNullable(toString("code_traitement"))) as code_traitement,
    toNullable(toInt256("etablissement_id")) as etablissement_id,
    toNullable(toString("etablissement_numero_identification")) as etablissement_numero_identification,
    toNullable(toInt256("created_by_id")) as created_by_id,
    toNullable(toInt256("last_modified_by_id")) as last_modified_by_id,
    toLowCardinality(toNullable(toString("code_unite"))) as code_unite,
    toNullable(toString("numero_bordereau")) as numero_bordereau,
    toNullable(toString("public_id")) as public_id,
    toNullable(toString("coordonnees_geographiques")) as coordonnees_geographiques,
    toNullable(toString("coordonnees_geographiques_valorisee")) as coordonnees_geographiques_valorisee,
    toNullable(toInt256("delegation_id")) as delegation_id,
    toLowCardinality(toNullable(toString("origine"))) as origine,
    toLowCardinality(toNullable(toString("code_dechet_bale"))) as code_dechet_bale,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toLowCardinality(toNullable(toString("producteur_type"))) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    toNullable(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    toNullable(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    toNullable(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    toLowCardinality(toNullable(toString("producteur_adresse_code_postal"))) as producteur_adresse_code_postal,
    toLowCardinality(toNullable(toString("producteur_adresse_pays"))) as producteur_adresse_pays,
    toLowCardinality(toNullable(toString("expediteur_type"))) as expediteur_type,
    toNullable(toString("expediteur_numero_identification")) as expediteur_numero_identification,
    toNullable(toString("expediteur_raison_sociale")) as expediteur_raison_sociale,
    toNullable(toString("expediteur_adresse_prise_en_charge")) as expediteur_adresse_prise_en_charge,
    toNullable(toString("expediteur_adresse_libelle")) as expediteur_adresse_libelle,
    toNullable(toString("expediteur_adresse_commune")) as expediteur_adresse_commune,
    toLowCardinality(toNullable(toString("expediteur_adresse_code_postal"))) as expediteur_adresse_code_postal,
    toLowCardinality(toNullable(toString("expediteur_adresse_pays"))) as expediteur_adresse_pays,
    toLowCardinality(toNullable(toString("courtier_type"))) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("numeros_indentification_transporteurs"),2,length("numeros_indentification_transporteurs")-2),''))) as numeros_indentification_transporteurs
 FROM old_dwh_rndts.texs_entrant
)

-- texs_sortant
create table trusted_zone_rndts.texs_sortant engine = MergeTree ORDER BY () as (
SELECT
    toNullable(toInt256("id")) as id,
    toNullable(toString("created_year_utc")) as created_year_utc,
    toLowCardinality(toNullable(toString("code_dechet"))) as code_dechet,
    toNullable(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    toNullable(toDate("date_expedition")) as date_expedition,
    toNullable(toString("denomination_usuelle")) as denomination_usuelle,
    toNullable(toString("identifiant_terrain_sis")) as identifiant_terrain_sis,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toNullable(toString("numero_document")) as numero_document,
    toNullable(toString("numero_notification")) as numero_notification,
    toNullable(toString("numero_saisie")) as numero_saisie,
    toNullable(toDecimal256("quantite", 9)) as quantite,
    toNullable(toBool("is_tex_pop")) as is_tex_pop,
    toLowCardinality(toNullable(toString("code_traitement"))) as code_traitement,
    toNullable(toInt256("etablissement_id")) as etablissement_id,
    toNullable(toString("etablissement_numero_identification")) as etablissement_numero_identification,
    toNullable(toInt256("created_by_id")) as created_by_id,
    toNullable(toInt256("last_modified_by_id")) as last_modified_by_id,
    toLowCardinality(toNullable(toString("code_unite"))) as code_unite,
    toNullable(toString("numero_bordereau")) as numero_bordereau,
    toNullable(toString("public_id")) as public_id,
    toNullable(toString("coordonnees_geographiques")) as coordonnees_geographiques,
    toNullable(toString("coordonnees_geographiques_valorisee")) as coordonnees_geographiques_valorisee,
    toLowCardinality(toNullable(toString("qualification_code"))) as qualification_code,
    toNullable(toInt256("delegation_id")) as delegation_id,
    toLowCardinality(toNullable(toString("origine"))) as origine,
    toLowCardinality(toNullable(toString("code_dechet_bale"))) as code_dechet_bale,
    toNullable(toString("identifiant_metier")) as identifiant_metier,
    toNullable(toInt256("canceled_by_id")) as canceled_by_id,
    toNullable(toString("canceled_comment")) as canceled_comment,
    toNullable(toDateTime64("canceled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("canceled_date",'Europe/Paris'))) as canceled_date,
    toNullable(toInt256("import_id")) as import_id,
    toLowCardinality(toNullable(toString("producteur_type"))) as producteur_type,
    toNullable(toString("producteur_numero_identification")) as producteur_numero_identification,
    toNullable(toString("producteur_raison_sociale")) as producteur_raison_sociale,
    toNullable(toString("producteur_adresse_libelle")) as producteur_adresse_libelle,
    toNullable(toString("producteur_adresse_commune")) as producteur_adresse_commune,
    toLowCardinality(toNullable(toString("producteur_adresse_code_postal"))) as producteur_adresse_code_postal,
    toLowCardinality(toNullable(toString("producteur_adresse_pays"))) as producteur_adresse_pays,
    toLowCardinality(toNullable(toString("destinataire_type"))) as destinataire_type,
    toNullable(toString("destinataire_numero_identification")) as destinataire_numero_identification,
    toNullable(toString("destinataire_raison_sociale")) as destinataire_raison_sociale,
    toNullable(toString("destinataire_adresse_destination")) as destinataire_adresse_destination,
    toNullable(toString("destinataire_adresse_libelle")) as destinataire_adresse_libelle,
    toNullable(toString("destinataire_adresse_commune")) as destinataire_adresse_commune,
    toLowCardinality(toNullable(toString("destinataire_adresse_code_postal"))) as destinataire_adresse_code_postal,
    toLowCardinality(toNullable(toString("destinataire_adresse_pays"))) as destinataire_adresse_pays,
    toLowCardinality(toNullable(toString("courtier_type"))) as courtier_type,
    toNullable(toString("courtier_numero_identification")) as courtier_numero_identification,
    toNullable(toString("courtier_raison_sociale")) as courtier_raison_sociale,
    toNullable(toString("courtier_numero_recepisse")) as courtier_numero_recepisse,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("numeros_indentification_transporteurs"),2,length("numeros_indentification_transporteurs")-2),''))) as numeros_indentification_transporteurs
 FROM old_dwh_rndts.texs_sortant
)

-- dd_entrant_transporteur
create table trusted_zone_rndts.dd_entrant_transporteur engine = MergeTree
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
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_code_postal"))) as transporteur_adresse_code_postal,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_pays"))) as transporteur_adresse_pays
 FROM
	old_dwh_rndts.dd_entrant_transporteur
)

-- dd_sortant_transporteur
create table trusted_zone_rndts.dd_sortant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("dd_sortant_id")) as dd_entrant_id,
    assumeNotNull(toString("dd_sortant_created_year_utc")) as dd_entrant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_code_postal"))) as transporteur_adresse_code_postal,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_pays"))) as transporteur_adresse_pays
 FROM
	old_dwh_rndts.dd_sortant_transporteur
)

-- dnd_sortant_transporteur
create table trusted_zone_rndts.dnd_sortant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("dnd_sortant_id")) as dd_entrant_id,
    assumeNotNull(toString("dnd_sortant_created_year_utc")) as dd_entrant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_code_postal"))) as transporteur_adresse_code_postal,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_pays"))) as transporteur_adresse_pays
 FROM
	old_dwh_rndts.dnd_sortant_transporteur
)

-- dnd_entrant_transporteur
create table trusted_zone_rndts.dnd_entrant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("dnd_entrant_id")) as dd_entrant_id,
    assumeNotNull(toString("dnd_entrant_created_year_utc")) as dd_entrant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_code_postal"))) as transporteur_adresse_code_postal,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_pays"))) as transporteur_adresse_pays
 FROM
	old_dwh_rndts.dnd_entrant_transporteur
)


-- texs_entrant_transporteur
create table trusted_zone_rndts.texs_entrant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("texs_entrant_id")) as dd_entrant_id,
    assumeNotNull(toString("texs_entrant_created_year_utc")) as dd_entrant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_code_postal"))) as transporteur_adresse_code_postal,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_pays"))) as transporteur_adresse_pays
 FROM
	old_dwh_rndts.texs_entrant_transporteur
)

-- texs_sortant_transporteur
create table trusted_zone_rndts.texs_sortant_transporteur engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("texs_sortant_id")) as dd_entrant_id,
    assumeNotNull(toString("texs_sortant_created_year_utc")) as dd_entrant_created_year_utc,
    assumeNotNull(toString("transporteur_type")) as transporteur_type,
    toNullable(toString("transporteur_numero_identification")) as transporteur_numero_identification,
    assumeNotNull(toString("transporteur_raison_sociale")) as transporteur_raison_sociale,
    toNullable(toString("transporteur_numero_recepisse")) as transporteur_numero_recepisse,
    assumeNotNull(toString("transporteur_adresse_libelle")) as transporteur_adresse_libelle,
    assumeNotNull(toString("transporteur_adresse_commune")) as transporteur_adresse_commune,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_code_postal"))) as transporteur_adresse_code_postal,
    toLowCardinality(assumeNotNull(toString("transporteur_adresse_pays"))) as transporteur_adresse_pays
 FROM
	old_dwh_rndts.texs_sortant_transporteur
)

-- etablissement
create table trusted_zone_rndts.etablissement engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toInt256("id")) as id,
    toNullable(toString("numero_identification")) as numero_identification,
    assumeNotNull(toString("raison_sociale")) as raison_sociale,
    assumeNotNull(toDateTime64("created_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("created_date",'Europe/Paris'))) as created_date,
    toNullable(toDateTime64("last_modified_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("last_modified_date",'Europe/Paris'))) as last_modified_date,
    toLowCardinality(toNullable(toString("type_code"))) as type_code,
    toLowCardinality(assumeNotNull(toString("timezone_code"))) as timezone_code,
    assumeNotNull(toString("public_id")) as public_id,
    toNullable(toInt256("created_by_id")) as created_by_id,
    toNullable(toInt256("last_modified_by_id")) as last_modified_by_id,
    toNullable(toDateTime64("disabled_date", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("disabled_date",'Europe/Paris'))) as disabled_date,
    toLowCardinality(toNullable(toString("disabled_reason_code"))) as disabled_reason_code,
    toNullable(toInt256("etablissement_validateur_id")) as etablissement_validateur_id
 FROM old_dwh_rndts.etablissement
)
 