GISTRID_NOTIFICATIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_gistrid.notifications (
	"numéro de notification" String,
	"type de dossier" Nullable(String),
	"état du dossier" Nullable(String),
	"département du dossier" Nullable(String),
	"nom du notifiant" Nullable(String),
	"numéro GISTRID du notifiant" Nullable(String),
	"commune du notifiant" Nullable(String),
	"pays du notifiant" Nullable(String),
	"nom du producteur" Nullable(String),
	"commune du producteur" Nullable(String),
	"pays du producteur" Nullable(String),
	"numéro SIRET de l'installation de traitement" Nullable(String),
	"nom de l'installation de traitement" Nullable(String),
	"numéro GISTRID de l'installation de traitement" Nullable(String),
	"commune de l'installation de traitement" Nullable(String),
	"pays de l'installation de traitement" Nullable(String),
	"date du consentement français" Nullable(String),
	"date autorisée du début des transferts" Nullable(String),
	"date autorisée de la fin des transferts" Nullable(String),
	"code D/R" Nullable(String),
	"code CB" Nullable(String),
	"code CED" Nullable(String),
	"code OCDE" Nullable(String),
	"code Y" Nullable(String),
	"code H" Nullable(String),
	"Caractéristiques physiques" Nullable(String),
	"quantité autorisée" Nullable(String),
	"unité" Nullable(String),
	"Nombre total de transferts autorisés" Nullable(String),
	"somme des quantités reçues" Nullable(String),
	"Nombre des transferts réceptionnés" Nullable(String),
	"annee" UInt16,
	"pays de transit" Nullable(String),
    inserted_at Datetime64 DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY ()
"""

GISTRID_NOTIFIANTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_gistrid.notifiants (
	"Type d'opérateur" Nullable(String),
	"Numéro GISTRID" String,
	"Nom de la société" Nullable(String),
	"N° d'enregistrement" Nullable(String),
	"Adresse" Nullable(String),
	"Pays" Nullable(String),
	"Code postal" Nullable(String),
	"Commune" Nullable(String),
	"Ville d'implantation de l'établissement" Nullable(String),
	"Pays de l'établissement" Nullable(String),
	"Installation de valorisation bénéficiant du consentement pré" Nullable(String),
	"Statut de l'opérateur" Nullable(String),
	"Date du statut" Nullable(String),
	"Département de l'établissement" Nullable(String),
	"SIRET" Nullable(String),
	"S3IC" Nullable(String),
	"Raison de refus" Nullable(String),
	"Nombre de notifications réservées" Nullable(String),
	"Indicateur d'activité" Nullable(String),
    inserted_at Datetime64 DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY ()
"""

GISTRID_INSTALLATIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_gistrid.installations (
    "Type d'opérateur" Nullable(String),
	"Numéro GISTRID" String,
	"Nom de la société" Nullable(String),
	"N° d'enregistrement" Nullable(String),
	"Adresse" Nullable(String),
	"Pays" Nullable(String),
	"Code postal" Nullable(String),
	"Commune" Nullable(String),
	"Ville d'implantation de l'établissement" Nullable(String),
	"Pays de l'établissement" Nullable(String),
	"Installation de valorisation bénéficiant du consentement pré" Nullable(String),
	"Statut de l'opérateur" Nullable(String),
	"Date du statut" Nullable(String),
	"Département de l'établissement" Nullable(String),
	"SIRET" Nullable(String),
	"S3IC" Nullable(String),
	"Raison de refus" Nullable(String),
	"Nombre de notifications réservées" Nullable(String),
	"Indicateur d'activité" Nullable(String),
    inserted_at Datetime64 DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY ()
"""
