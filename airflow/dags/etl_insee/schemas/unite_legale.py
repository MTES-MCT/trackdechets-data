UNITE_LEGALE_DDL = """
CREATE TABLE IF NOT EXISTS {database}.{table} (
            siren  Int64,
            siret_siege  Nullable(String),
            etat_administratif  String,
            statut_diffusion  String,
            nombre_etablissements  Nullable(String),
            nombre_etablissements_ouverts  Nullable(String),
            nom_complet  String,
            nature_juridique  Nullable(String),
            colter_code  String,
            colter_code_insee  String,
            colter_elus  String,
            colter_niveau  String,
            date_mise_a_jour_insee  String,
            date_mise_a_jour_rne  String,
            egapro_renseignee  String,
            est_achats_responsables  String,
            est_alim_confiance  String,
            est_association  String,
            est_entrepreneur_individuel  String,
            est_entrepreneur_spectacle  String,
            est_patrimoine_vivant  String,
            statut_entrepreneur_spectacle  String,
            est_ess  String,
            est_organisme_formation  String,
            est_qualiopi  String,
            est_service_public  String,
            est_societe_mission  String,
            liste_elus  String,
            liste_id_organisme_formation  String,
            liste_idcc  String,
            est_siae  String,
            type_siae  String
    )
    ENGINE = MergeTree()
    ORDER BY `siren`;
"""
