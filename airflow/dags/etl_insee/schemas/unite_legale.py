UNITE_LEGALE_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_insee.unite_legale_tmp
(
    siren                                           String,
    siret_siege                                     String,
    nom_complet                                     String,
    etat_administratif                              LowCardinality(String),
    statut_diffusion                                LowCardinality(String),
    nombre_etablissements                           Int32,
    nombre_etablissements_ouverts                   Int32,
    colter_code                                     Nullable(String),
    colter_code_insee                               Nullable(String),
    colter_elus                                     Nullable(String),
    colter_niveau                                   LowCardinality(Nullable(String)),
    date_mise_a_jour_insee                          Nullable(Date),
    date_mise_a_jour_rne                            Nullable(Date),
    egapro_renseignee                               Bool,
    est_association                                 Bool,
    est_entrepreneur_individuel                     Bool,
    est_entrepreneur_spectacle                      Bool,
    statut_entrepreneur_spectacle                   Nullable(String),
    est_ess                                         Bool,
    est_organisme_formation                         Bool,
    est_qualiopi                                    Bool,
    est_service_public                              Bool,
    est_societe_mission                             Bool,
    liste_elus                                      Nullable(String),
    liste_id_organisme_formation                   Nullable(String),
    liste_idcc                                      Nullable(String),
    est_siae                                        Bool,
    type_siae                                       Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (siren)
"""

