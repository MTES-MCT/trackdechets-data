UNITE_LEGALE_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_insee.unite_legale_tmp
(
    siren                                        String,
    statutDiffusionUniteLegale                   LowCardinality(Nullable(String)),
    unitePurgeeUniteLegale                       Nullable(UInt8),
    dateCreationUniteLegale                      Nullable(Int64),
    sigleUniteLegale                             Nullable(String),
    sexeUniteLegale                              LowCardinality(Nullable(String)),
    prenom1UniteLegale                           Nullable(String),
    prenom2UniteLegale                           Nullable(String),
    prenom3UniteLegale                           Nullable(String),
    prenom4UniteLegale                           Nullable(String),
    prenomUsuelUniteLegale                       Nullable(String),
    pseudonymeUniteLegale                        Nullable(String),
    identifiantAssociationUniteLegale            Nullable(String),
    trancheEffectifsUniteLegale                  LowCardinality(Nullable(String)),
    anneeEffectifsUniteLegale                    Nullable(Int16),
    dateDernierTraitementUniteLegale             Nullable(DateTime),
    nombrePeriodesUniteLegale                    Nullable(Int64),
    categorieEntreprise                          LowCardinality(Nullable(String)),
    anneeCategorieEntreprise                     Nullable(Int16),
    dateDebut                                    Nullable(Int64),
    etatAdministratifUniteLegale                 LowCardinality(Nullable(String)),
    nomUniteLegale                               Nullable(String),
    nomUsageUniteLegale                          Nullable(String),
    denominationUniteLegale                      Nullable(String),
    denominationUsuelle1UniteLegale              Nullable(String),
    denominationUsuelle2UniteLegale              Nullable(String),
    denominationUsuelle3UniteLegale              Nullable(String),
    categorieJuridiqueUniteLegale                Nullable(Int64),
    activitePrincipaleUniteLegale                LowCardinality(Nullable(String)),
    nomenclatureActivitePrincipaleUniteLegale    LowCardinality(Nullable(String)),
    nicSiegeUniteLegale                          Nullable(Int64),
    economieSocialeSolidaireUniteLegale          LowCardinality(Nullable(String)),
    societeMissionUniteLegale                    LowCardinality(Nullable(String)),
    caractereEmployeurUniteLegale                Nullable(String)

)
ENGINE = MergeTree()
ORDER BY (siren)
"""

