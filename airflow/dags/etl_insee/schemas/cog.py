CODE_ARRONDISSEMENT_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_insee.code_arrondissement_tmp
(
    ARR                                             LowCardinality(String),
    DEP                                             LowCardinality(String),
    REG                                             LowCardinality(String),
    CHEFLIEU                                        LowCardinality(String),
    TNCC                                            LowCardinality(String),
    NCC                                             LowCardinality(String),
    NCCENR                                          LowCardinality(String),
    LIBELLE                                         LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY ()
"""

CODE_CANTON_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_insee.code_canton_tmp
(
    CAN                                             String,
    DEP                                             LowCardinality(String),
    REG                                             LowCardinality(String),
    COMPCT                                          LowCardinality(String),
    BURCENTRAL                                      Nullable(String),
    TNCC                                            Int,
    NCC                                             String,
    NCCENR                                          String,
    LIBELLE                                         String,
    TYPECT                                          LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY ()
"""

CODE_COMMUNE_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_insee.code_commune_tmp
(
    TYPECOM                                         LowCardinality(String),
    COM                                             String,
    REG                                             LowCardinality(Nullable(String)),
    DEP                                             LowCardinality(Nullable(String)),
    CTCD                                            LowCardinality(Nullable(String)),
    ARR                                             LowCardinality(Nullable(String)),
    TNCC                                            UInt8,
    NCC                                             String,
    NCCENR                                          String,
    LIBELLE                                         String,
    CAN                                             LowCardinality(Nullable(String)),
    COMPARENT                                       Nullable(String)
)
ENGINE = MergeTree()
ORDER BY ()
"""

CODE_DEPARTEMENT_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_insee.code_departement_tmp
(
    DEP                                         LowCardinality(String),
    REG                                         LowCardinality(String),
    CHEFLIEU                                    LowCardinality(String),
    TNCC                                        UInt8,
    NCC                                         LowCardinality(String),
    NCCENR                                      LowCardinality(String),
    LIBELLE                                     LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY ()
"""

CODE_REGION_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_insee.code_region_tmp
(
    REG                                         LowCardinality(String),
    CHEFLIEU                                    LowCardinality(String),
    TNCC                                        UInt8,
    NCC                                         LowCardinality(String),
    NCCENR                                      LowCardinality(String),
    LIBELLE                                     LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY ()
"""

CODE_TERRITOIRES_OUTRE_MER_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_insee.code_territoires_outre_mer_tmp
(
    COM_COMER                                         LowCardinality(String),
    TNCC                                              UInt8,
    NCC                                               LowCardinality(String),
    NCCENR                                            LowCardinality(String),
    LIBELLE                                           LowCardinality(String),
    NATURE_ZONAGE                                     LowCardinality(String),
    COMER                                             LowCardinality(String),
    LIBELLE_COMER                                     LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY ()
"""
