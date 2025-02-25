--- create database
CREATE database raw_zone_referentials;

--- codes_operations_traitements
create table raw_zone_referentials.codes_operations_traitements engine = MergeTree
ORDER BY
() as (
SELECT
    toLowCardinality(toNullable(toString("code"))) as code,
    toLowCardinality(toNullable(toString("description"))) as description
FROM pg_dwh_raw_zone.codes_operations_traitements
);

--- laposte_hexasmal
create table raw_zone_referentials.laposte_hexasmal engine = MergeTree
ORDER BY
() as (
SELECT
    toNullable(toString("code_commune_insee")) as code_commune_insee,
    toNullable(toString("nom_commune")) as nom_commune,
    toNullable(toString("code_postal")) as code_postal,
    toNullable(toString("ligne_5")) as ligne_5,
    toNullable(toString("libellé_d_acheminement")) as "libellé_d_acheminement",
    toNullable(toString("coordonnees_gps")) as coordonnees_gps
 FROM pg_dwh_raw_zone.laposte_hexasmal
);

