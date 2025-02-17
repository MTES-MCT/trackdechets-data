--- create database
CREATE database raw_zone_icpe;

--- installations
create table raw_zone_icpe.installations engine = MergeTree
ORDER BY
() as (
SELECT
    assumeNotNull(toString("code_aiot")) as code_aiot,
    toNullable(toString("num_siret")) as num_siret,
    toNullable(toString("x")) as x,
    toNullable(toString("y")) as y,
    toNullable(toString("adresse1")) as adresse1,
    toNullable(toString("adresse2")) as adresse2,
    toNullable(toString("adresse3")) as adresse3,
    toNullable(toString("code_postal")) as code_postal,
    toNullable(toString("code_insee")) as code_insee,
    toNullable(toString("commune")) as commune,
    toNullable(toString("raison_sociale")) as raison_sociale,
    toLowCardinality(toNullable(toString("etat_activite"))) as etat_activite,
    toLowCardinality(toNullable(toString("code_naf"))) as code_naf,
    toNullable(toString("seveso")) as seveso,
    toNullable(toString("regime")) as regime,
    toNullable(toString("priorite_nationale")) as priorite_nationale,
    toNullable(toString("ied")) as ied,
    toNullable(toString("type_service_aiot")) as type_service_aiot,
    toNullable(toString("bovins")) as bovins,
    toNullable(toString("porcs")) as porcs,
    toNullable(toString("volailles")) as volailles,
    toNullable(toString("carriere")) as carriere,
    toNullable(toString("eolienne")) as eolienne,
    toNullable(toString("industrie")) as industrie,
    toNullable(toString("longitude")) as longitude,
    toNullable(toString("latitude")) as latitude,
    toNullable(toString("date_modification")) as date_modification
 FROM pg_dwh_raw_zone_icpe.installations
);
