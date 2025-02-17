create database raw_zone_gerep;


-- gerep_producteurs_2021
create table raw_zone_gerep.gerep_producteurs_2021 engine = MergeTree
ORDER BY
() as (
SELECT
    toNullable(toString("Annee")) as "Annee",
    toNullable(toString("Code établissement")) as "Code établissement",
    toNullable(toString("Nom Etablissement")) as "Nom Etablissement",
    toNullable(toString("Adresse Site Exploitation")) as "Adresse Site Exploitation",
    toLowCardinality(toNullable(toString("Code Postal Etablissement"))) as "Code Postal Etablissement",
    toNullable(toString("Commune")) as "Commune",
    toLowCardinality(toNullable(toString("Code Insee"))) as "Code Insee",
    toLowCardinality(toNullable(toString("Code APE"))) as "Code APE",
    toNullable(toString("Numero Siret")) as "Numero Siret",
    toNullable(toString("Nom Contact")) as "Nom Contact",
    toNullable(toString("Fonction Contact")) as "Fonction Contact",
    toNullable(toString("Tel Contact")) as "Tel Contact",
    toNullable(toString("Mail Contact")) as "Mail Contact",
    toLowCardinality(toNullable(toString("Code déchet produit"))) as "Code déchet produit",
    toNullable(toString("Déchet produit")) as "Déchet produit",
    toNullable(toString("Quntité produite (t/an)")) as "Quntité produite (t/an)"
 FROM pg_dwh_raw_zone_gerep.gerep_producteurs_2021
);


-- gerep_traiteurs_2021
create table raw_zone_gerep.gerep_traiteurs_2021 engine = MergeTree
ORDER BY
() as (
SELECT
    toNullable(toString("Annee")) as "Annee",
    toNullable(toString("Code établissement")) as "Code établissement",
    toNullable(toString("Nom Etablissement")) as "Nom Etablissement",
    toNullable(toString("Adresse Site Exploitation")) as "Adresse Site Exploitation",
    toLowCardinality(toNullable(toString("Code Postal Etablissement"))) as "Code Postal Etablissement",
    toNullable(toString("Commune")) as "Commune",
    toLowCardinality(toNullable(toString("Code Insee"))) as "Code Insee",
    toLowCardinality(toNullable(toString("Code APE"))) as "Code APE",
    toNullable(toString("Numero Siret")) as "Numero Siret",
    toNullable(toString("Nom Contact")) as "Nom Contact",
    toNullable(toString("Fonction Contact")) as "Fonction Contact",
    toNullable(toString("Tel Contact")) as "Tel Contact",
    toNullable(toString("Mail Contact")) as "Mail Contact",
    toLowCardinality(toNullable(toString("Code déchet traité"))) as "Code déchet traité",
    toNullable(toString("Déchet traité")) as "Déchet traité",
    toNullable(toString("Quantité traitée (t/an)")) as "Quantité traitée (t/an)"
 FROM pg_dwh_raw_zone_gerep.gerep_traiteurs_2021
);