SELECT
    code_commune_insee,
    code_postal,
    nom_commune,
    ligne_5,
    "libellé_d_acheminement"                  AS libelle_acheminement,
    split_part(coordonnees_gps, ',', 1)::float AS latitude,
    split_part(coordonnees_gps, ',', 2)::float AS longitude
FROM
    {{ source('raw_zone', 'laposte_hexasmal') }}
