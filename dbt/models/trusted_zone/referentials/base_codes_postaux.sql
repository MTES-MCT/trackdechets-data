SELECT
    code_commune_insee,
    code_postal::TEXT as code_postal,
    nom_commune,
    ligne_5,
    "libellé_d_acheminement"
        AS libelle_acheminement,
    toFloat64(
        arrayElementOrNull(
            splitByString(', ', assumeNotNull(coordonnees_gps)), 1
        )
    )                         AS latitude,
    toFloat64(
        arrayElementOrNull(
            splitByString(', ', assumeNotNull(coordonnees_gps)), 2
        )
    )                         AS longitude
FROM
    {{ source('raw_zone_referentials', 'laposte_hexasmal') }}
