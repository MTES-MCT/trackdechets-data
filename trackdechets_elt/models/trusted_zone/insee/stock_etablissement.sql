SELECT
    siren,
    nic,
    siret,
    "statutDiffusionEtablissement"                   AS statut_diffusion_etablissement,
    cast(
        "dateCreationEtablissement" AS date
    )                                                AS date_creation_etablissement,
    "trancheEffectifsEtablissement"                  AS tranche_effectifs_etablissement,
    cast(
        "anneeEffectifsEtablissement" AS integer
    )                                                AS annee_effectifs_etablissement,
    "activitePrincipaleRegistreMetiersEtablissement" AS activite_principale_registre_metiers_etablissement,
    cast(
        "dateDernierTraitementEtablissement" AS timestamp
    )                                                AS date_dernier_traitement_etablissement,
    cast("etablissementSiege" AS bool)               AS etablissement_siege,
    cast(
        "nombrePeriodesEtablissement" AS int
    )                                                AS nombre_periodes_etablissement,
    "complementAdresseEtablissement"                 AS complement_adresse_etablissement,
    "numeroVoieEtablissement"                        AS numero_voie_etablissement,
    "indiceRepetitionEtablissement"                  AS indice_repetition_etablissement,
    "typeVoieEtablissement"                          AS type_voie_etablissement,
    "libelleVoieEtablissement"                       AS libelle_voie_etablissement,
    "codePostalEtablissement"                        AS code_postal_etablissement,
    "libelleCommuneEtablissement"                    AS libelle_commune_etablissement,
    "libelleCommuneEtrangerEtablissement"            AS libelle_commune_etranger_etablissement,
    "distributionSpecialeEtablissement"              AS distribution_speciale_etablissement,
    "codeCommuneEtablissement"                       AS code_commune_etablissement,
    "codeCedexEtablissement"                         AS code_cedex_etablissement,
    "libelleCedexEtablissement"                      AS libelle_cedex_etablissement,
    cast(
        "codePaysEtrangerEtablissement" AS integer
    )                                                AS code_pays_etranger_etablissement,
    "libellePaysEtrangerEtablissement"               AS libelle_pays_etranger_etablissement,
    "complementAdresse2Etablissement"                AS complement_adresse_2_etablissement,
    "numeroVoie2Etablissement"                       AS numero_voie_2_etablissement,
    "indiceRepetition2Etablissement"                 AS indice_repetition_2_etablissement,
    "typeVoie2Etablissement"                         AS type_voie_2_etablissement,
    "libelleVoie2Etablissement"                      AS libelle_voie_2_etablissement,
    "codePostal2Etablissement"                       AS code_postal_2_etablissement,
    "libelleCommune2Etablissement"                   AS libelle_commune_2_etablissement,
    "libelleCommuneEtranger2Etablissement"           AS libelle_commune_etranger_2_etablissement,
    "distributionSpeciale2Etablissement"             AS distribution_speciale_2_etablissement,
    "codeCommune2Etablissement"                      AS code_commune_2_etablissement,
    "codeCedex2Etablissement"                        AS code_cedex_2_etablissement,
    "libelleCedex2Etablissement"                     AS libelle_cedex_2_etablissement,
    cast(
        "codePaysEtranger2Etablissement" AS integer
    )                                                AS code_pays_etranger_2_etablissement,
    "libellePaysEtranger2Etablissement"              AS libelle_pays_etranger_2_etablissement,
    cast("dateDebut" AS date)                        AS date_debut,
    "etatAdministratifEtablissement"                 AS etat_administratif_etablissement,
    "enseigne1Etablissement"                         AS enseigne_1_etablissement,
    "enseigne2Etablissement"                         AS enseigne_2_etablissement,
    "enseigne3Etablissement"                         AS enseigne_3_etablissement,
    "denominationUsuelleEtablissement"               AS denomination_usuelle_etablissement,
    "activitePrincipaleEtablissement"                AS activite_principale_etablissement,
    "nomenclatureActivitePrincipaleEtablissement"    AS nomenclature_activite_principale_etablissement,
    "caractereEmployeurEtablissement"                AS caractere_employeur_etablissement
FROM
    {{ source('raw_zone_insee', 'stock_etablissement') }}
