SELECT
    siren,
    nic,
    siret,
    statutdiffusionetablissement
        AS statut_diffusion_etablissement,
    datecreationetablissement
        AS date_creation_etablissement,
    trancheeffectifsetablissement
        AS tranche_effectifs_etablissement,
    anneeeffectifsetablissement
        AS annee_effectifs_etablissement,
    activiteprincipaleregistremetiersetablissement
        AS activite_principale_registre_metiers_etablissement,
    datederniertraitementetablissement
        AS date_dernier_traitement_etablissement,
    etablissementsiege                             AS etablissement_siege,
    nombreperiodesetablissement
        AS nombre_periodes_etablissement,
    complementadresseetablissement
        AS complement_adresse_etablissement,
    numerovoieetablissement
        AS numero_voie_etablissement,
    indicerepetitionetablissement
        AS indice_repetition_etablissement,
    typevoieetablissement                          AS type_voie_etablissement,
    libellevoieetablissement
        AS libelle_voie_etablissement,
    codepostaletablissement
        AS code_postal_etablissement,
    libellecommuneetablissement
        AS libelle_commune_etablissement,
    libellecommuneetrangeretablissement
        AS libelle_commune_etranger_etablissement,
    distributionspecialeetablissement
        AS distribution_speciale_etablissement,
    codecommuneetablissement
        AS code_commune_etablissement,
    codecedexetablissement
        AS code_cedex_etablissement,
    libellecedexetablissement
        AS libelle_cedex_etablissement,
    codepaysetrangeretablissement
        AS code_pays_etranger_etablissement,
    libellepaysetrangeretablissement
        AS libelle_pays_etranger_etablissement,
    complementadresse2etablissement
        AS complement_adresse_2_etablissement,
    numerovoie2etablissement
        AS numero_voie_2_etablissement,
    indicerepetition2etablissement
        AS indice_repetition_2_etablissement,
    typevoie2etablissement
        AS type_voie_2_etablissement,
    libellevoie2etablissement
        AS libelle_voie_2_etablissement,
    codepostal2etablissement
        AS code_postal_2_etablissement,
    libellecommune2etablissement
        AS libelle_commune_2_etablissement,
    libellecommuneetranger2etablissement
        AS libelle_commune_etranger_2_etablissement,
    distributionspeciale2etablissement
        AS distribution_speciale_2_etablissement,
    codecommune2etablissement
        AS code_commune_2_etablissement,
    codecedex2etablissement
        AS code_cedex_2_etablissement,
    libellecedex2etablissement
        AS libelle_cedex_2_etablissement,
    codepaysetranger2etablissement
        AS code_pays_etranger_2_etablissement,
    libellepaysetranger2etablissement
        AS libelle_pays_etranger_2_etablissement,
    datedebut                                      AS date_debut,
    etatadministratifetablissement
        AS etat_administratif_etablissement,
    enseigne1etablissement
        AS enseigne_1_etablissement,
    enseigne2etablissement
        AS enseigne_2_etablissement,
    enseigne3etablissement
        AS enseigne_3_etablissement,
    denominationusuelleetablissement
        AS denomination_usuelle_etablissement,
    activiteprincipaleetablissement
        AS activite_principale_etablissement,
    nomenclatureactiviteprincipaleetablissement
        AS nomenclature_activite_principale_etablissement,
    caractereemployeuretablissement
        AS caractere_employeur_etablissement
FROM
    {{ source('raw_zone_insee', 'stock_etablissement') }}
