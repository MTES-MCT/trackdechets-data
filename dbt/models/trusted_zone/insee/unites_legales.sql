SELECT
    siret_siege,
    statut_diffusion,
    nom_complet,
    nature_juridique,
    colter_code,
    colter_code_insee,
    colter_elus,
    colter_niveau,
    statut_entrepreneur_spectacle,
    liste_elus,
    liste_id_organisme_formation,
    liste_idcc,
    type_siae,
    length(siren::String) < 9,
    repeat('0', (9 - length(siren::String))) || siren::String AS siren,
    multiIf(
        etat_administratif = 'A', 'Active',
        etat_administratif = 'C', 'Cessée',
        etat_administratif
    )
        AS etat_administratif,
    toInt64(
        toFloat64(nombre_etablissements)
    )
        AS nombre_etablissements,
    toInt64(
        toFloat64(nombre_etablissements_ouverts)
    )
        AS nombre_etablissements_ouverts,
    parseDateTimeBestEffort(
        date_mise_a_jour_insee
    )
        AS date_mise_a_jour_insee,
    parseDateTimeBestEffort(
        date_mise_a_jour_rne
    )
        AS date_mise_a_jour_rne,
    multiIf(
        egapro_renseignee = 'True',
        true,
        egapro_renseignee = 'False',
        false,
        null
    )
        AS egapro_renseignee,
    multiIf(
        est_achats_responsables = 'True',
        true,
        est_achats_responsables = 'False',
        false,
        null
    )
        AS est_achats_responsables,
    multiIf(
        est_alim_confiance = 'True',
        true,
        est_alim_confiance = 'False',
        false,
        null
    )
        AS est_alim_confiance,
    multiIf(
        est_association = 'True', true, est_association = 'False', false, null
    )
        AS est_association,
    multiIf(
        est_entrepreneur_individuel = 'True',
        true,
        est_entrepreneur_individuel = 'False',
        false,
        null
    )
        AS est_entrepreneur_individuel,
    multiIf(
        est_entrepreneur_spectacle = 'True',
        true,
        est_entrepreneur_spectacle = 'False',
        false,
        null
    )
        AS est_entrepreneur_spectacle,
    multiIf(
        est_patrimoine_vivant = 'True',
        true,
        est_patrimoine_vivant = 'False',
        false,
        null
    )
        AS est_patrimoine_vivant,
    multiIf(
        est_ess = 'True', true, est_ess = 'False', false, null
    )                                                         AS est_ess,
    multiIf(
        est_organisme_formation = 'True',
        true,
        est_organisme_formation = 'False',
        false,
        null
    )
        AS est_organisme_formation,
    multiIf(
        est_qualiopi = 'True', true, est_qualiopi = 'False', false, null
    )                                                         AS est_qualiopi,
    multiIf(
        est_service_public = 'True',
        true,
        est_service_public = 'False',
        false,
        null
    )
        AS est_service_public,
    multiIf(
        est_societe_mission = 'True',
        true,
        est_societe_mission = 'False',
        false,
        null
    )
        AS est_societe_mission,
    multiIf(
        est_siae = 'True', true, est_siae = 'False', false, null
    )                                                         AS est_siae
FROM
    {{ source('raw_zone_insee', 'annuaire_entreprises_unite_legale') }}
