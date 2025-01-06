SELECT
    siret,
    raison_sociale,
    nom_eco_organisme,
    filiere_dsrep,
    produits_relevant_filiere_responsabilite_elargie,
    adresse,
    code_postal,
    ville
FROM
    {{ source('raw_zone_gsheet', 'eco_organismes_agrees_2022') }}
