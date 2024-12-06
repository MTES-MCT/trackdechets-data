select
    "Numero Siret"              as siret,
    "Code établissement"        as code_etablissement,
    "Annee"                     as annee,
    "Nom Etablissement"         as nom_etablissement,
    "Adresse Site Exploitation" as adresse_site_exploitation,
    "Code Postal Etablissement" as code_postal_etablissement,
    "Commune"                   as commune,
    "Code Insee"                as code_commune_insee,
    "Code APE"                  as code_ape,
    "Nom Contact"               as nom_contact,
    "Fonction Contact"          as fonction_contact,
    "Tel Contact"               as tel_contact,
    "Mail Contact"              as email_contact,
    "Code déchet traité"        as code_dechet_traite,
    "Déchet traité"             as dechet_traite,
    "Quantité traitée (t/an)"   as quantite_traitee
from
    {{ source('raw_zone_gerep', 'gerep_traiteurs_2021') }}
