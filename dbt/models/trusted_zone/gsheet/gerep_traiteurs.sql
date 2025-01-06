select
    annee,
    code_etablissement as code_s3ic,
    numero_siret,
    nom_etablissement,
    code_dechet_traite,
    dechet_traite,
    adresse_site_exploitation,
    code_postal_etablissement,
    commune,
    code_insee         as code_commune_insee,
    code_ape,
    nom_contact,
    tel_contact,
    fonction_contact,
    mail_contact
from
    {{ source('raw_zone_gsheet', 'gerep_traiteurs') }}
