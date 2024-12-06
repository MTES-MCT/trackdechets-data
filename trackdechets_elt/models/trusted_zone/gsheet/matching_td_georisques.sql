select
    code_aiot,
    code_postal,
    nom_etablissement_icpe,
    siret_gerep,
    siret_icpe,
    siret_td,
    traite
from
    {{ source('raw_zone_gsheet', 'matching_td_georisques') }}
