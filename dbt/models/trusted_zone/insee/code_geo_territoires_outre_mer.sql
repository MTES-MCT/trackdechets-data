select
    COM_COMER     as CODE_ZONAGE_OUTRE_MER,
    TNCC          as TYPE_NOM_EN_CLAIR,
    NCC           as NOM_EN_CLAIR_MAJUSCULES,
    NCCENR        as NOM_EN_CLAIR_ENRICHI,
    LIBELLE,
    NATURE_ZONAGE as CODE_NATURE_ZONAGE,
    COMER         as CODE_COLLECTIVITE_OUTRE_MER,
    LIBELLE_COMER as LIBELLE_COLLECTIVITE_OUTRE_MER
from
    {{ source('raw_zone_insee', 'code_territoires_outre_mer') }}
