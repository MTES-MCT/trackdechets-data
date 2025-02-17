select
    TYPECOM   as TYPE_COMMUNE,
    COM       as CODE_COMMUNE,
    REG       as CODE_REGION,
    DEP       as CODE_DEPARTEMENT,
    CTCD      as CODE_COLLECTIVITE_TERRITORIALE,
    ARR       as CODE_ARRONDISSEMENT,
    CAN       as CODE_CANTON,
    TNCC      as TYPE_NOM_EN_CLAIR,
    NCC       as NOM_EN_CLAIR,
    NCCENR    as NOM_EN_CLAIR_ENRICHI,
    LIBELLE,
    COMPARENT as CODE_COMMUNE_PARENTE
from
    {{ source('raw_zone_insee', 'code_commune') }}
