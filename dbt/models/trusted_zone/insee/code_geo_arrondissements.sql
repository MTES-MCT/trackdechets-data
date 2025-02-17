select
    ARR      as CODE_ARRONDISSEMENT,
    DEP      as CODE_DEPARTEMENT,
    REG      as CODE_REGION,
    CHEFLIEU as CODE_COMMUNE_CHEF_LIEU,
    TNCC     as TYPE_NOM_EN_CLAIR,
    NCC      as NOM_EN_CLAIR,
    NCCENR   as NOM_EN_CLAIR_ENRICHI,
    LIBELLE
from
    {{ source('raw_zone_insee', 'code_arrondissement') }}
