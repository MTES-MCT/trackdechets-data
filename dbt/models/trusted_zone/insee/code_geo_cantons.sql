select
    CAN        as CODE_CANTON,
    DEP        as CODE_DEPARTEMENT,
    REG        as CODE_REGION,
    COMPCT     as CODE_COMPOSITION_COMMUNALE,
    BURCENTRAL as CODE_COMMUNE_BUREU_CENTRAL,
    TNCC       as TYPE_NOM_EN_CLAIR,
    NCC        as NOM_EN_CLAIR,
    NCCENR     as NOM_EN_CLAIR_ENRICHI,
    LIBELLE,
    TYPECT     as TYPE_CANTON
from
    {{ source('raw_zone_insee', 'code_canton') }}
