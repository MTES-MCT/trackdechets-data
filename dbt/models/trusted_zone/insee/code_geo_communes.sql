select
    TYPECOM   as "type_commune",
    COM       as "code_commune",
    REG       as "code_region",
    DEP       as "code_departement",
    CTCD      as "code_collectivite_territoriale",
    ARR       as "code_arrondissement",
    CAN       as "code_canton",
    TNCC      as "type_nom_en_clair",
    NCC       as "nom_en_clair",
    NCCENR    as "nom_en_clair_enrichi",
    LIBELLE as "libelle",
    COMPARENT as "code_commune_parente"
from
    {{ source('raw_zone_insee', 'code_commune') }}
