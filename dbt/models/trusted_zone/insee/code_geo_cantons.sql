select
    CAN        as "code_canton",
    DEP        as "code_departement",
    REG        as "code_region",
    COMPCT     as "code_composition_communale",
    BURCENTRAL as "code_commune_bureau_central",
    TNCC       as "type_nom_en_clair",
    NCC        as "nom_en_clair",
    NCCENR     as "nom_en_clair_enrichi",
    LIBELLE as "libelle",
    TYPECT     as "type_canton"
from
    {{ source('raw_zone_insee', 'code_canton') }}
