select
    REG      as "code_region",
    CHEFLIEU as "code_commune_chef_lieu",
    TNCC     as "type_nom_en_clair",
    NCC      as "nom_en_clair",
    NCCENR   as "nom_en_clair_enrichi",
    LIBELLE as "libelle"
from
    {{ source('raw_zone_insee', 'code_region') }}
