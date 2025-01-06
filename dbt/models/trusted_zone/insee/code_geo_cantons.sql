select
    id_canton                  as "code_canton",
    id_departement             as "code_departement",
    cast(id_region as integer) as "code_region",
    cast(typct as integer)     as "type_canton",
    burcentral                 as "code_commune_bureau_centraliseur",
    cast(tncc as integer)      as "type_nom_en_clair",
    ncc                        as "nom_en_clair",
    nccenr                     as "nom_en_clair_enrichi",
    libelle,
    actual
from
    {{ source('raw_zone_insee', 'canton') }}
