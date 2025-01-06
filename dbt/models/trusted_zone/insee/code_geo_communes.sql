select
    typecom               as "type_commune",
    cast(reg as integer)  as "code_region",
    dep                   as "code_departement",
    com                   as "code_commune",
    can                   as "code_canton",
    arr                   as "code_arrondissement",
    ctcd                  as "code_ctcd",
    cast(tncc as integer) as "type_nom_en_clair",
    ncc                   as "nom_en_clair",
    nccenr                as "nom_en_clair_enrichi",
    libelle,
    comparent             as "code_commune_parente"
from
    {{ source('raw_zone_insee', 'commune') }}
