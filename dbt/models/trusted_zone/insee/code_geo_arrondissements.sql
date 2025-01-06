select
    arr                   as "code_arrondissement",
    dep                   as "code_departement",
    cast(reg as integer)  as "code_region",
    cheflieu              as "code_commune_chef_lieu",
    cast(tncc as integer) as "type_nom_en_clair",
    ncc                   as "nom_en_clair",
    nccenr                as "nom_en_clair_enrichi",
    libelle
from
    {{ source('raw_zone_insee', 'arrondissement') }}
