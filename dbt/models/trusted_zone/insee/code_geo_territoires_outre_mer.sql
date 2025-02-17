select
    COM_COMER     as "code_zonage_outre_mer",
    TNCC          as "type_nom_en_clair",
    NCC           as "nom_en_clair",
    NCCENR        as "nom_en_clair_enrichi",
    LIBELLE as "libelle",
    NATURE_ZONAGE as "code_nature_zonage",
    COMER         as "code_collectivite_outre_mer",
    LIBELLE_COMER as "libelle_collectivite_outre_mer"
from
    {{ source('raw_zone_insee', 'code_territoires_outre_mer') }}
