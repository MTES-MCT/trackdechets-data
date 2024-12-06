select
    cog                       as "code_pays",
    cast(actual as integer)   as "coe_actualité",
    capay                     as "code_ancien_pays_rattachement",
    crpay                     as "code_actuel_pays_rattachement",
    cast(ani as integer)      as "annee_independance",
    libcog                    as "libelle",
    libenr                    as "nom_officiel",
    ancnom                    as "ancien_nom",
    codeiso2                  as "code_iso_2",
    codeiso3                  as "code_iso_3",
    cast(codenum3 as integer) as "code_iso_num"
from
    {{ source('raw_zone_insee', 'pays') }}
