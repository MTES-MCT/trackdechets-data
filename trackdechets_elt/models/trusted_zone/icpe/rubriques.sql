select
    code_aiot,
    rubrique,
    alinea,
    quantite_totale::float,
    unite,
    nature,
    regime
from
    {{ source('raw_zone_icpe', 'rubriques') }}
