select
    is2.*,
    r.rubrique,
    r.alinea,
    r.quantite_totale,
    r.unite,
    r.nature,
    r.regime as regime_rubrique
from
    {{ ref('installations_siretise') }} as is2
inner join {{ ref('rubriques') }} as r on
    is2.code_aiot = r.code_aiot
