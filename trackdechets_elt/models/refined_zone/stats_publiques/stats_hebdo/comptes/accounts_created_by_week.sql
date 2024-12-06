select
    ucbw.creations as comptes_utilisateurs,
    ccbw.creations as comptes_etablissements,
    coalesce(
        ucbw.semaine,
        ccbw.semaine
    )              as semaine
from
    {{ ref('users_created_by_week') }} as ucbw
full outer join {{ ref('companies_created_by_week') }} as ccbw
    on ucbw.semaine = ccbw.semaine
order by
    coalesce(
        ucbw.semaine,
        ccbw.semaine
    ) desc
