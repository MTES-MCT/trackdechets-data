select
    toYear(created_at) as annee,
    _bs_type                                                 as type_bordereau,
    count(id)                                                as creations
from
    {{ ref('bordereaux_enriched') }}
where
    /* Uniquement déchets dangereux */
    (
        {{dangerous_waste_filter('bordereaux_enriched')}}
    )
    /* Pas de bouillons */
    and status != 'DRAFT'
    /* Uniquement les données jusqu'à la dernière semaine complète */
    and created_at < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
group by
    1,2
order by 1 desc, 2 asc
