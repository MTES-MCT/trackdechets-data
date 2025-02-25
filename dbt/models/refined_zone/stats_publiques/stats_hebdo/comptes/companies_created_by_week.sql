select
    toStartOfWeek( created_at,1,'Europe/Paris') as semaine,
    COUNT(id)                      as creations
from
    {{ ref('company') }}
where
    created_at < toStartOfWeek( now('Europe/Paris'),1,'Europe/Paris')
group by
    1
order by
    1 desc
