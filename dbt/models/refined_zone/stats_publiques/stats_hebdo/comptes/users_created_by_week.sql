select
    toStartOfWeek(created_at, 1, 'Europe/Paris') as semaine,
    count(id)                                    as creations
from
    {{ ref('user') }}
where
    created_at < toStartOfWeek(now(), 1, 'Europe/Paris')
    and is_active
group by
    1
order by
    1 desc
