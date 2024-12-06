select
    DATE_TRUNC('week', created_at) as semaine,
    COUNT(id)                      as creations
from
    {{ ref('company') }}
where
    created_at < DATE_TRUNC('week', NOW())
group by
    DATE_TRUNC('week', created_at)
order by
    DATE_TRUNC('week', created_at) desc
