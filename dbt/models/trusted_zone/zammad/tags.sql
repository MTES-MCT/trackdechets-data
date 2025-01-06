SELECT
    ticket_id,
    tags
FROM
    {{ source("raw_zone_zammad", "tags") }}
