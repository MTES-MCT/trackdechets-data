with source as (
    select
        id,
        created_at,
        created_by_id,
        updated_at,
        updated_by_id,
        name,
        content                      as content_html,
        note,
        keywords,
        active,
        group_ids,
        extractTextFromHTML(content) as content_text
    from
        {{ source("raw_zone_zammad", "text_modules") }}
)

select
    id,
    created_at,
    created_by_id,
    updated_at,
    updated_by_id,
    name,
    content_html,
    content_text,
    note,
    keywords,
    active,
    group_ids
from source
