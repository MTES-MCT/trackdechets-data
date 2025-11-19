SELECT
    id,
    name,
    name_last,
    created_at,
    created_by_id,
    updated_at,
    updated_by_id,
    active,
    email_address_id,
    follow_up_assignment,
    follow_up_possible,
    note,
    shared_drafts,
    signature_id,
    user_ids
FROM
    {{ source("raw_zone_zammad", "groups") }}
