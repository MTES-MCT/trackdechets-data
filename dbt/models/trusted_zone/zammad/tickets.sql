SELECT
    id,
    "number",
    created_at,
    created_by_id,
    updated_at,
    updated_by_id,
    title,
    article_count,
    article_ids,
    close_at,
    create_article_sender_id,
    create_article_type_id,
    customer_id,
    escalation_at,
    first_response_at,
    first_response_diff_in_min,
    first_response_escalation_at,
    first_response_in_min,
    group_id,
    last_close_at,
    last_contact_agent_at,
    last_contact_at,
    last_contact_customer_at,
    last_owner_update_at,
    organization_id,
    owner_id,
    pending_time,
    priority_id,
    state_id,
    ticket_time_accounting_ids,
    time_unit,
    tags
FROM
    {{ source("raw_zone_zammad", "tickets") }}
