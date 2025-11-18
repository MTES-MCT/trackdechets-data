WITH __tickets_new AS (
    SELECT
        id,
        "number",
        assumeNotNull(toDateTime64(created_at, 6, 'Europe/Paris')) as created_at,
        created_by_id,
        assumeNotNull(toDateTime64(updated_at, 6, 'Europe/Paris')) as updated_at,
        updated_by_id,
        title,
        article_count,
        close_at,
        create_article_sender_id,
        create_article_type_id,
        customer_id,
        first_response_at,
        first_response_diff_in_min,
        first_response_in_min,
        group_id,
        last_close_at,
        last_contact_agent_at,
        last_contact_at,
        last_contact_customer_at,
        last_owner_update_at,
        owner_id,
        pending_time,
        priority_id,
        state_id,
        _dlt_id
    FROM
        {{ source("raw_zone_zammad_new", "tickets") }} tickets_new
),

__tickets_new_with_tags AS (
    SELECT
        id,
        groupArray(toNullable(tags.value)) as tags    
    FROM
        __tickets_new
    LEFT JOIN {{ source("raw_zone_zammad_new", "tickets__tags") }} tags ON __tickets_new._dlt_id = tags._dlt_parent_id
    GROUP BY 1
),

__tickets_new_with_article_ids AS (
    SELECT
        id,
        groupArray(toNullable(article_ids.value::String)) as article_ids    
    FROM
        __tickets_new
    LEFT JOIN {{ source("raw_zone_zammad_new", "tickets__article_ids") }} article_ids ON __tickets_new._dlt_id = article_ids._dlt_parent_id
    GROUP BY 1    
),
 unioned_tickets AS (
    SELECT
        id,
        "number",
        assumeNotNull(toDateTime64(created_at, 6, 'Europe/Paris')) as created_at,
        created_by_id,
        assumeNotNull(toDateTime64(updated_at, 6, 'Europe/Paris')) as updated_at,
        updated_by_id,
        title,
        article_count,
        splitByChar(',',assumeNotNull(replaceRegexpAll(article_ids, '^\[|\]$|"', ''))) as article_ids,
        close_at,
        create_article_sender_id,
        create_article_type_id,
        customer_id,
        first_response_at,
        first_response_diff_in_min,
        first_response_in_min,
        group_id,
        last_close_at,
        last_contact_agent_at,
        last_contact_at,
        last_contact_customer_at,
        last_owner_update_at,
        owner_id,
        pending_time,
        priority_id,
        state_id,    
        splitByChar(',',assumeNotNull(replaceRegexpAll(tags, '^\[|\]$|"', ''))) as tags
    FROM
        {{ source("raw_zone_zammad", "tickets") }}

    UNION DISTINCT

    SELECT
        __tickets_new.id,
        __tickets_new."number",
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
        first_response_at,
        first_response_diff_in_min,
        first_response_in_min,
        group_id,
        last_close_at,
        last_contact_agent_at,
        last_contact_at,
        last_contact_customer_at,
        last_owner_update_at,
        owner_id,
        pending_time,
        priority_id,
        state_id,
        tags
    FROM __tickets_new
    LEFT JOIN __tickets_new_with_tags tags ON __tickets_new.id = tags.id
    LEFT JOIN __tickets_new_with_article_ids article_ids ON __tickets_new.id = article_ids.id    
    
),

ranked_tickets AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY id
            ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
        ) as rn
    FROM unioned_tickets
)

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
    first_response_at,
    first_response_diff_in_min,
    first_response_in_min,
    group_id,
    last_close_at,
    last_contact_agent_at,
    last_contact_at,
    last_contact_customer_at,
    last_owner_update_at,
    owner_id,
    pending_time,
    priority_id,
    state_id,
    tags
FROM ranked_tickets
WHERE rn = 1
