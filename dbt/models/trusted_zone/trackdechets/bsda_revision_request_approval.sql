{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'bsda_revision_request_approval') }} as b
    {% if is_incremental() %}
        where
            b."updatedAt"
            >= (select toString(toStartOfDay(max(updated_at))) from {{ this }})
    {% endif %}
)

select
    assumeNotNull(
        toString("id")
    ) as id,
    assumeNotNull(
        toTimeZone(toDateTime64("createdAt", 6), 'UTC')
    ) as created_at,
    assumeNotNull(
        toTimeZone(toDateTime64("updatedAt", 6), 'UTC')
    ) as updated_at,
    assumeNotNull(
        toString("revisionRequestId")
    ) as revision_request_id,
    assumeNotNull(
        toString("approverSiret")
    ) as approver_siret,
    toLowCardinality(
        assumeNotNull(toString("status"))
    ) as status,
    toNullable(
        toString("comment")
    ) as comment
from source
