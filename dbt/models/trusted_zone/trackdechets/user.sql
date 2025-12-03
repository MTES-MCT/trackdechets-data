{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('trackdechets_production', 'user') }} as b
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
        toString("email")
    ) as email,
    assumeNotNull(
        toString("password")
    ) as password,
    assumeNotNull(
        toString("name")
    ) as name,
    toNullable(
        toString("phone")
    ) as phone,
    assumeNotNull(
        toTimezone(toDateTime64("createdAt", 6), 'Europe/Paris')
    ) as created_at,
    assumeNotNull(
        toTimezone(toDateTime64("updatedAt", 6), 'Europe/Paris')
    ) as updated_at,
    toNullable(
        toBool("isActive")
    ) as is_active,
    toNullable(
        toTimezone(toDateTime64("activatedAt", 6), 'Europe/Paris')
    ) as activated_at,
    toNullable(
        toTimezone(toDateTime64("firstAssociationDate", 6), 'Europe/Paris')
    ) as first_association_date,
    assumeNotNull(
        toBool("isAdmin")
    ) as is_admin,
    toNullable(
        toString("governmentAccountId")
    ) as government_account_id
from source
