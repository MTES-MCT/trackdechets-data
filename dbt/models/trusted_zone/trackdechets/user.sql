{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id',
    on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('trackdechets_production', 'user') }} b
    {% if is_incremental() %}
    where b."updatedAt" >= (SELECT toString(toStartOfDay(max(updated_at)))  FROM {{ this }})
    {% endif %}
)
SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toString("email")) as email,
    assumeNotNull(toString("password")) as password,
    assumeNotNull(toString("name")) as name,
    toNullable(toString("phone")) as phone,
    assumeNotNull(toDateTime64("createdAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("createdAt",'Europe/Paris'))) as created_at,
    assumeNotNull(toDateTime64("updatedAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("updatedAt",'Europe/Paris'))) as updated_at,
    toNullable(toBool("isActive")) as is_active,
    toNullable(toDateTime64("activatedAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("activatedAt",'Europe/Paris'))) as activated_at,
    toNullable(toDateTime64("firstAssociationDate", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("firstAssociationDate",'Europe/Paris'))) as first_association_date,
    assumeNotNull(toBool("isAdmin")) as is_admin,
    toNullable(toInt256("passwordVersion")) as password_version,
    toNullable(toString("governmentAccountId")) as government_account_id
 FROM source
