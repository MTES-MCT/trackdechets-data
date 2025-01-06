{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True}
    ]
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'trader_receipt') }}
)
SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toString("receiptNumber")) as receipt_number,
    assumeNotNull(toDateTime64("validityLimit", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("validityLimit",'Europe/Paris'))) as validity_limit,
    toLowCardinality(assumeNotNull(toString("department"))) as department
 FROM source
