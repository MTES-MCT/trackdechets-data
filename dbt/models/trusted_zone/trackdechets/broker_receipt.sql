{{
  config(
    materialized = 'table',
    )
}}
with source as (
    select * from {{ source('trackdechets_production', 'broker_receipt') }}
)

select
    assumeNotNull(toString("id"))
        as id,
    assumeNotNull(toString("receiptNumber"))
        as receipt_number,
    assumeNotNull(
        toTimezone(toDateTime64("validityLimit", 6), 'Europe/Paris')
    )                                                       as validity_limit,
    toLowCardinality(assumeNotNull(toString("department")))
        as department
from source
